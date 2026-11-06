package com.databricks.hl7.subscriber.platform;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.GenerateTemporaryServiceCredentialAzureOptions;
import com.databricks.sdk.service.catalog.GenerateTemporaryServiceCredentialRequest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

/**
 * Reading this app's own Event Hub with no Azure secret. Unity Catalog mints a short-lived Entra token from the app's
 * service credential (sub_&lt;app&gt;_eventhubs; ACCESS for this app's SP only, Data Receiver on this app's hub only),
 * Kafka gets it through the OAUTHBEARER callback, and the consumer follows Microsoft's Event Hubs client guidance.
 */
public final class EventHubs {
    private EventHubs() { }

    static final Duration REFRESH_MARGIN = Duration.ofMinutes(5);

    public record Token(String value, Instant expiresAt) { }

    /** A token source, cached until close to expiry. */
    public static final class Tokens implements Supplier<Token> {
        private final Supplier<Token> fetch;
        private final Clock clock;
        private Token current;

        public Tokens(Supplier<Token> fetch, Clock clock) {
            this.fetch = Objects.requireNonNull(fetch);
            this.clock = Objects.requireNonNull(clock);
        }

        @Override
        public synchronized Token get() {
            if (current == null || clock.instant().isAfter(current.expiresAt().minus(REFRESH_MARGIN))) {
                Token t = fetch.get();
                if (t == null || t.value() == null || t.value().isBlank()) {
                    throw new IllegalStateException("Unity Catalog returned no Event Hubs token");
                }
                current = t;
            }
            return current;
        }
    }

    /**
     * One token from Unity Catalog for `scope`; PERMISSION_DENIED unless this identity has ACCESS on `credential`. Both
     * come from the Subscription API (getConnection).
     */
    public static Token mint(WorkspaceClient workspace, String credential, String scope) {
        var c = workspace.credentials().generateTemporaryServiceCredential(new GenerateTemporaryServiceCredentialRequest()
                .setCredentialName(credential)
                .setAzureOptions(new GenerateTemporaryServiceCredentialAzureOptions().setResources(List.of(scope))));
        return new Token(c.getAzureAad().getAadToken(), Instant.ofEpochMilli(c.getExpirationTime()));
    }

    /** Use tokens for `credential` in every Kafka client this process creates. */
    public static void useCredential(WorkspaceClient workspace, String credential, String scope) {
        Handler.tokens = new Tokens(() -> mint(workspace, credential, scope), Clock.systemUTC());
    }

    /** Kafka consumer settings: Entra OAUTHBEARER (no keys) plus Microsoft's Event Hubs client guidance. */
    public static Properties consumer(String bootstrapServers, String groupId) {
        var p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "OAUTHBEARER");
        p.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        p.put("sasl.login.callback.handler.class", Handler.class.getName());
        p.put("group.id", groupId);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "latest");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // learn.microsoft.com/azure/event-hubs/apache-kafka-configurations: Azure closes idle connections after 240 s.
        p.put("metadata.max.age.ms", "180000");
        p.put("connections.max.idle.ms", "180000");
        p.put("request.timeout.ms", "60000");
        p.put("session.timeout.ms", "30000");
        return p;
    }

    /** Kafka's SASL/OAUTHBEARER login callback; Kafka creates it by class name, so it reads the registered tokens. */
    public static final class Handler implements AuthenticateCallbackHandler {
        static volatile Tokens tokens;

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> entries) {
            if (!"OAUTHBEARER".equals(saslMechanism)) throw new IllegalArgumentException("Unexpected: " + saslMechanism);
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (!(callback instanceof OAuthBearerTokenCallback c)) throw new UnsupportedCallbackException(callback);
                Tokens source = tokens;
                if (source == null) throw new IllegalStateException("No Event Hubs token source registered");
                Token t = source.get();
                c.token(new Bearer(t.value(), t.expiresAt().toEpochMilli()));
            }
        }

        @Override
        public void close() { }
    }

    private record Bearer(String value, long lifetimeMs) implements OAuthBearerToken {
        @Override public Set<String> scope() { return Set.of(); }
        @Override public String principalName() { return "unity-catalog-service-credential"; }
        @Override public Long startTimeMs() { return null; }
    }
}
