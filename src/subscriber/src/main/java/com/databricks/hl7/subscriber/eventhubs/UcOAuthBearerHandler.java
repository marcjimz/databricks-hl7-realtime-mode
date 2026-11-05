package com.databricks.hl7.subscriber.eventhubs;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

/**
 * Kafka SASL/OAUTHBEARER login callback that answers with the Unity Catalog-minted Event Hubs token. Kafka creates the
 * handler by class name, so the token source is registered once with {@link #use(EventHubsTokens)}.
 */
public final class UcOAuthBearerHandler implements AuthenticateCallbackHandler {
    private static volatile EventHubsTokens tokens;

    public static void use(EventHubsTokens source) {
        tokens = source;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!"OAUTHBEARER".equals(saslMechanism)) {
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback c) {
                EventHubsTokens source = tokens;
                if (source == null) throw new IllegalStateException("No Event Hubs token source registered");
                EventHubsTokens.Token t = source.get();
                c.token(new Bearer(t.value(), t.expiresAt().toEpochMilli()));
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    @Override
    public void close() { }

    private record Bearer(String value, long lifetimeMs) implements OAuthBearerToken {
        @Override public Set<String> scope() { return Set.of(); }
        @Override public String principalName() { return "unity-catalog-service-credential"; }
        @Override public Long startTimeMs() { return null; }
    }
}
