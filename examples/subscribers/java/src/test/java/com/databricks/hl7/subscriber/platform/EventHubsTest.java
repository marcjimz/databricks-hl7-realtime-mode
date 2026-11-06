package com.databricks.hl7.subscriber.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.callback.Callback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventHubsTest {

    /** A clock the test moves by hand. */
    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-03T12:00:00Z");

        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    @AfterEach
    void reset() {
        EventHubs.Handler.tokens = null;
    }

    @Test
    void cachesTheTokenUntilCloseToExpiry() {
        var clock = new MutableClock();
        var calls = new AtomicInteger();
        var tokens = new EventHubs.Tokens(
                () -> new EventHubs.Token("t" + calls.incrementAndGet(), clock.now.plus(Duration.ofHours(1))), clock);

        assertEquals("t1", tokens.get().value());
        clock.now = clock.now.plus(Duration.ofMinutes(50));
        assertEquals("t1", tokens.get().value(), "still more than the refresh margin away from expiry");
        clock.now = clock.now.plus(Duration.ofMinutes(6));
        assertEquals("t2", tokens.get().value(), "within the refresh margin: fetch a new token");
        assertEquals(2, calls.get());
    }

    @Test
    void rejectsAnEmptyToken() {
        var clock = new MutableClock();
        var tokens = new EventHubs.Tokens(() -> new EventHubs.Token(" ", clock.now.plusSeconds(3600)), clock);
        assertThrows(IllegalStateException.class, tokens::get);
    }

    @Test
    void answersKafkaWithTheCurrentUcToken() throws Exception {
        Instant expiry = Instant.parse("2026-11-03T13:00:00Z");
        Clock clock = Clock.fixed(Instant.parse("2026-11-03T12:00:00Z"), ZoneOffset.UTC);
        EventHubs.Handler.tokens = new EventHubs.Tokens(() -> new EventHubs.Token("aad-token", expiry), clock);

        var handler = new EventHubs.Handler();
        handler.configure(Map.of(), "OAUTHBEARER", List.of());
        var callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertNotNull(callback.token());
        assertEquals("aad-token", callback.token().value());
        assertEquals(expiry.toEpochMilli(), callback.token().lifetimeMs());
    }

    @Test
    void consumerUsesEntraOAuthBearerAndNoSecrets() {
        var p = EventHubs.consumer("ns1.servicebus.windows.net:9093", "app-a");  // as getConnection returns it

        assertEquals("ns1.servicebus.windows.net:9093", p.get("bootstrap.servers"));
        assertEquals("SASL_SSL", p.get("security.protocol"));
        assertEquals("OAUTHBEARER", p.get("sasl.mechanism"));
        assertEquals(EventHubs.Handler.class.getName(), p.get("sasl.login.callback.handler.class"));
        assertEquals("app-a", p.get("group.id"));
        String jaas = (String) p.get("sasl.jaas.config");
        assertTrue(jaas.startsWith("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"));
        assertFalse(jaas.contains("password"), "no connection strings or keys");
    }

    @Test
    void followsMicrosoftsEventHubsClientGuidanceAndCommitsOnlyAfterHandling() {
        var p = EventHubs.consumer("ns1.servicebus.windows.net:9093", "app-a");
        assertTrue(Integer.parseInt((String) p.get("metadata.max.age.ms")) < 240_000, "Azure closes idle links at 240 s");
        assertTrue(Integer.parseInt((String) p.get("connections.max.idle.ms")) < 240_000);
        assertEquals("false", p.get("enable.auto.commit"));
    }
}
