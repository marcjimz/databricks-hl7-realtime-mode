package com.databricks.hl7.subscriber.eventhubs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class EventHubsTokensTest {

    /** A clock the test moves by hand. */
    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-03T12:00:00Z");

        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    @Test
    void cachesTheTokenUntilCloseToExpiry() {
        var clock = new MutableClock();
        var calls = new AtomicInteger();
        var tokens = new EventHubsTokens(
                () -> new EventHubsTokens.Token("t" + calls.incrementAndGet(), clock.now.plus(Duration.ofHours(1))),
                clock);

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
        var tokens = new EventHubsTokens(() -> new EventHubsTokens.Token(" ", clock.now.plusSeconds(3600)), clock);
        assertThrows(IllegalStateException.class, tokens::get);
    }

    @Test
    void scopeIsTheNamespaceDefaultScope() {
        assertEquals("https://ns1.servicebus.windows.net/.default", EventHubsTokens.scope("ns1"));
    }
}
