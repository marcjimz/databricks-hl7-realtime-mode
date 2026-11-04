package com.databricks.hl7.subscriber;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Entra ID tokens for the Event Hubs namespace, minted by Unity Catalog from this app's service credential. The app holds
 * no Azure secret: UC checks the caller's ACCESS privilege and returns a short-lived token for the credential's managed
 * identity (which has Data Receiver on this app's hub only). Cached until close to expiry.
 */
public final class EventHubsTokens implements Supplier<EventHubsTokens.Token> {
    static final Duration REFRESH_MARGIN = Duration.ofMinutes(5);

    public record Token(String value, Instant expiresAt) { }

    private final Supplier<Token> fetch;
    private final Clock clock;
    private Token current;

    public EventHubsTokens(Supplier<Token> fetch, Clock clock) {
        this.fetch = Objects.requireNonNull(fetch);
        this.clock = Objects.requireNonNull(clock);
    }

    public static String scope(String namespace) {
        return "https://" + namespace + ".servicebus.windows.net/.default";
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
