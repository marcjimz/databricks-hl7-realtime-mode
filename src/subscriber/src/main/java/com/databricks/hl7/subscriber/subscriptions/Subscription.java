package com.databricks.hl7.subscriber.subscriptions;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * One subscription: which gold source, which records (a structured {@link Predicate} over its columns), and which fields
 * to deliver. The registry checks all three against this app's grants when it subscribes.
 */
public record Subscription(String name, String source, Predicate where, List<String> fields) {
    private static final Pattern SLUG = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");

    public Subscription {
        Objects.requireNonNull(name, "name");
        if (!SLUG.matcher(name).matches()) {
            throw new IllegalArgumentException("subscription name must be a lowercase slug: " + name);
        }
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(where, "where");
        fields = List.copyOf(fields);
        if (fields.isEmpty()) throw new IllegalArgumentException(name + ": at least one field");
    }
}
