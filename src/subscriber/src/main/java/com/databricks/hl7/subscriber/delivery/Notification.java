package com.databricks.hl7.subscriber.delivery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * One matched record from the hub. notificationId is stable across redeliveries (the Kafka sink is at-least-once), so
 * deduplicate on it before acting.
 */
public record Notification(String notificationId, String subscriptionId, String subscription, int revision,
        String goldTs, JsonNode payload) {
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * True if this matches a subscription revision this run registered (subscriptionId -> revision). Anything else is
     * backlog: an earlier revision's matches, or a replaced subscription that reused the name.
     */
    public boolean isCurrent(java.util.Map<String, Integer> registered) {
        return Integer.valueOf(revision).equals(registered.get(subscriptionId));
    }

    /**
     * True if its gold record is older than `after`: the consumer group is resuming from where it last stopped (it never
     * skips), not reading live data.
     */
    public boolean isBacklog(java.time.Instant now, java.time.Duration after) {
        try {
            return java.time.Instant.parse(goldTs).isBefore(now.minus(after));
        } catch (java.time.format.DateTimeParseException e) {
            return false;
        }
    }

    public static Notification parse(String value) {
        JsonNode n;
        try {
            n = JSON.readTree(value);
        } catch (IOException e) {
            throw new IllegalArgumentException("not JSON: " + e.getMessage());
        }
        if (!n.hasNonNull("notificationId") || !n.hasNonNull("subscriptionId")) {
            throw new IllegalArgumentException("notification without notificationId/subscriptionId");
        }
        return new Notification(n.get("notificationId").asText(), n.get("subscriptionId").asText(),
                n.path("subscription").asText(), n.path("revision").asInt(), n.path("gold_ts").asText(),
                n.path("payload"));
    }
}
