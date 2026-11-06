package com.databricks.hl7.subscriber.notifications;

import com.databricks.hl7.subscriber.api.client.JSON;
import com.databricks.hl7.subscriber.api.model.Notification;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * What the app does with each notification on its hub (the contract's Notification, generated from docs/api/openapi.json). It prints only the subscription revisions this run registered,
 * once each (the transport is at-least-once, so it deduplicates on the stable notificationId), and counts the rest as
 * skipped. The consumer group resumes where it last committed and never skips, so after time away it first works through
 * the backlog: counted, with progress, rather than printed line by line. Commits only after handling, so a crash replays.
 */
public final class Listener {
    static final Duration BACKLOG_AFTER = Duration.ofMinutes(2);
    static final Duration REPORT_EVERY = Duration.ofSeconds(10);
    private static final int SEEN_LIMIT = 100_000;

    private final Map<String, Integer> current; // subscriptionId -> the revision this run registered
    private final String groupId;
    private final PrintStream out;
    private final Set<String> seen = new HashSet<>();
    private long skipped;
    private boolean live;
    private long replayed;
    private String replayFrom;
    private String replayAt;
    private Instant nextReport;

    public Listener(Map<String, Integer> current, String groupId, PrintStream out, Instant now) {
        this.current = Map.copyOf(current);
        this.groupId = groupId;
        this.out = out;
        this.nextReport = now.plus(REPORT_EVERY);
    }

    /** Handle one record's value from the hub. */
    public void onRecord(String value, Instant now) {
        Notification n = parse(value);
        if (!isCurrent(n, current)) {
            skipped++;
            return;
        }
        if (!seen.add(n.getNotificationId())) return;
        if (seen.size() > SEEN_LIMIT) seen.clear();
        if (!live && isBacklog(n, now, BACKLOG_AFTER)) {
            if (replayFrom == null) {
                replayFrom = n.getGoldTs();
                out.printf("catching up from %s, where consumer group %s last stopped%n", replayFrom, groupId);
            }
            replayed++;
            replayAt = n.getGoldTs();
            return;
        }
        if (!live && replayFrom != null) {
            out.printf("caught up   %d backlog notifications (%s to %s); live from here%n", replayed, replayFrom,
                    replayAt);
        }
        live = true;
        out.printf("%s  %-22s rev %d  %s%n", n.getGoldTs(), n.getSubscription(), n.getRevision(), payloadJson(n));
    }

    /** Periodic progress: backlog catch-up and skipped counts, at most every {@link #REPORT_EVERY}. */
    public void onTick(Instant now) {
        if (now.isBefore(nextReport)) return;
        if (!live && replayFrom != null) out.printf("catching up %d so far, at %s%n", replayed, replayAt);
        if (skipped > 0) {
            out.printf("skipped     %d notifications for other subscriptions or earlier revisions%n", skipped);
            skipped = 0;
        }
        nextReport = now.plus(REPORT_EVERY);
    }

    /** Consume until stopAt: handle, then commit (a crash replays, never skips); run healthCheck every healthEvery. */
    public void run(KafkaConsumer<String, String> consumer, Instant stopAt, Runnable healthCheck, Duration healthEvery) {
        Instant nextCheck = Instant.now().plus(healthEvery);
        while (Instant.now().isBefore(stopAt)) {
            for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofSeconds(1))) {
                onRecord(rec.value(), Instant.now());
            }
            onTick(Instant.now());
            consumer.commitSync();
            if (Instant.now().isAfter(nextCheck)) {
                healthCheck.run();
                nextCheck = Instant.now().plus(healthEvery);
            }
        }
    }

    // The delivery rules, on the contract's Notification -----------------------------------------------------------

    /** One message value as a Notification; rejects one without its identity (notificationId, subscriptionId). */
    public static Notification parse(String value) {
        Notification n;
        try {
            n = JSON.getDefault().getMapper().readValue(value, Notification.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("not a notification: " + e.getMessage());
        }
        if (n.getNotificationId() == null || n.getSubscriptionId() == null) {
            throw new IllegalArgumentException("notification without notificationId/subscriptionId");
        }
        return n;
    }

    /**
     * True if it matches a subscription revision this run registered (subscriptionId -> revision). Anything else is
     * backlog: an earlier revision's matches, or a replaced subscription that reused the name.
     */
    public static boolean isCurrent(Notification n, Map<String, Integer> registered) {
        return n.getRevision() != null && n.getRevision().equals(registered.get(n.getSubscriptionId()));
    }

    /** True if its gold record is older than {@code after}: the group is catching up, not reading live data. */
    public static boolean isBacklog(Notification n, Instant now, Duration after) {
        try {
            return n.getGoldTs() != null && Instant.parse(n.getGoldTs()).isBefore(now.minus(after));
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    /** The payload as compact JSON, the way the hub wrote it. */
    public static String payloadJson(Notification n) {
        try {
            return JSON.getDefault().getMapper().writeValueAsString(n.getPayload());
        } catch (IOException e) {
            return String.valueOf(n.getPayload());
        }
    }
}
