package com.databricks.hl7.subscriber.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ListenerTest {
    static final Instant NOW = Instant.parse("2026-11-05T12:00:00Z");

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final Listener listener = new Listener(Map.of("s1", 3), "my-group",
            new PrintStream(bytes, true, StandardCharsets.UTF_8), NOW);

    static String record(String id, String sid, int revision, Instant sourceTs) {
        return "{\"notificationId\":\"" + id + "\",\"subscriptionId\":\"" + sid + "\",\"subscription\":\"critical-k\","
                + "\"revision\":" + revision + ",\"source_ts\":\"" + sourceTs + "\",\"payload\":{\"value_num\":7.1}}";
    }

    String out() {
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void printsLiveMatchesOnceEach() {
        listener.onRecord(record("n1", "s1", 3, NOW.minusSeconds(3)), NOW);
        listener.onRecord(record("n1", "s1", 3, NOW.minusSeconds(3)), NOW); // redelivered: at-least-once

        assertEquals(1, out().lines().filter(l -> l.contains("critical-k")).count());
        assertTrue(out().contains("rev 3  {\"value_num\":7.1}"));
    }

    @Test
    void countsOtherSubscriptionsAndEarlierRevisionsAsSkipped() {
        listener.onRecord(record("n1", "s1", 2, NOW), NOW);
        listener.onRecord(record("n2", "s0", 3, NOW), NOW);
        listener.onTick(NOW.plusSeconds(11));

        assertTrue(out().contains("skipped     2 notifications for other subscriptions or earlier revisions"), out());
        assertTrue(out().lines().noneMatch(l -> l.contains("critical-k")), "skipped ones aren't printed");
    }

    @Test
    void summarizesTheBacklogThenPrintsLiveOnes() {
        listener.onRecord(record("n1", "s1", 3, NOW.minusSeconds(3600)), NOW);
        listener.onRecord(record("n2", "s1", 3, NOW.minusSeconds(3500)), NOW);
        listener.onRecord(record("n3", "s1", 3, NOW.minusSeconds(5)), NOW);
        listener.onRecord(record("n4", "s1", 3, NOW.minusSeconds(3400)), NOW); // a late old one after going live

        var lines = out().lines().toList();
        assertTrue(lines.get(0).startsWith("catching up from 2026-11-05T11:00:00Z, where consumer group my-group"), out());
        assertTrue(lines.get(1).startsWith("caught up   2 backlog notifications"), out());
        assertEquals(2, lines.stream().filter(l -> l.contains("critical-k")).count(), "live from here: every match");
        assertEquals(1, lines.stream().filter(l -> l.startsWith("caught up")).count(), "caught up once, never flaps");
    }

    @Test
    void reportsCatchUpProgress() {
        listener.onRecord(record("n1", "s1", 3, NOW.minusSeconds(3600)), NOW);
        listener.onTick(NOW.plusSeconds(11));

        assertTrue(out().contains("catching up 1 so far, at 2026-11-05T11:00:00Z"), out());
    }
}
