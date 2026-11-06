package com.databricks.hl7.subscriber.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.hl7.subscriber.api.model.Notification;
import org.junit.jupiter.api.Test;

/** The delivery rules on the contract's Notification (generated from docs/api/openapi.json). */
class NotificationTest {

    @Test
    void parsesTheHubEnvelope() {
        var n = Listener.parse("{\"notificationId\":\"n1\",\"subscriptionId\":\"s1\",\"subscription\":\"critical-k\","
                + "\"revision\":3,\"gold_ts\":\"2026-11-03T12:00:00.123Z\",\"payload\":{\"patient_id\":\"P1\",\"value_num\":6.4}}");

        assertEquals("n1", n.getNotificationId());
        assertEquals("critical-k", n.getSubscription());
        assertEquals(3, n.getRevision());
        assertEquals("P1", n.getPayload().get("patient_id"));
        assertEquals(6.4, n.getPayload().get("value_num"));
        assertEquals("{\"patient_id\":\"P1\",\"value_num\":6.4}", Listener.payloadJson(n), "printed as the hub wrote it");
    }

    @Test
    void onlyTheRevisionThisRunRegisteredIsCurrent() {
        var current = java.util.Map.of("s1", 3);
        assertEquals(true, Listener.isCurrent(parse("s1", 3), current));
        assertEquals(false, Listener.isCurrent(parse("s1", 2), current), "an earlier revision still in the hub's backlog");
        assertEquals(false, Listener.isCurrent(parse("s0", 3), current), "a cancelled or replaced subscription with the same name");
    }

    static Notification parse(String subscriptionId, int revision) {
        return Listener.parse("{\"notificationId\":\"n\",\"subscriptionId\":\"" + subscriptionId
                + "\",\"subscription\":\"k\",\"revision\":" + revision + ",\"payload\":{}}");
    }

    @Test
    void isBacklogWhenItsGoldRecordIsOld() {
        var n = Listener.parse("{\"notificationId\":\"n\",\"subscriptionId\":\"s\",\"revision\":1,"
                + "\"gold_ts\":\"2026-11-03T12:00:00.123Z\",\"payload\":{}}");
        var now = java.time.Instant.parse("2026-11-03T12:05:00Z");

        assertEquals(true, Listener.isBacklog(n, now, java.time.Duration.ofMinutes(2)));
        assertEquals(false, Listener.isBacklog(n, now, java.time.Duration.ofMinutes(10)));
        assertEquals(false, Listener.isBacklog(parse("s", 1), now, java.time.Duration.ofMinutes(2)), "no gold_ts: unknown, not old");
    }

    @Test
    void rejectsARecordWithoutIdentity() {
        assertThrows(IllegalArgumentException.class, () -> Listener.parse("{\"payload\":{}}"));
    }
}
