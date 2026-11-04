package com.databricks.hl7.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class NotificationTest {

    @Test
    void parsesTheHubEnvelope() {
        var n = Notification.parse("{\"notificationId\":\"n1\",\"subscriptionId\":\"s1\",\"subscription\":\"critical-k\","
                + "\"revision\":3,\"gold_ts\":\"2026-11-03T12:00:00.123Z\",\"payload\":{\"patient_id\":\"P1\",\"value_num\":6.4}}");

        assertEquals("n1", n.notificationId());
        assertEquals("critical-k", n.subscription());
        assertEquals(3, n.revision());
        assertEquals("P1", n.payload().get("patient_id").asText());
        assertEquals(6.4, n.payload().get("value_num").asDouble());
    }

    @Test
    void onlyTheRevisionThisRunRegisteredIsCurrent() {
        var current = java.util.Map.of("s1", 3);
        assertEquals(true, parse("s1", 3).isCurrent(current));
        assertEquals(false, parse("s1", 2).isCurrent(current), "an earlier revision still in the hub's backlog");
        assertEquals(false, parse("s0", 3).isCurrent(current), "a cancelled or replaced subscription with the same name");
    }

    static Notification parse(String subscriptionId, int revision) {
        return Notification.parse("{\"notificationId\":\"n\",\"subscriptionId\":\"" + subscriptionId
                + "\",\"subscription\":\"k\",\"revision\":" + revision + ",\"payload\":{}}");
    }

    @Test
    void rejectsARecordWithoutIdentity() {
        assertThrows(IllegalArgumentException.class, () -> Notification.parse("{\"payload\":{}}"));
    }
}
