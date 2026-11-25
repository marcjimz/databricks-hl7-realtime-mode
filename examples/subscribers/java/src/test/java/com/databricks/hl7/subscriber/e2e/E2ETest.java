package com.databricks.hl7.subscriber.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.hl7.subscriber.api.client.JSON;
import com.databricks.hl7.subscriber.api.model.Heartbeat;
import com.databricks.hl7.subscriber.api.model.Subscription;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** The same rules as tests/test_subscriber_examples.py for the Python example's e2e.py. */
class E2ETest {
    /** The ed-respiratory-monitoring template's columns (infra/azure/terraform.tfvars.example). */
    static final List<String> TEMPLATE = List.of("patient_id", "visit_id", "msg_type", "patient_class",
            "patient_point_of_care", "chief_complaint", "diagnosis_cd", "diagnosis_type", "observation_cd",
            "observation_cd_system", "alt_observation_cd", "alt_observation_cd_system", "value", "value_num", "units",
            "abnormal_flag", "order_cd", "diagnostic_service", "document_type", "document_text");

    private static <T> T parse(String json, Class<T> type) throws Exception {
        return JSON.getDefault().getMapper().readValue(json, type);
    }

    @Test
    void theSourceNotGrantedIsOneOutsideTheAppsGrant() {
        assertEquals("silver_encounters", E2E.ungrantedSource(List.of("silver_observations", "silver_diagnoses")));
        assertEquals("silver_orders", E2E.ungrantedSource(List.of("silver_encounters", "silver_observations")));
        assertNull(E2E.ungrantedSource(E2E.SOURCES), "granted every source: the check is skipped");
    }

    @Test
    void theRuleReadsOnlyWhatTheAppIsGranted() {
        var rules = E2E.potassium(TEMPLATE);
        assertTrue(rules.good().contains("alt_observation_cd = '2823-3'"), "by LOINC when the name isn't granted");
        assertTrue(rules.changed().contains("units") && !rules.good().contains("units"));
        assertTrue(E2E.potassium(List.of("observation_cd_desc")).good().contains("observation_cd_desc = 'Potassium'"));
        assertTrue(E2E.potassium(null).good().contains("observation_cd_desc = 'Potassium'"), "an API without fields");
        assertEquals(new E2E.Rules(
                "SELECT patient_id, value_num FROM silver_observations WHERE alt_observation_cd = '2823-3' "
                        + "AND value_num >= 6.5",
                "SELECT patient_id, value_num, units FROM silver_observations WHERE alt_observation_cd = '2823-3' "
                        + "AND value_num >= 7.0"), rules, "the same SQL as the Python example");
    }

    @Test
    void cleanupCancelsEveryE2eRuleLeftEnabled() throws Exception {
        var subs = List.of(parse("{\"name\": \"e2e-refused\", \"enabled\": true}", Subscription.class),
                parse("{\"name\": \"e2e-potassium\", \"enabled\": false}", Subscription.class),
                parse("{\"name\": \"critical-k\", \"enabled\": true}", Subscription.class),
                parse("{\"name\": \"e2e-ok\", \"enabled\": true}", Subscription.class));
        assertEquals(List.of("e2e-ok", "e2e-refused"), E2E.leftovers(subs), "the app's own rules are left alone");
    }

    @Test
    void theHeartbeatIsTheAppsOwnHubs() throws Exception {
        var beats = List.of(
                parse("{\"hub\": \"edp_shared\", \"tier\": \"realtime\", \"source\": \"silver_observations\", "
                        + "\"lastBatchAt\": \"2026-11-05T12:00:00Z\", \"batchId\": 1, \"rules\": 2}", Heartbeat.class),
                parse("{\"hub\": \"bu_demo\", \"tier\": \"deferred\", \"source\": \"silver_observations\", "
                        + "\"lastBatchAt\": \"2026-11-05T12:00:09Z\", \"batchId\": 1, \"rules\": 2}", Heartbeat.class),
                parse("{\"hub\": \"bu_demo\", \"tier\": \"realtime\", \"source\": \"silver_observations\", "
                        + "\"lastBatchAt\": \"2026-11-05T12:00:05Z\", \"batchId\": 1, \"rules\": 2}", Heartbeat.class));
        assertEquals(Instant.parse("2026-11-05T12:00:05Z"),
                OffsetDateTime.parse(E2E.heartbeat(beats, "bu_demo", "silver_observations")).toInstant());
        assertEquals(Instant.parse("2026-11-05T12:00:00Z"),
                OffsetDateTime.parse(E2E.heartbeat(beats, "edp_shared", "silver_observations")).toInstant());
        assertNull(E2E.heartbeat(beats, "edp_shared", "silver_orders"));
        assertNull(E2E.heartbeat(null, "bu_demo", "silver_observations"));
    }

    @Test
    void aSkippedCheckIsReportedAndDoesntFailTheRun() {
        var out = new ByteArrayOutputStream();
        PrintStream was = System.out;
        System.setOut(new PrintStream(out, true));
        try {
            var t = new E2E(null, null, null, new Properties());
            t.record("Sign in", "profile x", "a service principal", "sp", true);
            t.skip("Source not granted", "a source this app isn't granted", "TABLE_OR_VIEW_NOT_FOUND",
                    "this app is granted every source");
            t.print();
            assertTrue(t.ok());
            t.record("Cancel", "-", "CANCELLED", "PENDING", false);
            assertFalse(t.ok());
        } finally {
            System.setOut(was);
        }
        String printed = out.toString();
        assertTrue(printed.contains("| 2 | Source not granted | a source this app isn't granted | TABLE_OR_VIEW_NOT_FOUND "
                + "| this app is granted every source | SKIP |"), printed);
        assertTrue(printed.contains("2 checks, 0 failed, 1 skipped"), printed);
    }

    @Test
    void runAsAPersonTheFirstCheckFailsAndSaysWhy() {
        var out = new ByteArrayOutputStream();
        PrintStream was = System.out;
        System.setOut(new PrintStream(out, true));
        try {
            var t = new E2E(null, null, null, new Properties());
            assertFalse(t.signedIn("someone@example.com"), "the run stops: the checks are a service principal's");
            t.print();
            assertFalse(t.ok());
            assertTrue(new E2E(null, null, null, new Properties()).signedIn("00000000-0000-4000-8000-000000000001"));
        } finally {
            System.setOut(was);
        }
        String printed = out.toString();
        assertTrue(printed.contains("| 1 | Sign in | profile (ambient identity) | a service principal | someone@example.com: "
                + "a user (--e2e runs as a service principal) | **FAIL** |"), printed);
    }
}
