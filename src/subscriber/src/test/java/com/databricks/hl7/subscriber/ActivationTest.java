package com.databricks.hl7.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ActivationTest {

    static RegistryClient.Status status(String name, int revision, String state, String error) {
        return new RegistryClient.Status("id-" + name, name, revision, true, "gold_observations", "x", state, error);
    }

    @Test
    void readyOnlyWhenEveryDeclaredRevisionIsActive() {
        var declared = Map.of("a", 2, "b", 1);
        assertEquals(Activation.State.PENDING,
                Activation.check(declared, List.of(status("a", 2, "ACTIVE", null), status("b", 1, "PENDING", null))).state());
        assertEquals(Activation.State.READY,
                Activation.check(declared, List.of(status("a", 2, "ACTIVE", null), status("b", 1, "ACTIVE", null))).state());
    }

    @Test
    void anOlderRevisionBeingActiveIsNotReady() {
        var r = Activation.check(Map.of("a", 2), List.of(status("a", 1, "ACTIVE", null)));
        assertEquals(Activation.State.PENDING, r.state());
    }

    @Test
    void aFailedRevisionFailsWithTheHubsReason() {
        var r = Activation.check(Map.of("a", 1), List.of(status("a", 1, "FAILED", "FIELD_NOT_IN_SOURCE: x")));
        assertEquals(Activation.State.FAILED, r.state());
        assertEquals(List.of("a: FIELD_NOT_IN_SOURCE: x"), r.problems());
    }

    @Test
    void aSourceWithoutARecentHubBatchIsStale() {
        Instant now = Instant.parse("2026-11-03T12:00:00Z");
        var hub = Map.of("gold_observations", now.minusSeconds(20), "gold_orders", now.minusSeconds(600));
        assertEquals(List.of(), Activation.staleSources(hub, Set.of("gold_observations"), now, Duration.ofMinutes(2)));
        assertEquals(List.of("gold_orders"), Activation.staleSources(hub, Set.of("gold_orders"), now, Duration.ofMinutes(2)));
        assertEquals(List.of("gold_encounters"),
                Activation.staleSources(hub, Set.of("gold_encounters"), now, Duration.ofMinutes(2)), "never ran");
    }
}
