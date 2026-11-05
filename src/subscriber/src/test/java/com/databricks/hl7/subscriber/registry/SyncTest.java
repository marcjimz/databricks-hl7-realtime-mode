package com.databricks.hl7.subscriber.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.hl7.subscriber.rules.Predicate;
import com.databricks.hl7.subscriber.rules.Subscription;
import java.util.List;
import org.junit.jupiter.api.Test;

class SyncTest {

    static Subscription sub(String name) {
        return new Subscription(name, "gold_observations", Predicate.field("value_num").gt(6), List.of("patient_id"));
    }

    static RegistryClient.Status status(String name, boolean enabled) {
        return new RegistryClient.Status("11111111-1111-4111-8111-111111111111", name, 1, enabled, "gold_observations",
                "value_num > 6", "ACTIVE", null);
    }

    @Test
    void subscribesEverythingInCodeAndCancelsWhatWasRemoved() {
        var plan = Sync.plan(List.of(sub("a"), sub("b")), List.of(status("a", true), status("old", true)));

        assertEquals(List.of("a", "b"), plan.subscribe().stream().map(Subscription::name).toList());
        assertEquals(List.of("old"), plan.unsubscribe());
    }

    @Test
    void doesNotCancelAnAlreadyCancelledSubscription() {
        var plan = Sync.plan(List.of(sub("a")), List.of(status("gone", false)));
        assertEquals(List.of(), plan.unsubscribe());
    }

    @Test
    void rejectsDuplicateNamesInCode() {
        assertThrows(IllegalArgumentException.class, () -> Sync.plan(List.of(sub("a"), sub("a")), List.of()));
    }

    @Test
    void subscriptionNamesAreSlugs() {
        assertThrows(IllegalArgumentException.class, () -> sub("Not A Slug"));
    }
}
