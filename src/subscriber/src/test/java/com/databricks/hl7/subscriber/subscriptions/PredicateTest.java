package com.databricks.hl7.subscriber.subscriptions;

import static com.databricks.hl7.subscriber.subscriptions.Predicate.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class PredicateTest {

    @Test
    void aComparisonIsFieldOpValue() {
        assertEquals("{\"field\":\"value_num\",\"op\":\"lt\",\"value\":8}", field("value_num").lt(8).toJson().toString());
        assertEquals("{\"field\":\"units\",\"op\":\"eq\",\"value\":\"g/dL\"}", field("units").eq("g/dL").toJson().toString());
    }

    @Test
    void andChainsFlattenIntoOneList() {
        var p = field("a").eq("x").and(field("b").gt(1)).and(field("c").isNull());
        assertEquals("{\"and\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},{\"field\":\"b\",\"op\":\"gt\",\"value\":1},"
                + "{\"field\":\"c\",\"op\":\"is_null\"}]}", p.toJson().toString());
    }

    @Test
    void orAndNotNest() {
        var p = field("flag").eq("LL").or(field("flag").eq("HH")).not();
        assertEquals("{\"not\":{\"or\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"LL\"},"
                + "{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"HH\"}]}}", p.toJson().toString());
    }

    @Test
    void listsAndTextOperators() {
        assertEquals("{\"field\":\"patient_id\",\"op\":\"in\",\"value\":[\"P1\",\"P2\"]}",
                field("patient_id").in("P1", "P2").toJson().toString());
        assertEquals("{\"field\":\"desc\",\"op\":\"starts_with\",\"value\":\"Pot\"}",
                field("desc").startsWith("Pot").toJson().toString());
    }

    @Test
    void timestampsAreIso8601() {
        assertEquals("{\"field\":\"t\",\"op\":\"ge\",\"value\":\"2026-11-03T00:00:00Z\"}",
                field("t").ge(Instant.parse("2026-11-03T00:00:00Z")).toJson().toString());
    }

    @Test
    void rejectsValuesTheRegistryCantType() {
        assertThrows(IllegalArgumentException.class, () -> field("x").eq(new Object()));
        assertThrows(IllegalArgumentException.class, () -> field("x").in());
    }
}
