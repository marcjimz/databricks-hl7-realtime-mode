package com.databricks.hl7.subscriber.subscriptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Which records a subscription selects, as a structured predicate (never SQL text). The registry validates it against
 * the source's columns and this app's grants when you subscribe, and the hub builds its filter from the same structure.
 *
 * <pre>
 *   field("observation_cd_desc").eq("Hemoglobin").and(field("value_num").lt(8))
 * </pre>
 *
 * Comparisons follow SQL: a NULL value never matches (use {@link Field#isNull()} to select missing values).
 */
public abstract sealed class Predicate permits Predicate.Leaf, Predicate.Group, Predicate.Not {
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    public static Field field(String name) {
        return new Field(name);
    }

    public Predicate and(Predicate other) {
        return Group.of("and", this, other);
    }

    public Predicate or(Predicate other) {
        return Group.of("or", this, other);
    }

    public Predicate not() {
        return new Not(this);
    }

    /** The JSON the registry receives. */
    public abstract ObjectNode toJson();

    @Override
    public String toString() {
        return toJson().toString();
    }

    /** A column of the subscription's source. */
    public static final class Field {
        private final String name;

        private Field(String name) {
            this.name = Objects.requireNonNull(name, "field");
        }

        public Predicate eq(Object value) { return new Leaf(name, "eq", literal(value)); }
        public Predicate ne(Object value) { return new Leaf(name, "ne", literal(value)); }
        public Predicate lt(Object value) { return new Leaf(name, "lt", literal(value)); }
        public Predicate le(Object value) { return new Leaf(name, "le", literal(value)); }
        public Predicate gt(Object value) { return new Leaf(name, "gt", literal(value)); }
        public Predicate ge(Object value) { return new Leaf(name, "ge", literal(value)); }
        public Predicate in(Object... values) { return new Leaf(name, "in", list(values)); }
        public Predicate notIn(Object... values) { return new Leaf(name, "not_in", list(values)); }
        public Predicate isNull() { return new Leaf(name, "is_null", null); }
        public Predicate isNotNull() { return new Leaf(name, "is_not_null", null); }
        /** Literal substring (no wildcards). */
        public Predicate contains(String text) { return new Leaf(name, "contains", literal(text)); }
        public Predicate startsWith(String text) { return new Leaf(name, "starts_with", literal(text)); }
        public Predicate endsWith(String text) { return new Leaf(name, "ends_with", literal(text)); }
    }

    static final class Leaf extends Predicate {
        private final String field;
        private final String op;
        private final JsonNode value;

        private Leaf(String field, String op, JsonNode value) {
            this.field = field;
            this.op = op;
            this.value = value;
        }

        @Override
        public ObjectNode toJson() {
            ObjectNode n = JSON.objectNode().put("field", field).put("op", op);
            if (value != null) n.set("value", value);
            return n;
        }
    }

    static final class Group extends Predicate {
        private final String op;
        private final List<Predicate> parts;

        private Group(String op, List<Predicate> parts) {
            this.op = op;
            this.parts = List.copyOf(parts);
        }

        /** a.and(b).and(c) is one AND of three, not nested pairs. */
        static Group of(String op, Predicate left, Predicate right) {
            List<Predicate> parts = new ArrayList<>();
            for (Predicate p : List.of(left, Objects.requireNonNull(right))) {
                if (p instanceof Group g && g.op.equals(op)) parts.addAll(g.parts);
                else parts.add(p);
            }
            return new Group(op, parts);
        }

        @Override
        public ObjectNode toJson() {
            ArrayNode a = JSON.arrayNode();
            parts.forEach(p -> a.add(p.toJson()));
            return JSON.objectNode().set(op, a);
        }
    }

    static final class Not extends Predicate {
        private final Predicate inner;

        private Not(Predicate inner) {
            this.inner = inner;
        }

        @Override
        public ObjectNode toJson() {
            return JSON.objectNode().set("not", inner.toJson());
        }
    }

    private static JsonNode literal(Object v) {
        if (v instanceof String s) return JSON.textNode(s);
        if (v instanceof Integer i) return JSON.numberNode(i);
        if (v instanceof Long l) return JSON.numberNode(l);
        if (v instanceof Double d) return JSON.numberNode(d);
        if (v instanceof Float f) return JSON.numberNode(f.doubleValue());
        if (v instanceof Instant t) return JSON.textNode(t.toString());
        throw new IllegalArgumentException("unsupported value " + v + ": use a String, number, or Instant");
    }

    private static JsonNode list(Object... values) {
        if (values.length == 0) throw new IllegalArgumentException("in / notIn need at least one value");
        ArrayNode a = JSON.arrayNode();
        for (Object v : values) a.add(literal(v));
        return a;
    }
}
