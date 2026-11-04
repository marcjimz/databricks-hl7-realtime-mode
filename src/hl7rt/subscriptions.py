"""Subscription hub matching: each app's structured predicate, applied to a micro-batch of one gold topic.

A predicate is JSON, never SQL text (the registry validates it when it is written; see src/registry/registry.sql):

    {"and": [p, ...]} | {"or": [p, ...]} | {"not": p} | {"field": "<column>", "op": "<op>"[, "value": <literal>]}

It compiles to a Spark Column from F.col / literals only, so nothing an app writes is ever parsed as SQL. The structure
is re-checked here (defense in depth): an unknown key, operator, field, or literal type raises PredicateError.
Comparisons follow SQL three-valued logic, as the registry's rendering reads: NULL compared with anything is unknown and
never matches.

Each match becomes a notification on the app's own topic, carrying only the fields the subscription asked for (the
registry caps those at the app's grant).
"""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructType, TimestampType

from hl7rt import gold

SOURCES: dict[str, StructType] = {
    "gold_encounters": gold.ENCOUNTER_SCHEMA,
    "gold_observations": gold.OBSERVATION_SCHEMA,
    "gold_orders": gold.ORDER_SCHEMA,
}
_TYPES = {StringType(): "string", IntegerType(): "int", LongType(): "long", DoubleType(): "double",
          TimestampType(): "timestamp"}  # fmt: skip
_COMPARE = {"eq": "__eq__", "ne": "__ne__", "lt": "__lt__", "le": "__le__", "gt": "__gt__", "ge": "__ge__"}
_TEXT = {"contains": "contains", "starts_with": "startswith", "ends_with": "endswith"}
MAX_DEPTH = 8


class PredicateError(ValueError):
    pass


@dataclass(frozen=True)
class Rule:
    """One enabled subscription revision, as the hub runs it."""

    subscription_id: str
    name: str
    revision: int
    source: str
    predicate: dict
    fields: list[str]
    topic: str


def registry_columns() -> list[tuple[str, str, str]]:
    """(source, column, type) rows for subscription_registry.source_columns."""
    return [(s, f.name, _TYPES[f.dataType]) for s, schema in SOURCES.items() for f in schema]


def _literal(value, kind: str, field: str):
    ok = {
        "string": isinstance(value, str),
        "int": isinstance(value, int) and not isinstance(value, bool),
        "long": isinstance(value, int) and not isinstance(value, bool),
        "double": isinstance(value, (int, float)) and not isinstance(value, bool),
        "timestamp": isinstance(value, str),
    }[kind]
    if not ok:
        raise PredicateError(f"TYPE_MISMATCH: {field} is {kind}, got {value!r}")
    return F.to_timestamp(F.lit(value)) if kind == "timestamp" else F.lit(value)


def compile_predicate(node, source: str, depth: int = 1) -> Column:
    """The Spark filter for a structured predicate over `source`'s columns."""
    if depth > MAX_DEPTH:
        raise PredicateError("PREDICATE_TOO_COMPLEX")
    if not isinstance(node, dict):
        raise PredicateError("PREDICATE_INVALID: expected an object")
    keys = set(node)
    if keys in ({"and"}, {"or"}):
        (op,) = keys
        children = node[op]
        if not isinstance(children, list) or not 2 <= len(children) <= 50:
            raise PredicateError(f"PREDICATE_INVALID: {op} takes 2 to 50 predicates")
        cols = [compile_predicate(c, source, depth + 1) for c in children]
        out = cols[0]
        for c in cols[1:]:
            out = (out & c) if op == "and" else (out | c)
        return out
    if keys == {"not"}:
        return ~compile_predicate(node["not"], source, depth + 1)
    if keys not in ({"field", "op"}, {"field", "op", "value"}):
        raise PredicateError(f"PREDICATE_INVALID: unknown keys {sorted(keys)}")

    field, op, value = node["field"], node["op"], node.get("value")
    kinds = {f.name: _TYPES[f.dataType] for f in SOURCES[source]}
    if field not in kinds:
        raise PredicateError(f"FIELD_NOT_IN_SOURCE: {field}")
    kind, col = kinds[field], F.col(field)
    if op in ("is_null", "is_not_null"):
        if "value" in node:
            raise PredicateError(f"PREDICATE_INVALID: {op} takes no value")
        return col.isNull() if op == "is_null" else col.isNotNull()
    if "value" not in node:
        raise PredicateError(f"PREDICATE_INVALID: {op} needs a value")
    if op in _COMPARE:
        if kind == "string" and op not in ("eq", "ne"):
            raise PredicateError(f"OPERATOR_NOT_ALLOWED: {op} on string column {field}")
        return getattr(col, _COMPARE[op])(_literal(value, kind, field))
    if op in ("in", "not_in"):
        if kind == "timestamp" or not isinstance(value, list) or not 1 <= len(value) <= 1000:
            raise PredicateError(f"PREDICATE_INVALID: {op} takes a list of 1 to 1000 values")
        member = col.isin([_literal(v, kind, field) for v in value])
        return member if op == "in" else ~member
    if op in _TEXT:
        if kind != "string" or not isinstance(value, str) or not 1 <= len(value) <= 200:
            raise PredicateError(f"OPERATOR_NOT_ALLOWED: {op} on {field}")
        return getattr(col, _TEXT[op])(F.lit(value))  # literal substring match; no wildcards
    raise PredicateError(f"OPERATOR_NOT_ALLOWED: {op}")


def notifications(topic_records: DataFrame, source: str, rules: list[Rule]) -> DataFrame | None:
    """Kafka records (topic, key, value) for every rule's matches in a micro-batch of `source`'s topic, or None."""
    if not rules:
        return None
    events = topic_records.select(
        F.from_json(F.col("value").cast("string"), SOURCES[source]).alias("e"),
        F.col("partition").alias("_partition"), F.col("offset").alias("_offset"), F.col("timestamp").alias("_gold_ts"),
    ).select("e.*", "_partition", "_offset", "_gold_ts")  # fmt: skip
    out = None
    for r in rules:
        matched = events.where(compile_predicate(r.predicate, source)).select(
            F.lit(r.topic).alias("topic"),
            F.col("patient_id").alias("key"),
            F.to_json(
                F.struct(
                    # Stable across redeliveries: the subscription revision plus the source record's coordinates.
                    F.sha2(
                        F.concat_ws(":", F.lit(r.subscription_id), F.lit(r.revision), "_partition", "_offset"), 256
                    ).alias("notificationId"),
                    F.lit(r.subscription_id).alias("subscriptionId"),
                    F.lit(r.name).alias("subscription"),
                    F.lit(r.revision).alias("revision"),
                    F.lit(source).alias("source"),
                    F.col("_partition").alias("sourcePartition"),
                    F.col("_offset").alias("sourceOffset"),
                    F.col("_gold_ts").alias("gold_ts"),
                    F.struct(*[F.col(f) for f in r.fields]).alias("payload"),
                ),
                {"ignoreNullFields": "false"},
            ).alias("value"),
        )
        out = matched if out is None else out.unionByName(matched)
    return out
