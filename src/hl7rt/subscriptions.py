"""Subscription hub matching: every app's SQL rule, applied to a micro-batch of one gold topic, in one pass.

A rule is Databricks SQL (`SELECT <fields> FROM <source> WHERE <condition>`), compiled by Databricks against the app's
own views when it was registered (src/subscription_api). The hub evaluates each stored condition per record with Spark,
so the same SQL in the SQL editor selects the same records. Before it does, it re-checks each condition
(subscription_api.rules.check_condition): one filter, no functions that reach outside the record, granted columns only.

All of a source's rules run in one projection per batch: an array of the matching rules per record, then explode. That
is one scan and one plan however many rules there are, in chunks small enough to stay under the JVM's 64 KB method
limit. A rule that fails at runtime (a cast of bad data, say) is found and reported FAILED alone; the others keep
running.

Each match becomes a notification on the app's own topic, carrying only the fields its rule selects.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from hl7rt import gold
from subscription_api.rules import RuleError, check_condition

SOURCES: dict[str, StructType] = {
    "gold_encounters": gold.ENCOUNTER_SCHEMA,
    "gold_observations": gold.OBSERVATION_SCHEMA,
    "gold_orders": gold.ORDER_SCHEMA,
}
CHUNK = 200  # rules per projection


@dataclass(frozen=True)
class Rule:
    """One enabled subscription revision, as the hub runs it."""

    subscription_id: str
    name: str
    revision: int
    source: str
    where: str | None
    fields: list[str]
    topic: str
    granted: frozenset[str]


def runnable(rules: list[Rule]) -> tuple[list[Rule], dict[str, str]]:
    """The rules that pass the hub's own check, and the error for each that doesn't (subscription id -> message)."""
    good, failed = [], {}
    for r in rules:
        try:
            if not set(r.fields) <= r.granted:
                raise RuleError("FIELDS_DENIED", f"{sorted(set(r.fields) - r.granted)} not granted to this app")
            check_condition(r.where, set(r.granted))
            good.append(r)
        except RuleError as e:
            failed[r.subscription_id] = str(e)
    return good, failed


def events(topic_records: DataFrame, source: str) -> DataFrame:
    """A micro-batch of `source`'s topic as records, with the coordinates notifications carry."""
    return topic_records.select(
        F.from_json(F.col("value").cast("string"), SOURCES[source]).alias("e"),
        F.col("partition").alias("_partition"), F.col("offset").alias("_offset"), F.col("timestamp").alias("_gold_ts"),
    ).select("e.*", "_partition", "_offset", "_gold_ts")  # fmt: skip


def _condition(r: Rule):
    return F.expr(r.where) if r.where else F.lit(True)


def notifications(records: DataFrame, source: str, rules: list[Rule]) -> DataFrame | None:
    """Kafka records (topic, key, value) for every rule's matches in a batch of `events(...)`, or None."""
    out = None
    for start in range(0, len(rules), CHUNK):
        chunk = rules[start : start + CHUNK]
        hits = F.array_compact(F.array(*[
            F.when(_condition(r), F.struct(
                F.lit(i).alias("i"),
                F.to_json(F.struct(*[F.col(f) for f in r.fields]), {"ignoreNullFields": "false"}).alias("payload"),
            ))
            for i, r in enumerate(chunk)
        ]))  # fmt: skip
        meta = F.element_at(F.array(*[
            F.struct(F.lit(r.topic).alias("topic"), F.lit(r.subscription_id).alias("sid"),
                     F.lit(r.name).alias("name"), F.lit(r.revision).alias("rev"))
            for r in chunk
        ]), F.col("h.i") + 1)  # fmt: skip
        matched = records.select("patient_id", "_partition", "_offset", "_gold_ts", F.explode(hits).alias("h")).select(
            meta["topic"].alias("topic"),
            F.col("patient_id").alias("key"),
            F.to_json(
                F.struct(
                    # Stable across redeliveries: the subscription revision plus the source record's coordinates.
                    F.sha2(F.concat_ws(":", meta["sid"], meta["rev"], "_partition", "_offset"), 256).alias(
                        "notificationId"
                    ),
                    meta["sid"].alias("subscriptionId"),
                    meta["name"].alias("subscription"),
                    meta["rev"].alias("revision"),
                    F.lit(source).alias("source"),
                    F.col("_partition").alias("sourcePartition"),
                    F.col("_offset").alias("sourceOffset"),
                    F.col("_gold_ts").alias("gold_ts"),
                    F.parse_json("h.payload").alias("payload"),
                )
            ).alias("value"),
        )
        out = matched if out is None else out.unionByName(matched)
    return out


def failing(records: DataFrame, rules: list[Rule]) -> dict[str, str]:
    """After a batch failed: which rules fail on this batch's records, with Spark's error (subscription id -> message).
    Evaluates each rule over every record, so only when a batch failed."""
    errors = {}
    for r in rules:
        try:
            records.select(F.count(F.when(_condition(r), 1))).collect()
        except Exception as e:  # noqa: BLE001 - Spark's error is the answer
            errors[r.subscription_id] = str(e).strip().splitlines()[0][:1000]
    return errors
