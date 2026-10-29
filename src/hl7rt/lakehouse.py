"""Near-real-time path: every hot-path topic -> Delta streaming table rows, with per-record latency telemetry.

Telemetry uses only Event Hubs broker timestamps (LogAppendTime) carried forward by the hot path, so the
mirror's own (micro-batch) lag never affects the numbers:

    hop_ms = this topic's broker timestamp - the upstream topic's broker timestamp
    e2e_ms = this topic's broker timestamp - the raw topic's broker timestamp

Resolution is about +/-16 ms (Event Hubs timestamp tick). See docs/latency.md.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from hl7rt import hl7v2

# Upsert keys. Spark's Kafka sink is at-least-once: after a restart a hop can re-send records. Every table is an
# AUTO CDC (SCD type 1) target keyed by the record's raw-topic coordinates, so a replay updates its row, not adds one.
KEYS = {
    "raw_hl7": ["partition", "offset"],
    "bronze_hl7": ["raw_partition", "raw_offset"],
    "silver_hl7": ["raw_partition", "raw_offset"],
    "quarantine_hl7": ["raw_partition", "raw_offset"],
    "gold_encounters": ["raw_partition", "raw_offset"],
    "gold_observations": ["raw_partition", "raw_offset", "set_id"],
    "gold_orders": ["raw_partition", "raw_offset"],
}
SUBSCRIPTION_KEYS = ["record_id"]


def _ms(later: Column, earlier: Column) -> Column:
    return (F.unix_micros(later) - F.unix_micros(earlier)) / 1000.0


def _value(schema) -> Column:
    return F.from_json(F.col("value").cast("string"), schema)


def raw_rows(raw: DataFrame) -> DataFrame:
    """raw_hl7 topic: the message as received; hop = producer send time (sent_ms header) -> broker."""
    sent_ms = F.expr("try_cast(cast(filter(headers, h -> h.key = 'sent_ms')[0].value AS STRING) AS BIGINT)")
    sent = F.timestamp_millis(sent_ms)
    return raw.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("hl7"),
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        sent.alias("sent_ts"),
        _ms(F.col("timestamp"), sent).alias("hop_ms"),
        F.current_timestamp().alias("_ingested_at"),
    )


def bronze_rows(bronze: DataFrame) -> DataFrame:
    b = bronze.select(_value(hl7v2.BRONZE_SCHEMA).alias("b"), F.col("timestamp").alias("kafka_timestamp"))
    return b.select(
        "b.*",
        "kafka_timestamp",
        _ms(F.col("kafka_timestamp"), F.col("b.raw_ts")).alias("hop_ms"),
        _ms(F.col("kafka_timestamp"), F.col("b.raw_ts")).alias("e2e_ms"),
        F.current_timestamp().alias("_ingested_at"),
    )


def silver_rows(silver: DataFrame) -> DataFrame:
    """Customer silver contract first (34 fields, payload, kafka_timestamp, _ingested_at), then extras."""
    s = silver.select(_value(hl7v2.SILVER_SCHEMA).alias("s"), F.col("timestamp").alias("kafka_timestamp"))
    return s.select(
        *[F.col(f"s.{n}") for n in hl7v2.SILVER_FIELDS],
        F.col("s.payload"),
        "kafka_timestamp",
        F.current_timestamp().alias("_ingested_at"),
        F.col("s.observations"),
        F.col("s.raw_ts"),
        F.col("s.bronze_ts"),
        F.col("s.raw_partition"),
        F.col("s.raw_offset"),
        _ms(F.col("kafka_timestamp"), F.col("s.bronze_ts")).alias("hop_ms"),
        _ms(F.col("kafka_timestamp"), F.col("s.raw_ts")).alias("e2e_ms"),
    )


def quarantine_rows(quarantine: DataFrame) -> DataFrame:
    q = quarantine.select(_value(hl7v2.QUARANTINE_SCHEMA).alias("q"), F.col("timestamp").alias("kafka_timestamp"))
    return q.select(
        "q.*",
        "kafka_timestamp",
        _ms(F.col("kafka_timestamp"), F.col("q.bronze_ts")).alias("hop_ms"),
        F.current_timestamp().alias("_ingested_at"),
    )


def gold_rows(gold_topic: DataFrame, schema) -> DataFrame:
    """gold_* topics: the entity's fields (gold.*_SCHEMA order), then hop (silver -> gold) and end-to-end latency,
    and `_meta` (record id + timestamps): the struct subscription SQL selects, so a rule's query also runs in a SQL
    editor against these tables."""
    g = gold_topic.select(_value(schema).alias("g"), F.col("timestamp").alias("kafka_timestamp"))
    ids = [F.col("g.raw_partition"), F.col("g.raw_offset")]
    ids += [F.col("g.set_id")] if "set_id" in schema.fieldNames() else []
    meta = F.struct(
        F.concat_ws("-", *ids).alias("record_id"),
        F.col("kafka_timestamp").alias("gold_ts"),
        F.col("g.raw_ts").alias("raw_ts"),
    )
    return g.select(
        "g.*",
        "kafka_timestamp",
        _ms(F.col("kafka_timestamp"), F.col("g.silver_ts")).alias("hop_ms"),
        _ms(F.col("kafka_timestamp"), F.col("g.raw_ts")).alias("e2e_ms"),
        F.current_timestamp().alias("_ingested_at"),
        meta.alias("_meta"),
    )


def subscription_rows(sub_topic: DataFrame, schema) -> DataFrame:
    """sub_*: the subscription record (subscriptions.output_schema), then gold -> sub and end-to-end latency."""
    s = sub_topic.select(F.col("key").cast("string").alias("key"), _value(schema).alias("s"),
                         F.col("timestamp").alias("kafka_timestamp"))  # fmt: skip
    return s.select(
        "key",
        "s.*",
        "kafka_timestamp",
        _ms(F.col("kafka_timestamp"), F.col("s.gold_ts")).alias("hop_ms"),
        _ms(F.col("kafka_timestamp"), F.col("s.raw_ts")).alias("e2e_ms"),
        F.current_timestamp().alias("_ingested_at"),
    )


_LATENCY_SCHEMA = "record_id STRING, gold_ts TIMESTAMP, raw_ts TIMESTAMP"


def subscription_latency_rows(sub_topic: DataFrame, subscription: str, mode: str) -> DataFrame:
    """One row per record on any subscription topic, so the dashboard picks up new subscriptions by itself."""
    s = sub_topic.select(F.from_json(F.col("value").cast("string"), _LATENCY_SCHEMA).alias("s"),
                         F.col("timestamp").alias("kafka_timestamp"))  # fmt: skip
    return s.select(
        F.lit(subscription).alias("subscription"),
        F.lit(mode).alias("mode"),
        "s.record_id",
        "kafka_timestamp",
        _ms(F.col("kafka_timestamp"), F.col("s.gold_ts")).alias("hop_ms"),
        _ms(F.col("kafka_timestamp"), F.col("s.raw_ts")).alias("e2e_ms"),
        F.current_timestamp().alias("_ingested_at"),
    )
