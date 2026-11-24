"""Near-real-time path: every hot-path topic -> Delta streaming table rows, with per-record latency telemetry.

Telemetry uses only Event Hubs broker timestamps (LogAppendTime) carried forward by the hot path, so the
mirror's own (micro-batch) lag never affects the numbers:

    hop_ms = this topic's broker timestamp - the upstream topic's broker timestamp
    e2e_ms = this topic's broker timestamp - the raw topic's broker timestamp

Resolution is about +/-16 ms (Event Hubs timestamp tick). See docs/latency.md.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from hl7rt import hl7v2, silver

# Upsert keys. Spark's Kafka sink is at-least-once: after a restart a hop can re-send records. Every table is an
# AUTO CDC (SCD type 1) target keyed by the record's raw-topic coordinates, so a replay updates its row, not adds one.
KEYS = {
    "raw_hl7": ["partition", "offset"],
    "bronze_hl7": ["raw_partition", "raw_offset"],
    "silver_messages": ["raw_partition", "raw_offset"],
    "silver_encounters": ["raw_partition", "raw_offset"],
    "silver_observations": ["raw_partition", "raw_offset", "position"],
    "silver_orders": ["raw_partition", "raw_offset"],
    "silver_diagnoses": ["raw_partition", "raw_offset", "position"],
    "quarantine_hl7": ["raw_partition", "raw_offset"],
}


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


def quarantine_rows(quarantine: DataFrame) -> DataFrame:
    q = quarantine.select(_value(hl7v2.QUARANTINE_SCHEMA).alias("q"), F.col("timestamp").alias("kafka_timestamp"))
    return q.select(
        "q.*",
        "kafka_timestamp",
        _ms(F.col("kafka_timestamp"), F.col("q.bronze_ts")).alias("hop_ms"),
        F.current_timestamp().alias("_ingested_at"),
    )


def silver_rows(topic: DataFrame, table: str) -> DataFrame:
    """silver_* topics: the entity's fields (silver.SCHEMAS order), then hop (bronze -> silver) and end-to-end latency,
    and `_meta` (record id + timestamps), a stable record identity for downstream consumers."""
    schema = silver.SCHEMAS[table]
    r = topic.select(_value(schema).alias("r"), F.col("timestamp").alias("kafka_timestamp"))
    meta = F.struct(
        F.concat_ws("-", *[F.col(f"r.{k}") for k in KEYS[table]]).alias("record_id"),
        F.col("kafka_timestamp").alias("source_ts"),
        F.col("r.raw_ts").alias("raw_ts"),
    )
    return r.select(
        "r.*",
        "kafka_timestamp",
        _ms(F.col("kafka_timestamp"), F.col("r.bronze_ts")).alias("hop_ms"),
        _ms(F.col("kafka_timestamp"), F.col("r.raw_ts")).alias("e2e_ms"),
        F.current_timestamp().alias("_ingested_at"),
        meta.alias("_meta"),
    )
