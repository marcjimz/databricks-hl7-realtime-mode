"""Parked: subscription row builders from src/hl7rt/lakehouse.py (plus SUBSCRIPTION_KEYS = ["record_id"]).
Move back into lakehouse.py to restore."""

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
