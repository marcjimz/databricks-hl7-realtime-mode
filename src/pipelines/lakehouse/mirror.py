"""Near-real-time path (micro-batch, NOT Real-Time Mode): every hot-path topic -> the Delta streaming table of the
same name (topic <catalog>.<schema>.<table> lands in table <catalog>.<schema>.<table>).

RTM can't write Delta, so this separate pipeline lands the topics for audit, replay, BI, and live latency
telemetry (hop_ms / e2e_ms per record, from broker timestamps). Delivery is at-least-once, so every table is an
AUTO CDC upsert on lakehouse.KEYS: a re-sent record updates its row instead of adding a duplicate.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from hl7rt import eventhubs, lakehouse
from hl7rt.config import conf


def _subscribe(topic: str):
    return (
        spark.readStream.format("kafka")
        .options(**eventhubs.kafka_options_from_conf(spark))
        .option("subscribe", topic)
        .option("startingOffsets", conf(spark, "kafka.starting_offsets"))
        .option("maxOffsetsPerTrigger", conf(spark, "kafka.max_offsets_per_trigger"))
        .option("includeHeaders", "true")
        .load()
    )


def _upsert(table: str, topic: str, rows, keys: list[str], comment: str, **table_options):
    """Streaming table `table` = AUTO CDC (SCD type 1) of `rows(topic stream)`, latest arrival wins."""
    feed = f"_{table}_feed"
    dp.temporary_view(name=feed)(lambda: rows(_subscribe(topic)))
    dp.create_streaming_table(table, comment=comment, cluster_by=["kafka_timestamp"], **table_options)
    dp.create_auto_cdc_flow(target=table, source=feed, keys=keys, sequence_by=F.col("kafka_timestamp"),
                            stored_as_scd_type=1)  # fmt: skip


def _layer(table: str, config_key: str, rows, comment: str, **table_options):
    _upsert(table, conf(spark, config_key), rows, lakehouse.KEYS[table], comment, **table_options)


_layer("raw_hl7", "topics.raw", lakehouse.raw_rows, "The raw HL7 topic as received, with producer -> broker latency.")
_layer("bronze_hl7", "topics.bronze", lakehouse.bronze_rows,
       "Bronze: raw message + MSH envelope + raw-topic lineage, hop latency.")  # fmt: skip
_layer("quarantine_hl7", "topics.quarantine", lakehouse.quarantine_rows,
       "Quarantine: messages that could not be parsed, with the reason.")  # fmt: skip
# Silver: one table per entity, each row with its message's context, lineage, and bronze -> silver latency. Delta keeps
# stats on the first 32 columns, and observations put kafka_timestamp past #32.
_STATS = {
    "delta.dataSkippingStatsColumns": "kafka_timestamp,msg_dt_tm,msg_type,patient_id,e2e_ms,raw_partition,raw_offset"
}
_layer("silver_messages", "topics.silver_messages", lambda df: lakehouse.silver_rows(df, "silver_messages"),
       "Silver messages: one row per parsed message (header, patient, visit, raw payload).",
       table_properties=_STATS)  # fmt: skip
_layer("silver_encounters", "topics.silver_encounters", lambda df: lakehouse.silver_rows(df, "silver_encounters"),
       "Silver encounters: one row per ADT event.", table_properties=_STATS)  # fmt: skip
_layer("silver_observations", "topics.silver_observations",
       lambda df: lakehouse.silver_rows(df, "silver_observations"),
       "Silver observations: one row per OBX result.", table_properties=_STATS)  # fmt: skip
_layer("silver_orders", "topics.silver_orders", lambda df: lakehouse.silver_rows(df, "silver_orders"),
       "Silver orders: one row per ORM order.", table_properties=_STATS)  # fmt: skip
_layer("silver_diagnoses", "topics.silver_diagnoses", lambda df: lakehouse.silver_rows(df, "silver_diagnoses"),
       "Silver diagnoses: one row per DG1 diagnosis.", table_properties=_STATS)  # fmt: skip
