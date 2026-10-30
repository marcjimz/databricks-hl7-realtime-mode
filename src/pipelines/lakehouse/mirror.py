"""Near-real-time path (micro-batch, NOT Real-Time Mode): every hot-path topic -> the Delta streaming table of the
same name (topic <catalog>.<schema>.<table> lands in table <catalog>.<schema>.<table>).

RTM can't write Delta, so this separate pipeline lands the topics for audit, replay, BI, and live latency
telemetry (hop_ms / e2e_ms per record, from broker timestamps). Delivery is at-least-once, so every table is an
AUTO CDC upsert on lakehouse.KEYS: a re-sent record updates its row instead of adding a duplicate.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from hl7rt import eventhubs, gold, lakehouse
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
_layer(
    "silver_hl7",
    "topics.silver",
    lakehouse.silver_rows,
    "Silver: parsed HL7 in the Epic silver column contract, plus observations (one per OBX), lineage, and hop / "
    "end-to-end latency.",
    # Delta collects stats on the first 32 columns by default; the contract puts kafka_timestamp at #36.
    table_properties={
        "delta.dataSkippingStatsColumns": "kafka_timestamp,msg_dt_tm,msg_control_id,msg_type,patient_id,e2e_ms,"
        "raw_partition,raw_offset"
    },
)
_layer("quarantine_hl7", "topics.quarantine", lakehouse.quarantine_rows,
       "Quarantine: messages that could not be silvered, with the reason.")  # fmt: skip
_layer("gold_encounters", "topics.gold_encounters", lambda df: lakehouse.gold_rows(df, gold.ENCOUNTER_SCHEMA),
       "Gold encounters: one row per ADT event, with silver -> gold latency.")  # fmt: skip
_layer("gold_observations", "topics.gold_observations", lambda df: lakehouse.gold_rows(df, gold.OBSERVATION_SCHEMA),
       "Gold observations: one row per OBX result, with latency.")  # fmt: skip
_layer("gold_orders", "topics.gold_orders", lambda df: lakehouse.gold_rows(df, gold.ORDER_SCHEMA),
       "Gold orders: one row per ORM order, with latency.")  # fmt: skip
