"""Near-real-time path (micro-batch, NOT Real-Time Mode): every hot-path topic -> the Delta streaming table of the
same name (topic <catalog>.<schema>.<table> lands in table <catalog>.<schema>.<table>).

RTM can't write Delta, so this separate pipeline lands the topics for audit, replay, BI, and live latency
telemetry (hop_ms / e2e_ms per record, from broker timestamps).
"""

from pathlib import Path

from pyspark import pipelines as dp

from hl7rt import eventhubs, gold, lakehouse, subscriptions
from hl7rt.config import conf


def _topic(key: str):
    return _subscribe(conf(spark, key))


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


@dp.table(name="raw_hl7", comment="The raw HL7 topic as received, with producer -> Event Hubs latency.",
          cluster_by=["kafka_timestamp"])  # fmt: skip
def raw_hl7():
    return lakehouse.raw_rows(_topic("topics.raw"))


@dp.table(name="bronze_hl7", comment="Bronze: raw message + MSH envelope + raw-topic lineage, hop latency.",
          cluster_by=["kafka_timestamp"])  # fmt: skip
def bronze_hl7():
    return lakehouse.bronze_rows(_topic("topics.bronze"))


@dp.table(
    name="silver_hl7",
    comment="Silver: parsed HL7 in the Epic silver column contract, plus observations (one per OBX), "
    "lineage, and hop / end-to-end latency.",
    cluster_by=["kafka_timestamp"],
    # Delta collects stats on the first 32 columns by default; the contract puts kafka_timestamp at #36.
    table_properties={
        "delta.dataSkippingStatsColumns": "kafka_timestamp,msg_dt_tm,msg_control_id,msg_type,patient_id,e2e_ms"
    },
)
def silver_hl7():
    return lakehouse.silver_rows(_topic("topics.silver"))


@dp.table(name="quarantine_hl7", comment="Quarantine: messages that could not be silvered, with the reason.",
          cluster_by=["kafka_timestamp"])  # fmt: skip
def quarantine_hl7():
    return lakehouse.quarantine_rows(_topic("topics.quarantine"))


@dp.table(name="gold_encounters", comment="Gold encounters: one row per ADT event, with silver -> gold latency.",
          cluster_by=["kafka_timestamp"])  # fmt: skip
def gold_encounters():
    return lakehouse.gold_rows(_topic("topics.gold_encounters"), gold.ENCOUNTER_SCHEMA)


@dp.table(name="gold_observations", comment="Gold observations: one row per OBX result, with latency.",
          cluster_by=["kafka_timestamp"])  # fmt: skip
def gold_observations():
    return lakehouse.gold_rows(_topic("topics.gold_observations"), gold.OBSERVATION_SCHEMA)


@dp.table(name="gold_orders", comment="Gold orders: one row per ORM order, with latency.",
          cluster_by=["kafka_timestamp"])  # fmt: skip
def gold_orders():
    return lakehouse.gold_rows(_topic("topics.gold_orders"), gold.ORDER_SCHEMA)


# Every Real-Time Mode subscription rule (src/subscriptions/*.yml) lands its topic in the table of the same name.
def _subscription_table(rule):
    @dp.table(name=rule.table, comment=f"Subscription {rule.name} (src/subscriptions/{rule.name}.yml), with latency.",
              cluster_by=["kafka_timestamp"])  # fmt: skip
    def _mirror():
        topic = f"{conf(spark, 'topics.prefix')}.{rule.table}"
        return lakehouse.subscription_rows(_subscribe(topic), subscriptions.output_schema(rule))


for _path in sorted(Path(conf(spark, "subscriptions.dir")).glob("[a-z]*.yml")):
    _rule = subscriptions.load(_path)
    if _rule.mode == "rtm" and "delta" in _rule.destinations:
        _subscription_table(_rule)
