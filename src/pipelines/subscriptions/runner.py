"""Generic subscription runner: one rule file (hl7.sub.rule) -> topic <catalog>.<schema>.sub_<name> (+ Delta).

rtm:        Real-Time Mode flow to Event Hubs; the lakehouse mirror lands the Delta table. (RTM joins aren't
            supported in pipelines; they run as a Structured Streaming job, hl7rt.subscription_job.)
microbatch: micro-batch flows; the Delta table is an upsert on record_id, and records go to Event Hubs.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from hl7rt import eventhubs, rtm, subscriptions
from hl7rt.config import conf

rule = subscriptions.load(conf(spark, "sub.rule"))
topic = f"{conf(spark, 'topics.prefix')}.{rule.table}"


def _read(table: str):
    t = conf(spark, f"topics.{table}")
    if rule.mode == "rtm":
        return rtm.read_topic(spark, t)
    return (
        spark.readStream.format("kafka")
        .options(**eventhubs.kafka_options_from_conf(spark))
        .option("subscribe", t)
        .option("startingOffsets", conf(spark, "kafka.starting_offsets"))
        .load()
    )


def _records():
    return subscriptions.build(rule, _read)


if rule.mode == "rtm":
    if rule.is_join:
        raise ValueError(f"{rule.name}: RTM joins run as a Structured Streaming job (hl7rt.subscription_job), not here")
    dp.create_sink(rule.table, "kafka", rtm.sink_options(spark, topic))
    dp.update_flow(name=rule.table, target=rule.table, spark_conf=rtm.flow_conf(spark))(_records)

else:
    if "delta" in rule.destinations:
        # At-least-once inputs: upsert on record_id so a re-sent record updates its row.
        feed = f"_{rule.table}_feed"

        def _parsed():
            parsed = F.from_json("value", subscriptions.output_schema(rule)).alias("r")
            keyed = _records().select(F.col("key").cast("string").alias("key"), parsed)
            return keyed.select("key", "r.*", F.current_timestamp().alias("_ingested_at"))

        dp.temporary_view(name=feed)(_parsed)
        dp.create_streaming_table(rule.table, comment=f"Subscription {rule.name} (src/subscriptions/{rule.name}.yml).")
        dp.create_auto_cdc_flow(target=rule.table, source=feed, keys=["record_id"], sequence_by=F.col("gold_ts"),
                                stored_as_scd_type=1)  # fmt: skip

    if "eventhubs" in rule.destinations:
        dp.create_sink(f"{rule.table}_eventhubs", "kafka", rtm.sink_options(spark, topic))
        dp.append_flow(name=f"{rule.table}_to_eventhubs", target=f"{rule.table}_eventhubs")(_records)
