"""Generic subscription runner: one rule file (hl7.sub.rule) -> topic <catalog>.<schema>.sub_<name> (+ Delta).

rtm:        Real-Time Mode flow to Event Hubs; the lakehouse mirror lands the Delta table.
microbatch: micro-batch flows; the Delta table is written directly and/or forwarded to Event Hubs.
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
    extra = {"spark.sql.shuffle.partitions": conf(spark, "rtm.shuffle_partitions")} if rule.is_join else None
    dp.create_sink(rule.table, "kafka", rtm.sink_options(spark, topic))
    dp.update_flow(name=rule.table, target=rule.table, spark_conf=rtm.flow_conf(spark, extra))(_records)

else:
    if "delta" in rule.destinations:

        @dp.table(name=rule.table, comment=f"Subscription {rule.name} (src/subscriptions/{rule.name}.yml).")
        def _table():
            parsed = F.from_json("value", subscriptions.output_schema(rule)).alias("r")
            keyed = _records().select(F.col("key").cast("string").alias("key"), parsed)
            return keyed.select("key", "r.*", F.current_timestamp().alias("_ingested_at"))

    if "eventhubs" in rule.destinations:
        dp.create_sink(f"{rule.table}_eventhubs", "kafka", rtm.sink_options(spark, topic))

        def _to_kafka():
            if "delta" not in rule.destinations:
                return _records()
            t = spark.readStream.table(rule.table)
            fields = [c for c in t.columns if c not in ("key", "_ingested_at")]
            return t.select("key", F.to_json(F.struct(*fields)).alias("value"))

        dp.append_flow(name=f"{rule.table}_to_eventhubs", target=f"{rule.table}_eventhubs")(_to_kafka)
