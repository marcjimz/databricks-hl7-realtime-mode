"""Real-Time Mode subscription as a Structured Streaming job, for rules Lakeflow pipelines can't run in RTM.

Lakeflow pipelines don't support stream-stream joins in Real-Time Mode, but Structured Streaming does (DBR 18 LTS+
with subscriptions.RTM_JOIN_CONF on the cluster). So `when` + `then` rules in `rtm` mode run here: same rule file,
same subscriptions.build(), same Kafka wiring (hl7rt.rtm), same output topic. Job parameters arrive as
`--conf key=value` and are set as `hl7.<key>` Spark conf, so hl7rt.config.conf() works exactly as in pipelines.
"""

import argparse

from hl7rt import rtm, subscriptions
from hl7rt.config import PREFIX, conf


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rule", required=True, help="workspace path to src/subscriptions/<name>.yml")
    p.add_argument("--checkpoint", required=True, help="checkpoint location (a UC volume path)")
    p.add_argument("--conf", action="append", default=[], help="key=value, set as Spark conf hl7.<key>")
    return p.parse_args(argv)


def hl7_conf(args: argparse.Namespace) -> dict[str, str]:
    out = {}
    for item in args.conf:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise ValueError(f"--conf must be key=value, got {item!r}")
        out[PREFIX + key] = value
    return out


def check(rule: subscriptions.Rule) -> None:
    if rule.mode != "rtm" or not rule.is_join:
        raise ValueError(
            f"{rule.name}: only rtm rules with `then` run as a job; others run in a pipeline (make sub-new)"
        )


def main(argv: list[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    for key, value in {**hl7_conf(args), **subscriptions.RTM_JOIN_CONF}.items():
        spark.conf.set(key, value)
    rule = subscriptions.load(args.rule)
    check(rule)
    spark.conf.set("spark.sql.shuffle.partitions", conf(spark, "rtm.shuffle_partitions"))
    topic = f"{conf(spark, 'topics.prefix')}.{rule.table}"
    records = subscriptions.build(rule, lambda table: rtm.read_topic(spark, conf(spark, f"topics.{table}")))
    query = (
        records.writeStream.format("kafka")
        .options(**rtm.sink_options(spark, topic))
        .option("checkpointLocation", args.checkpoint)
        .outputMode("update")
        .trigger(realTime=conf(spark, "rtm.trigger_interval"))
        .queryName(rule.table)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
