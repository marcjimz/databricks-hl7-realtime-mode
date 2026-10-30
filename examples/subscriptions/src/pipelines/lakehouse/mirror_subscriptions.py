"""Parked: the part of src/pipelines/lakehouse/mirror.py that lands subscription topics. Append it to mirror.py
(and `from pathlib import Path`, `subscriptions` imports) to restore."""

# Subscriptions (src/subscriptions/*.sql, *.yml). RTM rules land in their own table here; micro-batch ones write theirs.
# Every rule's topic also feeds subscription_latency, so the dashboard shows new subscriptions without edits.
dp.create_streaming_table(
    "subscription_latency",
    cluster_by=["kafka_timestamp"],
    comment="One row per subscription record: gold -> subscription and end-to-end latency.",
)


def _subscription(rule):
    topic = f"{conf(spark, 'topics.prefix')}.{rule.table}"
    if rule.mode == "rtm" and "delta" in rule.destinations:
        schema = subscriptions.output_schema(rule, spark)
        _upsert(rule.table, topic, lambda df: lakehouse.subscription_rows(df, schema), lakehouse.SUBSCRIPTION_KEYS,
                f"Subscription {rule.name} (src/subscriptions/{rule.name}.yml), with latency.")  # fmt: skip
    if "eventhubs" in rule.destinations:
        dp.append_flow(name=f"{rule.table}_latency", target="subscription_latency")(
            lambda: lakehouse.subscription_latency_rows(_subscribe(topic), rule.name, rule.mode)
        )


_rules_dir = Path(conf(spark, "subscriptions.dir"))
for _path in sorted(p for p in _rules_dir.glob("[a-z]*.*") if p.suffix in (".yml", ".sql")):
    _subscription(subscriptions.load(_path))
