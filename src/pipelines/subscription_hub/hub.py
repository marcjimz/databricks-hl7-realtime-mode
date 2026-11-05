"""Subscription hub (micro-batch, NOT Real-Time Mode): every gold topic -> each app's own topic, by its live rules.

Rules are Databricks SQL registered at runtime through the Subscription API (src/subscription_api) and stored in the
Lakebase registry; the pipeline graph never changes. Each micro-batch:
  1. reload the registry if its generation moved (one indexed query otherwise),
  2. re-check each rule (one filter, granted columns, no outside-the-record functions); a rule that fails is FAILED,
  3. evaluate all of this source's rules in one pass and write the matches to each app's topic (Kafka sink routes by
     the `topic` column). If the batch fails, find the rules that fail on it, report them FAILED, and write the rest,
  4. record ACTIVE / CANCELLED / FAILED per revision and a heartbeat for this source.

foreach_batch_sink is Public Preview (PREVIEW channel); Real-Time Mode doesn't support foreachBatch, so this hop is
seconds, not milliseconds. Delivery is at-least-once with stable notification IDs.
"""

from pyspark import pipelines as dp

from hl7rt import eventhubs, subscriptions
from hl7rt.config import conf
from hl7rt.registry import Registry

_registry = Registry(conf(spark, "registry.endpoint"), conf(spark, "registry.database"))


def _write(records, source, rules):
    out = subscriptions.notifications(records, source, rules)
    if out is not None:
        out.write.format("kafka").options(**eventhubs.kafka_options_from_conf(spark)).save()


def _hub(source: str, topic: str):
    def match(batch, batch_id):
        _registry.refresh()
        rules, failed = subscriptions.runnable(_registry.rules(source))
        records = subscriptions.events(batch, source)
        try:
            _write(records, source, rules)
        except Exception:  # noqa: BLE001 - one rule's runtime error must not stop every app's delivery
            failed |= subscriptions.failing(records, rules)
            rules = [r for r in rules if r.subscription_id not in failed]
            _write(records, source, rules)
        by_id = {r.subscription_id: r for r in _registry.rules(source)}
        for subscription_id, error in failed.items():
            _registry.report(subscription_id, by_id[subscription_id].revision, "FAILED", error)
        for r in rules:
            _registry.report(r.subscription_id, r.revision, "ACTIVE")
        for subscription_id, revision in _registry.cancelled(source):
            _registry.report(subscription_id, revision, "CANCELLED")
        _registry.heartbeat(source, batch_id, len(rules))

    sink = f"subscription_hub_{source}"
    dp.foreach_batch_sink(name=sink)(match)

    @dp.append_flow(target=sink, name=f"subscribe_{source}")
    def read():
        return (
            spark.readStream.format("kafka")
            .options(**eventhubs.kafka_options_from_conf(spark))
            .option("subscribe", topic)
            .option("startingOffsets", "latest")  # a new checkpoint starts at the live edge; then it resumes
            .load()
        )


for _source in subscriptions.SOURCES:
    _hub(_source, conf(spark, f"topics.{_source}"))
