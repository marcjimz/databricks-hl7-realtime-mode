"""Subscription hub (micro-batch, NOT Real-Time Mode): every gold topic -> each app's own topic, by its live rules.

Rules live in the Lakebase registry and change at runtime; the pipeline graph never does. Each micro-batch:
  1. reload the registry if its generation moved (one indexed query otherwise),
  2. build each rule's Spark filter from its structured predicate (hl7rt.subscriptions; no SQL text from apps),
  3. write the matches to each app's topic (Kafka sink routes by the `topic` column),
  4. record ACTIVE / CANCELLED / FAILED per revision and a heartbeat for this source.

foreach_batch_sink is Public Preview (PREVIEW channel); Real-Time Mode doesn't support foreachBatch, so this hop is
seconds, not milliseconds. Delivery is at-least-once with stable notification IDs.
"""

from pyspark import pipelines as dp

from hl7rt import eventhubs, subscriptions
from hl7rt.config import conf
from hl7rt.registry import Registry

_registry = Registry(conf(spark, "registry.endpoint"), conf(spark, "registry.database"))


def _hub(source: str, topic: str):
    def match(batch, batch_id):
        _registry.refresh()
        rules = []
        for r in _registry.rules(source):
            try:
                subscriptions.compile_predicate(r.predicate, source)  # validated at registration; re-checked here
                rules.append(r)
                _registry.report(r.subscription_id, r.revision, "ACTIVE")
            except subscriptions.PredicateError as e:
                _registry.report(r.subscription_id, r.revision, "FAILED", str(e))
        for subscription_id, revision in _registry.cancelled(source):
            _registry.report(subscription_id, revision, "CANCELLED")
        out = subscriptions.notifications(batch, source, rules)
        if out is not None:
            out.write.format("kafka").options(**eventhubs.kafka_options_from_conf(spark)).save()
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
