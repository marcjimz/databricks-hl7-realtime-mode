"""Subscription hub (micro-batch, NOT Real-Time Mode): every gold topic -> each app's own topic, by its live rules.

Rules are Databricks SQL registered at runtime through the Subscription API (src/subscription_api) and stored in the
Lakebase registry; the pipeline graph never changes. Each micro-batch:
  1. check the registry's generation (one indexed query) and reload the rules if it moved,
  2. evaluate this source's compiled rule set in one pass and write the matches to each app's topic (the Kafka sink
     routes by the `topic` column). If the batch fails, find the rules that fail on it, report them FAILED, and write
     the rest,
  3. record ACTIVE / CANCELLED / FAILED per revision, a heartbeat, and where the batch's time went (hub_progress).

Compiling a rule set (checking every rule, building its Spark expressions) happens once per registry change, in a
background thread: batches keep using the last compiled set meanwhile, so a change never blocks delivery. Only a
source's first set is compiled in the batch itself.

foreach_batch_sink is Public Preview (PREVIEW channel); Real-Time Mode doesn't support foreachBatch, so this hop is
seconds, not milliseconds. Delivery is at-least-once with stable notification IDs.
"""

import threading
import time

from pyspark import pipelines as dp

from hl7rt import eventhubs, subscriptions
from hl7rt.config import conf
from hl7rt.registry import Registry

_registry = Registry(conf(spark, "registry.endpoint"), conf(spark, "registry.database"))
_CHUNK = int(conf(spark, "subscriptions.chunk"))  # rules per projection: small enough for Spark to compile and cache it


class _RuleSet:
    """A source's compiled rules, rebuilt in the background whenever the registry generation moves."""

    def __init__(self, source: str):
        self.source, self.current, self._building = source, None, False
        self._lock = threading.Lock()

    def get(self, generation) -> tuple[subscriptions.Compiled, bool]:
        """The set to use for this batch, and whether it was compiled in the batch (only a source's first)."""
        if self.current is None:
            self.current = self._compile(generation)
            return self.current, True
        with self._lock:
            if self.current.generation != generation and not self._building:
                self._building = True
                threading.Thread(target=self._rebuild, args=(generation,), daemon=True).start()
        return self.current, False

    def drop(self, failed: set[str]):
        """Rules found failing at runtime leave the set at once (they're FAILED until a new revision)."""
        self.current = self._compile(self.current.generation, exclude=failed)

    def _compile(self, generation, exclude=frozenset()):
        rules = [r for r in _registry.rules(self.source) if r.subscription_id not in exclude]
        return subscriptions.compile_rules(rules, generation, _CHUNK, spark)

    def _rebuild(self, generation):
        try:
            self.current = self._compile(generation)
        finally:
            with self._lock:
                self._building = False


def _write(records, source, rule_set):
    out = subscriptions.notifications(records, source, rule_set)
    if out is not None:
        out.write.format("kafka").options(**eventhubs.kafka_options_from_conf(spark)).save()


def _hub(source: str, topic: str):
    rule_set = _RuleSet(source)

    def match(batch, batch_id):
        t0 = time.monotonic()
        _registry.refresh()
        t1 = time.monotonic()
        compiled, compiled_now = rule_set.get(_registry.generation)
        t2 = time.monotonic()
        # Read the micro-batch once, into memory (it's bounded by maxOffsetsPerTrigger): every chunk of rules is then
        # evaluated against local rows, not a fresh read of the topic per chunk.
        parsed = subscriptions.events(batch, source)
        rows = parsed.collect()
        records = spark.createDataFrame(rows, parsed.schema) if rows else None
        failed = dict(compiled.failed)
        try:
            if records is not None:
                _write(records, source, compiled)
        except Exception:  # noqa: BLE001 - one rule's runtime error must not stop every app's delivery
            failed |= subscriptions.failing(records, compiled.rules)
            rule_set.drop(set(failed))
            compiled = rule_set.current
            _write(records, source, compiled)
        t3 = time.monotonic()
        revisions = {r.subscription_id: r.revision for r in _registry.rules(source)}
        for subscription_id, error in failed.items():
            if subscription_id in revisions:
                _registry.report(subscription_id, revisions[subscription_id], "FAILED", error)
        for r in compiled.rules:
            _registry.report(r.subscription_id, r.revision, "ACTIVE")
        for subscription_id, revision in _registry.cancelled(source):
            _registry.report(subscription_id, revision, "CANCELLED")
        _registry.heartbeat(source, batch_id, len(compiled.rules))
        t4 = time.monotonic()
        ms = {"refresh": t1 - t0, "prepare": (t2 - t1) if compiled_now else 0, "write": t3 - t2, "report": t4 - t3}
        ms = {k: int(v * 1000) for k, v in ms.items()} | {"compile": compiled.compile_ms}
        _registry.progress(source, batch_id, len(compiled.rules), ms)

    sink = f"subscription_hub_{source}"
    dp.foreach_batch_sink(name=sink)(match)

    @dp.append_flow(target=sink, name=f"subscribe_{source}")
    def read():
        return (
            spark.readStream.format("kafka")
            .options(**eventhubs.kafka_options_from_conf(spark))
            .option("subscribe", topic)
            .option("startingOffsets", "latest")  # a new checkpoint starts at the live edge; then it resumes
            .option(
                "maxOffsetsPerTrigger", conf(spark, "subscriptions.max_records_per_batch")
            )  # bounds what's collected
            .load()
        )


for _source in subscriptions.SOURCES:
    _hub(_source, conf(spark, f"topics.{_source}"))
