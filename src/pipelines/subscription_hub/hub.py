"""Subscription hub (micro-batch, NOT Real-Time Mode): silver entity topics -> each served app's own topic, by live
rules.

One pipeline per hub and tier (hub/resources/subscription_hub.yml): edp_shared, the platform's, or a team's own hub
(hub/databricks.yml), each serving only the apps assigned to it; edp_shared's deferred pipeline runs every app's
deferred rules. A flow per silver source (hl7.subscriptions.sources; unset, all of them), all sharing one engine
(hl7rt.engine): rules placed when they change, each batch read once, simple rules matched on the driver, the batch's
decisions bound before they're published through a producer per app, so a retried batch decides as its first attempt
did and one app's broken destination never holds up another's.

Rules are Databricks SQL registered at runtime through the Subscription API (src/subscription_api) and stored in the
Lakebase registry; the pipeline graph never changes. foreach_batch_sink is Public Preview (PREVIEW channel); Real-Time
Mode doesn't support foreachBatch, so this hop is seconds, not milliseconds. Delivery is at-least-once with stable
notification IDs.
"""

from pyspark import pipelines as dp

from hl7rt import delivery, eventhubs, subscriptions
from hl7rt.config import conf
from hl7rt.engine import Engine
from hl7rt.registry import Pool, Registry

_NAMESPACE, _CREDENTIAL = conf(spark, "eh.namespace"), conf(spark, "eh.service_credential")
_ENDPOINT, _DATABASE = conf(spark, "registry.endpoint"), conf(spark, "registry.database")
_SOURCES = conf(spark, "subscriptions.sources", ",".join(subscriptions.SOURCES)).split(",")


class _ServiceCredential:
    """The hub's UC service credential as an azure-identity TokenCredential, for the producers' Entra tokens."""

    def get_token(self, scope):
        from databricks.sdk.runtime import dbutils

        return dbutils.credentials.getServiceCredentialsProvider(_CREDENTIAL).get_token(scope)


def _producer(app: str):
    from confluent_kafka import Producer

    return Producer(delivery.producer_config(_NAMESPACE, _ServiceCredential()))


_engine = Engine(
    Registry(
        Pool.lakebase(_ENDPOINT, _DATABASE, int(conf(spark, "registry.connections"))),  # the batch threads'
        conf(spark, "subscriptions.hub"), conf(spark, "subscriptions.tier"), _SOURCES,
        drain_pool=Pool.lakebase(_ENDPOINT, _DATABASE, 1),  # every drainer's, whatever number of apps drain
    ),
    _producer, conf(spark, "subscriptions.on_failure"), int(conf(spark, "subscriptions.chunk")),
)  # fmt: skip


def _query(topic: str, flow: str):
    """This flow's streaming query ID (bindings are keyed by it: a new checkpoint is a new query) and Spark's split of
    its last batch (durationMs: offsets, planning, our batch function, commits)."""
    for q in spark.streams.active:
        try:
            p = q.lastProgress
            if (q.name and q.name.endswith(flow)) or (p and any(topic in s["description"] for s in p["sources"])):
                return str(q.id), dict(p["durationMs"]) if p else None
        except Exception:  # noqa: BLE001 - telemetry only
            continue
    return "unknown", None


def _hub(source: str, topic: str):
    flow = f"subscribe_{source}"

    def match(batch, batch_id):
        query_id, spark_ms = _query(topic, flow)
        _engine.process(batch, batch_id, source, query_id, spark_ms)

    sink = f"subscription_hub_{source}"
    dp.foreach_batch_sink(name=sink)(match)

    @dp.append_flow(target=sink, name=flow)
    def read():
        return (
            spark.readStream.format("kafka")
            .options(
                **eventhubs.hub_read_options(
                    _NAMESPACE, _CREDENTIAL, topic, conf(spark, "subscriptions.max_records_per_batch")
                )
            )  # fmt: skip
            .load()
        )


for _source in _SOURCES:
    _hub(_source, conf(spark, f"topics.{_source}"))
