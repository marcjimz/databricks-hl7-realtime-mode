"""Real-Time Mode wiring shared by every hot-path hop: Kafka source, Kafka sink, and flow trigger settings.

All values come from pipeline configuration (hl7.*), set by the bundle. See AGENTS.md "Real-Time Mode rules".
"""

from pyspark.sql import DataFrame, SparkSession

from hl7rt import eventhubs
from hl7rt.config import conf

# Latency over batching on the producer side; Event Hubs Premium acks quickly.
_SINK_TUNING = {"kafka.linger.ms": "0", "kafka.acks": "1"}


def read_topic(spark: SparkSession, topic: str) -> DataFrame:
    """Streaming read of one Event Hubs topic for an RTM flow (one task per maxPartitions)."""
    return (
        spark.readStream.format("kafka")
        .options(**eventhubs.kafka_options_from_conf(spark))
        .option("subscribe", topic)
        .option("startingOffsets", conf(spark, "kafka.starting_offsets"))
        .option("maxPartitions", conf(spark, "rtm.max_partitions"))
        .load()
    )


def sink_options(spark: SparkSession, topic: str | None = None) -> dict[str, str]:
    """Options for dp.create_sink(..., "kafka", ...). Without `topic`, records route by their `topic` column."""
    opts = {**eventhubs.kafka_options_from_conf(spark), **_SINK_TUNING}
    return {**opts, "topic": topic} if topic else opts


# For flows whose projection is too large for one JVM method (64 KB): whole-stage codegen fails to compile,
# Spark falls back to non-fused execution anyway, and the failed compile is retried at EVERY epoch boundary,
# adding seconds to the inter-batch pause (seen on silver: ~4 s vs ~1 s on bronze). Skip the doomed attempt.
NO_WHOLE_STAGE_CODEGEN = {"spark.sql.codegen.wholeStage": "false"}


def flow_conf(spark: SparkSession, extra: dict[str, str] | None = None) -> dict[str, str]:
    """spark_conf for @dp.update_flow: run the flow in Real-Time Mode."""
    return {
        "pipelines.trigger": "RealTime",
        "pipelines.trigger.interval": conf(spark, "rtm.trigger_interval"),
        **(extra or {}),
    }
