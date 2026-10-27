"""Bronze: raw Epic JSON events from Event Hubs (Kafka endpoint), Kafka metadata preserved."""

from pyspark import pipelines as dp

from hl7rt import epic_json, eventhubs
from hl7rt.config import conf


@dp.table(
    name=conf(spark, "epic_json.bronze_table", "epic_near_realtime_json_from_eventhub"),
    comment="Bronze layer: raw events ingested from Azure Event Hubs via the Kafka-compatible endpoint. "
    "Preserves all Kafka metadata columns with minimal transformation.",
    table_properties={"quality": "bronze"},
    cluster_by=["kafka_timestamp"],
)
def epic_near_realtime_json_from_eventhub():
    return epic_json.to_bronze(
        spark.readStream.format("kafka")
        .options(**eventhubs.kafka_options_from_conf(spark))
        .option("subscribe", conf(spark, "epic_json.topic"))
        .option("startingOffsets", conf(spark, "kafka.starting_offsets", "latest"))
        .option("maxOffsetsPerTrigger", conf(spark, "kafka.max_offsets_per_trigger", "50000"))
        .option("failOnDataLoss", conf(spark, "kafka.fail_on_data_loss", "false"))
        .load()
    )
