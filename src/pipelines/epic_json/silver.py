"""Silver: normalized Epic HL7 event fields (same column contract as before) + quarantine for bad rows."""

from pyspark import pipelines as dp

from hl7rt import epic_json
from hl7rt.config import conf

BRONZE = conf(spark, "epic_json.bronze_table", "epic_near_realtime_json_from_eventhub")


@dp.table(
    name=conf(spark, "epic_json.silver_table", "epic_near_realtime_table_from_json"),
    comment="Silver layer: normalized Epic HL7 event fields extracted from raw Event Hubs JSON payloads. "
    "Pivots the nested columns array into typed columns matching the fn_normalize_epic_hl7 spec.",
    table_properties={"quality": "silver"},
    spark_conf=epic_json.SPARK_CONF,
)
def epic_near_realtime_table_from_json():
    return epic_json.to_silver(spark.readStream.table(BRONZE))


@dp.table(
    name=conf(spark, "epic_json.quarantine_table", "epic_near_realtime_quarantine"),
    comment="Events that could not be normalized (invalid JSON, no columns array, or no msg_control_id), "
    "with the reason and Kafka coordinates for replay.",
    table_properties={"quality": "quarantine"},
    spark_conf=epic_json.SPARK_CONF,
)
def epic_near_realtime_quarantine():
    return epic_json.to_quarantine(spark.readStream.table(BRONZE))
