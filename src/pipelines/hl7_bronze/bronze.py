"""Hot path, bronze (Real-Time Mode): raw HL7 topic -> bronze topic (message + MSH envelope + Kafka lineage)."""

from pyspark import pipelines as dp

from hl7rt import hl7v2, rtm
from hl7rt.config import conf

dp.create_sink("bronze_topic", "kafka", rtm.sink_options(spark, conf(spark, "topics.bronze")))


@dp.update_flow(name="hl7_bronze", target="bronze_topic", spark_conf=rtm.flow_conf(spark))
def hl7_bronze():
    return hl7v2.to_bronze_records(rtm.read_topic(spark, conf(spark, "topics.raw")))
