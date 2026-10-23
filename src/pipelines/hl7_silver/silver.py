"""Hot path, silver (Real-Time Mode): bronze topic -> silver topic (parsed HL7), bad messages -> quarantine topic.

One flow writes both topics: the sink has no fixed topic, so each record's `topic` column routes it.
"""

from pyspark import pipelines as dp

from hl7rt import hl7v2, rtm
from hl7rt.config import conf

dp.create_sink("silver_or_quarantine", "kafka", rtm.sink_options(spark))


@dp.update_flow(
    name="hl7_silver",
    target="silver_or_quarantine",
    # The full parse projection exceeds the 64 KB method limit; see rtm.NO_WHOLE_STAGE_CODEGEN.
    spark_conf=rtm.flow_conf(spark, rtm.NO_WHOLE_STAGE_CODEGEN),
)
def hl7_silver():
    return hl7v2.to_silver_records(
        rtm.read_topic(spark, conf(spark, "topics.bronze")),
        silver_topic=conf(spark, "topics.silver"),
        quarantine_topic=conf(spark, "topics.quarantine"),
    )
