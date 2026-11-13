"""Hot path, silver (Real-Time Mode): bronze topic -> one topic per entity (messages, encounters, observations, orders,
diagnoses), bad messages -> quarantine topic.

One flow writes every topic: each message is parsed once, exploded into its entity rows, and the sink (no fixed topic)
routes each row by its `topic` column.
"""

from pyspark import pipelines as dp

from hl7rt import rtm, silver
from hl7rt.config import conf

dp.create_sink("silver_entities", "kafka", rtm.sink_options(spark))


@dp.update_flow(
    name="hl7_silver",
    target="silver_entities",
    # The full parse projection exceeds the 64 KB method limit; see rtm.NO_WHOLE_STAGE_CODEGEN.
    spark_conf=rtm.flow_conf(spark, rtm.NO_WHOLE_STAGE_CODEGEN),
)
def hl7_silver():
    return silver.to_silver_records(
        rtm.read_topic(spark, conf(spark, "topics.bronze")),
        messages=conf(spark, "topics.silver_messages"),
        encounters=conf(spark, "topics.silver_encounters"),
        observations=conf(spark, "topics.silver_observations"),
        orders=conf(spark, "topics.silver_orders"),
        diagnoses=conf(spark, "topics.silver_diagnoses"),
        quarantine=conf(spark, "topics.quarantine"),
    )
