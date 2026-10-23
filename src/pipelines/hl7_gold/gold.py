"""Hot path, gold (Real-Time Mode): silver topic -> encounters | observations | orders topics.

One flow writes all three: each silver record is exploded into its entities, routed per record by `topic`.
"""

from pyspark import pipelines as dp

from hl7rt import gold, rtm
from hl7rt.config import conf

dp.create_sink("gold_entities", "kafka", rtm.sink_options(spark))


@dp.update_flow(name="hl7_gold", target="gold_entities", spark_conf=rtm.flow_conf(spark))
def hl7_gold():
    return gold.to_gold_records(
        rtm.read_topic(spark, conf(spark, "topics.silver")),
        encounters=conf(spark, "topics.gold_encounters"),
        observations=conf(spark, "topics.gold_observations"),
        orders=conf(spark, "topics.gold_orders"),
    )
