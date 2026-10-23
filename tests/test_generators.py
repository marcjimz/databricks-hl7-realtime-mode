"""The Epic JSON generator produces payloads the silver transform accepts (and a few it rejects)."""

import random

from hl7rt import epic_json
from hl7rt.generators import epic_json as gen


def test_generated_payloads_normalize(bronze_df):
    rng = random.Random(7)
    values = [gen.payload(n, rng, bad_ratio=0.0)[1] for n in range(50)]
    silver = epic_json.to_silver(bronze_df(*values))
    assert silver.count() == 50
    assert silver.where("msg_dt_tm IS NULL OR msg_control_id IS NULL").count() == 0


def test_bad_ratio_routes_to_quarantine(bronze_df):
    rng = random.Random(7)
    values = [gen.payload(n, rng, bad_ratio=1.0)[1] for n in range(20)]
    assert epic_json.to_silver(bronze_df(*values)).count() == 0
    assert epic_json.to_quarantine(bronze_df(*values)).count() == 20


def test_generated_hl7_messages_parse_cleanly(spark):
    from pyspark.sql import functions as F

    from hl7rt import hl7v2
    from hl7rt.generators import hl7v2 as hl7gen

    rng = random.Random(11)
    msgs = [hl7gen.build(n, rng, bad_ratio=0.0)[1] for n in range(300)]
    df = hl7v2.parse(spark.createDataFrame([(m,) for m in msgs], "hl7 string"), F.col("hl7")).select(
        F.col("message").alias("m"), F.col("reason").alias("r")
    )
    assert df.where("r IS NOT NULL").count() == 0
    types = {r.t for r in df.selectExpr("m.msg_type AS t").distinct().collect()}
    assert types == {t for t, _ in hl7gen.MESSAGE_MIX}
    oru = df.where("m.msg_type = 'ORU^R01'")
    assert oru.where("size(m.observations) <> 4 OR m.observations[0].abnormal_flag IS NULL").count() == 0


def test_generated_bad_hl7_messages_quarantine(spark):
    from pyspark.sql import functions as F

    from hl7rt import hl7v2
    from hl7rt.generators import hl7v2 as hl7gen

    rng = random.Random(11)
    msgs = [hl7gen.build(n, rng, bad_ratio=1.0)[1] for n in range(20)]
    reasons = hl7v2.parse(spark.createDataFrame([(m,) for m in msgs], "hl7 string"), F.col("hl7")).select("reason")
    assert {r[0] for r in reasons.collect()} == {"not_hl7"}
