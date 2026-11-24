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
    assert oru.where("size(m.observations) = 0 OR m.observations[0].abnormal_flag IS NULL").count() == 0


def test_generated_hl7_has_the_shapes_customer_rules_filter_on(spark):
    """Labs carry LOINC as the alternate code (alt_observation_cd_system = 'LN'), vitals are flowsheet rows, admits
    carry ICD-10-CM diagnoses with a type, and patients are spread over several points of care."""
    from pyspark.sql import functions as F

    from hl7rt import hl7v2
    from hl7rt.generators import hl7v2 as hl7gen

    rng = random.Random(7)
    msgs = [hl7gen.build(n, rng, bad_ratio=0.0)[1] for n in range(600)]
    m = hl7v2.parse(spark.createDataFrame([(x,) for x in msgs], "hl7 string"), F.col("hl7")).select("message.*")
    obs = m.select(F.explode("observations").alias("o")).select("o.*")
    labs = obs.where("alt_observation_cd_system = 'LN'")
    assert {r[0] for r in labs.select("alt_observation_cd").distinct().collect()} == {c for c, *_ in hl7gen.LABS}
    vitals = obs.where("observation_cd_system = 'FLOWSHEET'")
    assert {r[0] for r in vitals.select("observation_cd").distinct().collect()} == {c for c, *_ in hl7gen.VITALS}
    admits = m.where("msg_type IN ('ADT^A01', 'ADT^A04', 'ADT^A08')")
    dx = admits.select(F.explode("diagnoses").alias("d")).select("d.*")
    assert dx.where("diagnosis_cd_system <> 'ICD-10-CM' OR diagnosis_type IS NULL").count() == 0
    assert {r[0] for r in dx.select("diagnosis_cd").distinct().collect()} == {c for c, _ in hl7gen.DIAGNOSES}
    assert {r[0] for r in dx.select("diagnosis_type").distinct().collect()} == set(hl7gen.DIAGNOSIS_TYPES)
    assert m.where("msg_type NOT IN ('ADT^A01', 'ADT^A04', 'ADT^A08') AND size(diagnoses) > 0").count() == 0
    points = {r[0] for r in m.select("patient_point_of_care").distinct().collect()}
    assert points == set(hl7gen.POINTS_OF_CARE)


def test_generated_bad_hl7_messages_quarantine(spark):
    from pyspark.sql import functions as F

    from hl7rt import hl7v2
    from hl7rt.generators import hl7v2 as hl7gen

    rng = random.Random(11)
    msgs = [hl7gen.build(n, rng, bad_ratio=1.0)[1] for n in range(20)]
    reasons = hl7v2.parse(spark.createDataFrame([(m,) for m in msgs], "hl7 string"), F.col("hl7")).select("reason")
    assert {r[0] for r in reasons.collect()} == {"not_hl7"}
