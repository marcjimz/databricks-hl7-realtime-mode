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


def _journey_messages(spark, seed=3, minutes=30):
    """Every message a few ED journeys emit, parsed, in order: (journey MRN -> [message struct])."""
    from pyspark.sql import functions as F

    from hl7rt import hl7v2
    from hl7rt.generators import hl7v2 as hl7gen

    j = hl7gen.Journeys(random.Random(seed), per_minute=2, gap_s=(5, 20), patients=1_000)
    out, n = [], 0
    for second in range(minutes * 60):
        while (m := j.due(n, float(second))) is not None:
            out.append(m[1])
            n += 1
    df = hl7v2.parse(spark.createDataFrame([(i, x) for i, x in enumerate(out)], "i int, hl7 string"), F.col("hl7"))
    assert df.where("reason IS NOT NULL").count() == 0
    by_mrn = {}
    for r in df.orderBy("i").select("message.*").collect():
        by_mrn.setdefault(r.patient_id, []).append(r)
    return by_mrn


def test_an_ed_journey_is_one_patient_and_visit_from_registration_to_discharge_or_transfer(spark):
    from hl7rt.generators import hl7v2 as hl7gen

    journeys = _journey_messages(spark)
    finished = {k: v for k, v in journeys.items() if v[-1].msg_type in ("ADT^A03", "ADT^A02")}
    assert len(finished) >= 20
    for mrn, msgs in finished.items():
        assert int(mrn.removeprefix("MRN")) > 1_000, "journey patients are outside the background population"
        first = msgs[0]
        assert (first.msg_type, first.patient_class, first.admission_type) == ("ADT^A04", "E", "E")
        assert first.patient_point_of_care in hl7gen.ED_POINTS_OF_CARE and first.chief_complaint
        assert [d.diagnosis_type for d in first.diagnoses] == ["ADMITTING"]
        assert len({m.visit_id for m in msgs}) == 1
        assert msgs[1].msg_type == "ADT^A08"
        if msgs[-1].msg_type == "ADT^A02":
            assert msgs[-1].prior_point_of_care == first.patient_point_of_care
        else:
            assert msgs[-1].discharge_dt_tm is not None


def test_respiratory_journeys_get_a_chest_xray_its_report_and_a_note(spark):
    from hl7rt.generators import hl7v2 as hl7gen

    respiratory = {c for c, _, r in hl7gen.CHIEF_COMPLAINTS if r}
    journeys = _journey_messages(spark)
    kinds = {"resp": set(), "other": set()}
    for msgs in journeys.values():
        if msgs[-1].msg_type not in ("ADT^A03", "ADT^A02"):
            continue
        resp = msgs[0].chief_complaint_cd in respiratory
        assert (msgs[0].diagnoses[0].diagnosis_cd in {c for c, _ in hl7gen.RESPIRATORY_DIAGNOSES}) == resp
        rad = [m for m in msgs if m.diagnostic_service == "RAD"]
        if resp:
            assert [m.msg_type for m in rad] == ["ORM^O01", "ORU^R01"]
            assert rad[1].observations[0].value_type == "TX"
            assert any(m.msg_type == "MDM^T02" and m.document_id for m in msgs)
        else:
            assert rad == []
        kinds["resp" if resp else "other"].add(len(msgs))
    assert kinds["resp"] and kinds["other"], "both kinds of journey occur"
