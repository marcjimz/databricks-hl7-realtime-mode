"""Specs for self-service subscriptions: YAML rule -> streaming query over gold topics -> sub_<name> records."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hl7rt import gold, hl7v2, subscriptions
from hl7rt.generators.hl7v2 import message, segment

ROOT = Path(__file__).resolve().parents[1]
RULES = ROOT / "src" / "subscriptions"
T0 = datetime(2026, 10, 23, 12, 0, 0)
TOPICS = {"encounters": "e", "observations": "o", "orders": "r"}


# --- test data: real HL7 -> bronze -> silver -> gold records -----------------------------------------------------


def _hl7(kind: str, mrn: str, at: datetime, n: int, flag: str = "N", patient_class: str = "E") -> str:
    msh = {3: "EPIC", 4: "FAC01", 7: at.strftime("%Y%m%d%H%M%S"), 9: kind, 10: f"C{n}", 11: "P", 12: "2.5.1"}
    segs = [segment("MSH", msh), segment("PID", {1: "1", 3: f"{mrn}^^^EPIC^MR"}),
            segment("PV1", {1: "1", 2: patient_class, 3: "ED^01^A^FAC01", 19: f"V{n}"})]  # fmt: skip
    if kind.startswith("ORU"):
        obx = {1: "1", 2: "NM", 3: "2823-3^Potassium^LN", 5: "6.9", 6: "mmol/L", 8: flag}
        segs += [segment("OBR", {1: "1", 4: "80048^BMP^CPT"}), segment("OBX", obx)]
    return message(segs)


def _kafka(spark, values, ts):
    return spark.createDataFrame(
        [(b"k", v.encode(), "t", 0, i, ts, 1) for i, v in enumerate(values)],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, timestampType int",
    )


def gold_topics(spark, *messages):
    """table name -> Kafka-shaped DataFrame of that gold topic's records."""
    bronze = [r.value for r in hl7v2.to_bronze_records(_kafka(spark, messages, T0)).collect()]
    silver = hl7v2.to_silver_records(_kafka(spark, bronze, T0), "s", "q").collect()
    silver = _kafka(spark, [r.value for r in silver if r.topic == "s"], T0)
    records = gold.to_gold_records(silver, **TOPICS).collect()
    by = {"e": "gold_encounters", "o": "gold_observations", "r": "gold_orders"}
    out = {t: [] for t in by.values()}
    for r in records:
        out[by[r.topic]].append(r.value)
    return {t: _kafka(spark, v, T0 + timedelta(milliseconds=30)) for t, v in out.items() if v}


def run(spark, rule, *messages):
    topics = gold_topics(spark, *messages)
    out = subscriptions.build(rule, lambda table: topics[table])
    assert out.columns == ["key", "value"]
    return [json.loads(r.value) for r in out.collect()]


# --- rule files ----------------------------------------------------------------------------------------------


def test_shipped_rules_and_template_load():
    files = [p for p in RULES.iterdir() if p.suffix in (".yml", ".sql")]
    assert {"_template.yml", "_template.sql", "_template_join.sql", "critical_lab.yml"} <= {p.name for p in files}
    for p in files:
        subscriptions.load(p)


def test_rule_name_matches_file_name(tmp_path):
    p = tmp_path / "abc.yml"
    p.write_text("name: other\nmode: rtm\nkey: patient_id\nwhen: {source: gold_orders}\noutput: [patient_id]\n")
    with pytest.raises(ValueError, match="must match the file name"):
        subscriptions.load(p)


@pytest.mark.parametrize(
    "patch, error",
    [
        ({"mode": "batch"}, "mode"),
        ({"when": {"source": "silver_hl7"}}, "source"),
        ({"then": {"source": "gold_observations"}}, "within"),
        ({"then": {"source": "gold_observations"}, "within": "soon"}, "within"),
        ({"destinations": ["delta"]}, "rtm"),
        ({"output": ["nope"]}, "nope"),
    ],
)
def test_invalid_rules_fail_with_a_clear_message(patch, error):
    base = {"name": "x", "mode": "rtm", "key": "patient_id", "when": {"source": "gold_encounters"},
            "output": ["patient_id"], "destinations": ["eventhubs"]}  # fmt: skip
    with pytest.raises(ValueError, match=error):
        subscriptions.parse({**base, **patch})


def test_rule_names_its_table_and_topic():
    rule = subscriptions.load(RULES / "ed_critical_lab.yml")
    assert rule.table == "sub_ed_critical_lab"
    assert rule.is_join and rule.mode == "rtm"


# --- single event (stateless) --------------------------------------------------------------------------------


def test_single_event_rule_filters_and_projects(spark):
    rule = subscriptions.load(RULES / "critical_lab.yml")
    out = run(spark, rule, _hl7("ORU^R01", "MRN1", T0, 1, flag="HH"), _hl7("ORU^R01", "MRN2", T0, 2, flag="N"),
              _hl7("ADT^A04", "MRN3", T0, 3))  # fmt: skip
    assert [o["patient_id"] for o in out] == ["MRN1"]
    assert out[0]["abnormal_flag"] == "HH" and out[0]["value_num"] == 6.9
    assert out[0]["gold_ts"].startswith("2026-10-23T12:00:00.030") and out[0]["raw_ts"].startswith(
        "2026-10-23T12:00:00"
    )


# --- event A, then event B for the same key within the window ------------------------------------------------


def test_join_rule_matches_b_after_a_within_window(spark):
    rule = subscriptions.load(RULES / "ed_critical_lab.yml")
    out = run(
        spark,
        rule,
        _hl7("ADT^A04", "MRN1", T0, 1),  # ED registration
        _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=20), 2, flag="HH"),  # critical lab 20 min later: match
        _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=90), 3, flag="LL"),  # outside 60 min: no
        _hl7("ORU^R01", "MRN1", T0 - timedelta(minutes=5), 4, flag="HH"),  # before the registration: no
        _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=10), 5, flag="N"),  # not critical: no
        _hl7("ORU^R01", "MRN2", T0 + timedelta(minutes=10), 6, flag="HH"),  # other patient: no
    )
    assert [(o["when_msg_control_id"], o["then_msg_control_id"]) for o in out] == [("C1", "C2")]
    o = out[0]
    assert o["matched_after_ms"] == 20 * 60 * 1000
    assert o["patient_id"] == "MRN1" and o["abnormal_flag"] == "HH" and o["when_visit_id"] == "V1"


def test_join_rule_ignores_non_ed_registrations(spark):
    rule = subscriptions.load(RULES / "ed_critical_lab.yml")
    out = run(spark, rule, _hl7("ADT^A04", "MRN1", T0, 1, patient_class="I"),
              _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=5), 2, flag="HH"))  # fmt: skip
    assert out == []


def test_output_schema_matches_records(spark):
    rule = subscriptions.load(RULES / "ed_critical_lab.yml")
    schema = subscriptions.output_schema(rule)
    out = run(spark, rule, _hl7("ADT^A04", "MRN1", T0, 1), _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=1), 2, "HH"))
    assert list(out[0]) == schema.fieldNames()


# --- upsert key ------------------------------------------------------------------------------------------------


def test_records_carry_a_stable_id_from_raw_lineage(spark):
    """At-least-once delivery re-sends records after a restart; record_id lets tables upsert instead of duplicate."""
    rule = subscriptions.load(RULES / "ed_critical_lab.yml")
    msgs = (_hl7("ADT^A04", "MRN1", T0, 1), _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=1), 2, "HH"))
    first, again = run(spark, rule, *msgs), run(spark, rule, *msgs)
    assert list(first[0])[0] == "record_id"
    assert [r["record_id"] for r in first] == [r["record_id"] for r in again]
    single = run(spark, subscriptions.load(RULES / "critical_lab.yml"), _hl7("ORU^R01", "MRN1", T0, 1, "HH"))
    assert single[0]["record_id"] == "0-0-1"  # raw partition - raw offset - OBX set id


# --- SQL rules ---------------------------------------------------------------------------------------------------

SQL_CRITICAL = """-- name: {name}
-- mode: rtm
-- key: patient_id
SELECT _meta, patient_id, visit_id, patient_facility, msg_control_id, observation_cd, observation_cd_desc,
       value_num, units, abnormal_flag, observation_dt_tm
FROM gold_observations
WHERE abnormal_flag IN ('HH', 'LL')
"""
SQL_ED_LAB = """-- name: {name}
-- mode: microbatch
-- key: patient_id
SELECT e._meta AS _meta_registration, o._meta AS _meta_lab,
       o.patient_id, o.patient_facility, e.visit_id AS when_visit_id, o.observation_cd, o.observation_cd_desc,
       o.value_num, o.abnormal_flag
FROM gold_encounters e
JOIN gold_observations o
  ON e.patient_id = o.patient_id
 AND o.msg_dt_tm BETWEEN e.msg_dt_tm AND e.msg_dt_tm + INTERVAL 60 MINUTES
WHERE e.event = 'A04' AND e.patient_class = 'E' AND o.abnormal_flag IN ('HH', 'LL')
"""


def _sql_rule(tmp_path, body, name="sql_rule"):
    p = tmp_path / f"{name}.sql"
    p.write_text(body.replace("{name}", name))
    return subscriptions.load(p)


def test_sql_rule_loads_with_header_settings(tmp_path):
    rule = _sql_rule(tmp_path, SQL_CRITICAL)
    assert (rule.name, rule.mode, rule.key, rule.table) == ("sql_rule", "rtm", "patient_id", "sub_sql_rule")
    assert rule.sources == ("gold_observations",) and not rule.is_join
    join = _sql_rule(tmp_path, SQL_ED_LAB, "sql_join")
    assert join.is_join and set(join.sources) == {"gold_encounters", "gold_observations"}


def test_sql_single_event_matches_the_yaml_rule(spark, tmp_path):
    msgs = (
        _hl7("ORU^R01", "MRN1", T0, 1, flag="HH"),
        _hl7("ORU^R01", "MRN2", T0, 2, flag="N"),
        _hl7("ADT^A04", "MRN3", T0, 3),
    )
    yaml_out = run(spark, subscriptions.load(RULES / "critical_lab.yml"), *msgs)
    sql_out = run(spark, _sql_rule(tmp_path, SQL_CRITICAL), *msgs)
    assert sql_out == yaml_out


def test_sql_join_matches_the_yaml_rule(spark, tmp_path):
    msgs = (
        _hl7("ADT^A04", "MRN1", T0, 1),
        _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=20), 2, flag="HH"),  # match
        _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=90), 3, flag="LL"),  # outside the window
        _hl7("ORU^R01", "MRN1", T0 - timedelta(minutes=5), 4, flag="HH"),  # before the registration
        _hl7("ORU^R01", "MRN2", T0 + timedelta(minutes=10), 6, flag="HH"),  # other patient
    )
    sql_out = run(spark, _sql_rule(tmp_path, SQL_ED_LAB, "sql_join"), *msgs)
    yaml_out = run(spark, subscriptions.load(RULES / "ed_critical_lab.yml"), *msgs)
    assert [o["record_id"] for o in sql_out] == [o["record_id"] for o in yaml_out]
    assert sql_out[0]["observation_cd"] == "2823-3" and sql_out[0]["when_visit_id"] == "V1"


def test_sql_output_schema_matches_records(spark, tmp_path):
    rule = _sql_rule(tmp_path, SQL_ED_LAB, "sql_join")
    out = run(spark, rule, _hl7("ADT^A04", "MRN1", T0, 1), _hl7("ORU^R01", "MRN1", T0 + timedelta(minutes=1), 2, "HH"))
    assert list(out[0]) == subscriptions.output_schema(rule, spark).fieldNames()
    assert list(out[0])[0] == "record_id" and "_meta_lab" not in out[0]


@pytest.mark.parametrize(
    "body, error",
    [
        ("-- name: x\n-- mode: rtm\n-- key: patient_id\nSELECT patient_id FROM gold_observations", "_meta"),
        ("-- name: x\n-- mode: rtm\n-- key: patient_id\nSELECT _meta, patient_id FROM silver_hl7", "gold_"),
        ("-- name: x\n-- mode: batch\n-- key: patient_id\nSELECT _meta, patient_id FROM gold_orders", "mode"),
        ("-- name: x\n-- mode: rtm\nSELECT _meta, patient_id FROM gold_orders", "key"),
    ],
)
def test_invalid_sql_rules_fail_clearly(tmp_path, body, error):
    p = tmp_path / "x.sql"
    p.write_text(body)
    with pytest.raises(ValueError, match=error):
        subscriptions.load(p)


def test_sql_literals_with_braces_are_left_alone(spark, tmp_path):
    body = SQL_CRITICAL.replace(
        "WHERE abnormal_flag IN ('HH', 'LL')", "WHERE abnormal_flag IN ('HH', 'LL') AND '{x}' = '{x}'"
    )
    assert len(run(spark, _sql_rule(tmp_path, body), _hl7("ORU^R01", "MRN1", T0, 1, flag="HH"))) == 1
