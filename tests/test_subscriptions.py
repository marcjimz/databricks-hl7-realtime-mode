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
    names = {p.stem for p in RULES.glob("*.yml")}
    assert {"_template", "critical_lab", "ed_critical_lab"} <= names
    for p in RULES.glob("*.yml"):
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
