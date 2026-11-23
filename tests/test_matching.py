"""The hub's driver path (hl7rt.matching + hl7rt.render) against its Spark expression path (hl7rt.subscriptions): the
same decisions and byte-identical notifications, on records built to hit every rendering and comparison edge."""

import json
import math
import random
from dataclasses import replace

import pytest
from pyspark.sql import functions as F

from hl7rt import matching, render, subscriptions
from hl7rt.subscriptions import Rule
from subscription_api.rules import parse

SOURCE = "silver_observations"
COLS = [f.name for f in subscriptions.SOURCES[SOURCE]]
GRANTED = frozenset(COLS)
TOPIC = "cat.subscribers.app-a"

EDGE_NUMS = [None, 0.0, -0.0, 7.2, 13.0, 6.9, 1e7, 1e-4, 123456789.123, -1.5, float("nan"), float("inf"),
             float("-inf"), 2.0**53 + 1, 1e300]  # fmt: skip
EDGE_STRS = [None, "", "LL", "HH", "g/dL", "mmol/L", "Hemoglobin", "Potassium", 'quote " and \\ backslash',
             "tab\tnew\nline", "ctl\u0001\u001f", "é ü 中文 😀", "MRN000001", "P1", "P3", "LN", "2160-0", "RAD",
             "FLOWSHEET", "I", "J"]  # fmt: skip


def records(n=400, seed=7):
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        r = {}
        for f in subscriptions.SOURCES[SOURCE]:
            t = type(f.dataType).__name__
            if t == "DoubleType":
                r[f.name] = rng.choice(EDGE_NUMS)
            elif t in ("IntegerType", "LongType"):
                r[f.name] = rng.choice([None, 0, 1, 2, 2**31 - 1] + ([2**62 + 7] if t == "LongType" else []))
            elif t == "TimestampType":
                r[f.name] = rng.choice([None, "2026-11-18T16:00:00.123Z", "2026-11-18T16:00:00Z"])
            else:
                r[f.name] = rng.choice(EDGE_STRS)
        out.append(r)
    return out


@pytest.fixture(scope="module")
def batch(spark):
    """(the batch as subscriptions.events, its records collected with every field's rendered member)."""
    rows = [(json.dumps(r, allow_nan=True), i % 4, 1000 + i, f"2026-11-18T16:00:00.{i % 1000:03d}Z")
            for i, r in enumerate(records())]  # fmt: skip
    topic = spark.createDataFrame(rows, "value string, partition int, offset long, ts string").select(
        F.col("value").cast("binary").alias("value"), "partition", "offset", F.to_timestamp("ts").alias("timestamp")
    )
    parsed = subscriptions.events(topic, SOURCE)
    return parsed, [r.asDict() for r in parsed.select("*", *render.members(SOURCE)).collect()]


def rule(sql, sid="11111111-1111-4111-8111-111111111111", name="r", revision=3, topic=TOPIC):
    p = parse(sql, set(subscriptions.SOURCES), "cat.sub_app_a")
    return Rule(sid, name, revision, p.source, p.where, list(p.fields), topic, GRANTED)


def _order(n):  # (topic, key, value); a NULL key sorts first
    return n[0], n[1] is not None, n[1] or "", n[2]


def spark_path(parsed, rules):
    """The oracle: every rule on the Spark expression path (compile_rules without a session)."""
    out = subscriptions.notifications(parsed, SOURCE, subscriptions.compile_rules(rules))
    return sorted(((x.topic, x.key, x.value) for x in out.collect()), key=_order) if out is not None else []


def driver_path(rows, rules):
    good, failed = subscriptions.runnable(rules)  # what the hub places: checked rules, canonical WHERE
    assert not failed
    m = matching.Matcher()
    for r in good:
        m.place(r, "app_a")
    return sorted(((e.rule.topic, *render.notification(e.rule, SOURCE, rec)) for rec in rows for e in m.match(rec)),
                  key=_order)  # fmt: skip


FIELDS = "patient_id, value_num, units, abnormal_flag, observation_cd_desc, raw_offset, position, observation_dt_tm"
CONDITIONS = [
    "value_num < 8", "8 > value_num", "value_num >= 6.9 AND observation_cd_desc = 'Potassium'",
    "abnormal_flag <> 'LL'", "abnormal_flag IS NULL", "abnormal_flag IS NOT NULL",
    "patient_id IN ('P1', 'P3', 'MRN000001')", "abnormal_flag NOT IN ('HH')", "value_num BETWEEN 7 AND 14",
    "value_num > -1 AND value_num <= 13", "value_num = 7.2", "observation_cd_desc >= 'I'", "value_num IN (7.2, 13)",
    "value_num NOT IN (7.2)", "value_num > 1e9", "value_num < 1e9", "value_num <> 7.2", "value_num >= 0",
    "raw_offset > 100", "position = 1", "set_id IN (0, 2)",
    "value_num > 1 OR abnormal_flag = 'HH'", "value_num > 7 OR abnormal_flag = 'LL'",
    "patient_id IN ('P1', 'P3') AND (observation_cd_desc = 'Hemoglobin' OR value_num > 6)",
    "(units = 'g/dL' AND value_num < 8) OR (units = 'mmol/L' AND abnormal_flag IN ('HH', 'LL'))",
    "abnormal_flag IS NULL OR value_num < 8", "units = 'é ü 中文 😀'", "units > 'quote'",
    "alt_observation_cd_system = 'LN' AND alt_observation_cd = '2160-0'",
]  # fmt: skip


@pytest.mark.parametrize("condition", CONDITIONS)
def test_driver_sends_the_spark_paths_bytes(batch, condition):
    parsed, rows = batch
    r = rule(f"SELECT {FIELDS} FROM {SOURCE} WHERE {condition}")
    want = spark_path(parsed, [r])
    assert want, f"no matches: not a useful case ({condition})"
    assert driver_path(rows, [r]) == want


def test_many_rules_and_a_rule_with_no_where(batch):
    parsed, rows = batch
    rules = [rule(f"SELECT {FIELDS} FROM {SOURCE} WHERE {c}", sid=f"00000000-0000-4000-8000-{i:012d}", name=f"r{i}",
                  topic=f"cat.subscribers.app-{i % 3}") for i, c in enumerate(CONDITIONS)]  # fmt: skip
    rules.append(rule(f"SELECT patient_id, value FROM {SOURCE}", sid="22222222-2222-4222-8222-222222222222"))
    assert driver_path(rows, rules) == spark_path(parsed, rules)


def test_a_two_thousand_patient_list(batch):
    parsed, rows = batch
    ids = ["P1", "MRN000001", "é ü 中文 😀"] + [f"MRN{p:07d}" for p in range(2_000_000, 2_001_997)]
    r = rule(f"SELECT {FIELDS} FROM {SOURCE} WHERE patient_id IN ({', '.join(repr(i) for i in ids)}) AND "
             "value_num < 8")  # fmt: skip
    assert len(ids) == 2_000
    want = spark_path(parsed, [r])
    assert want and driver_path(rows, [r]) == want


def test_notification_ids_are_the_spark_paths(batch):
    parsed, rows = batch
    r = rule(f"SELECT patient_id FROM {SOURCE} WHERE abnormal_flag = 'HH'", revision=12)
    for topic, key, value in driver_path(rows, [r]):
        v = json.loads(value)
        assert topic == TOPIC and key == v["payload"]["patient_id"]
        assert v["notificationId"] == render.notification_id(r.subscription_id, 12, v["sourcePartition"],
                                                             v["sourceOffset"])  # fmt: skip


def test_an_indexed_rule_is_not_checked_against_other_patients(batch, monkeypatch):
    _, rows = batch
    checked = []
    real = matching.holds
    monkeypatch.setattr(matching, "holds", lambda c, rec: checked.append(rec.get("patient_id")) or real(c, rec))
    m = matching.Matcher()
    m.place(subscriptions.runnable([rule(f"SELECT patient_id FROM {SOURCE} WHERE patient_id IN ('P1', 'P3') "
                                         "AND value_num < 8")])[0][0], "app_a")  # fmt: skip
    hits = [e for rec in rows for e in m.match(rec)]
    assert hits and set(checked) <= {"P1", "P3"}  # only the listed patients' records reach a clause
    assert m.index_size() == 2


def test_editing_a_rule_touches_only_its_own_entries(batch):
    _, rows = batch
    m = matching.Matcher()
    a = rule(f"SELECT {FIELDS} FROM {SOURCE} WHERE patient_id IN ('P1')", sid="a" * 8 + "-0000-4000-8000-" + "0" * 12)
    b = rule(f"SELECT {FIELDS} FROM {SOURCE} WHERE abnormal_flag = 'HH'", sid="b" * 8 + "-0000-4000-8000-" + "0" * 12)
    m.place(a, "app_a")
    m.place(b, "app_b")
    count = lambda: sum(len(m.match(r)) for r in rows)  # noqa: E731
    before = count()
    m.place(replace(b, revision=4), "app_b")
    assert count() == before and m.entries[b.subscription_id].rule.revision == 4
    assert m.entries[b.subscription_id].owner == "app_b"
    m.remove(b.subscription_id)
    assert 0 < count() < before
    m.remove(a.subscription_id)
    assert count() == 0 and m.index_size() == 0 and not m.entries


def test_a_record_is_one_notification_per_rule_however_many_groups_hold(batch):
    _, rows = batch
    m = matching.Matcher()
    m.place(rule(f"SELECT patient_id FROM {SOURCE} WHERE abnormal_flag = 'HH' OR units = 'g/dL' OR value_num > 0"),
            "app_a")  # fmt: skip
    for rec in rows:
        assert len(m.match(rec)) <= 1


def test_a_rule_that_is_not_simple_is_refused_and_its_old_revision_removed():
    m = matching.Matcher()
    old = rule(f"SELECT patient_id FROM {SOURCE} WHERE units = 'g/dL'")
    m.place(old, "app_a")
    with pytest.raises(matching.RuleNotSimple):
        m.place(rule(f"SELECT patient_id FROM {SOURCE} WHERE lower(units) = 'g/dl'", revision=4), "app_a")
    assert not m.entries and m.index_size() == 0  # revision 3 no longer runs on the driver: the expression path's now


def test_fields_are_what_placed_rules_select():
    m = matching.Matcher()
    m.place(rule(f"SELECT patient_id, units FROM {SOURCE} WHERE units = 'g/dL'"), "app_a")
    m.place(rule(f"SELECT value_num FROM {SOURCE} WHERE value_num > 1", sid="33333333-3333-4333-8333-333333333333"),
            "app_a")  # fmt: skip
    assert m.fields() == {"patient_id", "units", "value_num"}


def test_nan_orders_above_every_number():
    c = {"col": "x", "op": "gt_n", "num": 1e300}
    assert matching.holds(c, {"x": math.nan}) and not matching.holds({**c, "op": "lt_n"}, {"x": math.nan})
    assert matching.holds({"col": "x", "op": "ne_n", "num": 1.0}, {"x": math.nan})
    assert not matching.holds({"col": "x", "op": "eq_n", "num": 1.0}, {"x": math.nan})
    assert matching.holds({"col": "x", "op": "in_n", "nums": [math.nan]}, {"x": math.nan})


def test_the_batch_covers_the_rendering_edges(batch):
    _, rows = batch
    rendered = {r[render.PREFIX + "value_num"] for r in rows}
    for member in [
        '"value_num":"NaN"',
        '"value_num":"Infinity"',
        '"value_num":"-Infinity"',
        '"value_num":1.0E7',
        '"value_num":1.0E-4',
        '"value_num":null',
        '"value_num":9.007199254740992E15',
    ]:
        assert member in rendered, member
    assert any("\\u0001" in r[render.PREFIX + "units"] for r in rows)
    assert any(r["patient_id"] is None for r in rows) and any(r[render.TS].endswith('.123Z"') for r in rows)
