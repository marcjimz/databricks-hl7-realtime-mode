"""Hub matching: every app's SQL rule over a micro-batch, with the semantics of the SQL editor; matches become
notifications routed to each app's own topic. The hub (hl7rt.engine) matches simple rules on the driver and the rest as
Spark expressions; the expression path whole (subscriptions.notifications) is the reference both are held to."""

import json
import re

import pytest
from pyspark.sql import functions as F

from hl7rt import engine, matching, silver, subscriptions
from hl7rt.subscriptions import Rule
from subscription_api.rules import parse

OBS = [
    {"patient_id": "P1", "observation_cd_desc": "Hemoglobin", "value_num": 7.2, "units": "g/dL", "abnormal_flag": "LL"},
    {"patient_id": "P2", "observation_cd_desc": "Hemoglobin", "value_num": 13.0, "units": "g/dL",
     "abnormal_flag": None},
    {"patient_id": "P3", "observation_cd_desc": "Potassium", "value_num": 6.9, "units": "mmol/L",
     "abnormal_flag": "HH"},
    {"patient_id": "P4", "observation_cd_desc": "Potassium", "value_num": None, "units": None, "abnormal_flag": None},
]  # fmt: skip
GRANTED = frozenset({"patient_id", "observation_cd_desc", "value_num", "units", "abnormal_flag"})
SID = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def observations(spark):
    """silver_observations topic records (value = silver JSON), with Kafka coordinates and broker timestamp."""
    rows = [(json.dumps(r), 0, i, "2026-11-03T12:00:00Z") for i, r in enumerate(OBS)]
    return spark.createDataFrame(rows, "value string, partition int, offset long, ts string").select(
        F.col("value").cast("binary").alias("value"), "partition", "offset",
        F.to_timestamp("ts").alias("timestamp"),
    )  # fmt: skip


def rule(sql, sid=SID, name="r", revision=1, topic="cat.subscribers.app-a", granted=GRANTED):
    r = parse(sql, set(subscriptions.SOURCES), "cat.sub_app_a")
    return Rule(subscription_id=sid, name=name, revision=revision, source=r.source, where=r.where,
                fields=list(r.fields), topic=topic, granted=granted)  # fmt: skip


def delivered(observations, rules):
    """What the hub delivers for `rules` (hl7rt.engine), as JSON with its topic and key."""
    out = engine.notifications(observations, "silver_observations", rules)
    return [json.loads(v) | {"topic": t, "key": k} for t, k, v in out]


def reference(observations, rules):
    """(topic, key, value) from the reference path: every rule a Spark expression."""
    out = subscriptions.notifications(subscriptions.events(observations, "silver_observations"), "silver_observations",
                                      subscriptions.compile_rules(rules))  # fmt: skip
    return sorted((x.topic, x.key, x.value) for x in out.collect()) if out is not None else []


def editor(spark, sql):
    spark.createDataFrame(
        OBS, silver.OBSERVATION_SCHEMA.__class__([f for f in silver.OBSERVATION_SCHEMA if f.name in OBS[0]])
    ).createOrReplaceTempView("silver_observations")
    return sorted(r.patient_id for r in spark.sql(sql).collect())


def simple(sql, **kw) -> matching.Matcher:
    m = matching.Matcher()
    m.place(subscriptions.runnable([rule(sql, **kw)])[0][0], "app")  # raises RuleNotSimple: the expression path's
    return m


CONDITIONS = [
    "value_num < 8",
    "value_num >= 6.9",
    "observation_cd_desc = 'Potassium'",
    "abnormal_flag <> 'LL'",  # NULL <> 'LL' is unknown: no match
    "abnormal_flag IS NULL",
    "patient_id IN ('P1', 'P4', 'P9')",
    "abnormal_flag NOT IN ('HH')",
    "contains(observation_cd_desc, 'glob')",
    "startswith(observation_cd_desc, 'Pot') AND value_num IS NOT NULL",
    "units LIKE '%/L'",
    "lower(units) = 'g/dl' OR abnormal_flag = 'HH'",
    "value_num BETWEEN 7 AND 14",
    "NOT (value_num > 7)",  # NOT unknown is unknown: P4 excluded
    "coalesce(abnormal_flag, 'none') = 'none'",
]


@pytest.mark.parametrize("condition", CONDITIONS)
def test_the_hub_delivers_exactly_what_the_sql_editor_returns(spark, observations, condition):
    sql = f"SELECT patient_id FROM silver_observations WHERE {condition}"
    hub = sorted(n["payload"]["patient_id"] for n in delivered(observations, [rule(sql)]))
    assert hub == editor(spark, sql)


def test_all_rules_run_in_one_pass_and_route_to_their_own_topics(observations):
    a = rule("SELECT patient_id, value_num FROM silver_observations WHERE observation_cd_desc = 'Hemoglobin' "
             "AND value_num < 8", name="low-hgb")  # fmt: skip
    b = rule("SELECT patient_id FROM silver_observations WHERE value_num > 6.5", sid="22222222-2222-4222-8222-2",
             name="high", topic="cat.subscribers.app-b")  # fmt: skip
    c = rule("SELECT patient_id FROM silver_observations WHERE lower(units) = 'mmol/l'",
             sid="33333333-3333-4333-8333-3", name="mmol", topic="cat.subscribers.app-c")  # fmt: skip
    out = delivered(observations, [a, b, c])

    assert sorted((n["topic"], n["key"]) for n in out) == [
        ("cat.subscribers.app-a", "P1"),
        ("cat.subscribers.app-b", "P1"), ("cat.subscribers.app-b", "P2"), ("cat.subscribers.app-b", "P3"),
        ("cat.subscribers.app-c", "P3"),
    ]  # fmt: skip
    hgb = next(n for n in out if n["subscription"] == "low-hgb")
    assert hgb["payload"] == {"patient_id": "P1", "value_num": 7.2}  # nothing beyond the selected fields
    assert hgb["revision"] == 1 and hgb["source"] == "silver_observations"
    assert (hgb["sourcePartition"], hgb["sourceOffset"]) == (0, 0)


def test_many_expression_rules_split_into_chunks_give_the_same_answer(observations, monkeypatch):
    rules = [rule(f"SELECT patient_id FROM silver_observations WHERE coalesce(value_num, 0) > {i}",
                  sid=f"{i:08d}-0000-4000-8000-{i:012d}", name=f"r{i}") for i in range(15)]  # fmt: skip
    whole = sorted((n["subscription"], n["key"]) for n in delivered(observations, rules))
    monkeypatch.setattr(subscriptions, "CHUNK", 4)
    assert sorted((n["subscription"], n["key"]) for n in delivered(observations, rules)) == whole
    assert sorted((json.loads(v)["subscription"], k) for _, k, v in reference(observations, rules)) == whole


def test_nulls_are_kept_in_the_payload(observations):
    out = delivered(
        observations, [rule("SELECT patient_id, abnormal_flag FROM silver_observations WHERE patient_id = 'P2'")]
    )
    assert out[0]["payload"] == {"patient_id": "P2", "abnormal_flag": None}


def test_notifications_follow_the_published_contract(observations):
    """What the hub writes is the Notification schema in docs/api/openapi.json, field for field, on every path."""
    from subscription_api.app import Notification

    rules = [rule("SELECT patient_id, value_num FROM silver_observations WHERE observation_cd_desc = 'Potassium'"),
             rule("SELECT patient_id FROM silver_observations WHERE lower(units) = 'g/dl'", sid="e")]  # fmt: skip
    values = [v for _, _, v in engine.notifications(observations, "silver_observations", rules)]
    values += [v for _, _, v in reference(observations, rules)]
    assert len(values) == 8
    for v in values:
        assert set(json.loads(v)) == set(Notification.model_fields)
        Notification.model_validate(json.loads(v))


def test_notification_ids_are_stable_per_revision_and_source_record(observations):
    sql = "SELECT patient_id FROM silver_observations WHERE value_num < 8"

    def ids(revision):
        return [n["notificationId"] for n in delivered(observations, [rule(sql, revision=revision)])]

    assert ids(1) == ids(1)  # a redelivery of the same record keeps its ID
    assert ids(2) != ids(1)


def test_the_hub_rechecks_each_rule_before_running_it(observations):
    ok = rule("SELECT patient_id FROM silver_observations WHERE value_num > 1")
    granted = frozenset({"patient_id", "value_num"})
    ungranted = rule("SELECT patient_id FROM silver_observations WHERE units = 'x'", sid="u", granted=granted)
    smuggled = Rule(
        subscription_id="s",
        name="s",
        revision=1,
        source="silver_observations",
        fields=["patient_id"],
        topic="t",
        granted=GRANTED,
        where="http_request('GET', 'x') IS NOT NULL",
    )  # written around the API
    good, failed = subscriptions.runnable([ok, ungranted, smuggled])

    assert good == [ok]
    assert failed["u"].startswith("FIELD_DENIED") and failed["s"].startswith("SQL_NOT_ALLOWED")
    assert {n["subscriptionId"] for n in delivered(observations, [ok, ungranted, smuggled])} == {SID}


def test_a_rule_that_fails_at_runtime_is_found_alone(spark, observations):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    good = rule("SELECT patient_id FROM silver_observations WHERE value_num > 1")
    bad = rule(
        "SELECT patient_id FROM silver_observations WHERE units > 1", sid="bad"
    )  # 'g/dL' can't be cast to a number
    records = subscriptions.events(observations, "silver_observations")

    errors = subscriptions.failing(records, [good, bad])
    assert list(errors) == ["bad"] and "CAST" in errors["bad"].upper()
    assert len(delivered(observations, [good])) == 3


def test_no_rules_means_no_output(observations):
    assert subscriptions.notifications(subscriptions.events(observations, "silver_observations"), "silver_observations",
                                       []) is None  # fmt: skip
    assert engine.notifications(observations, "silver_observations", []) == []


def test_a_compiled_rule_set_is_reused_across_batches(spark, observations):
    """The reference path compiles rules once and applies them to every batch."""
    compiled = subscriptions.compile_rules([rule("SELECT patient_id FROM silver_observations WHERE value_num < 8")], 7)
    assert compiled.generation == 7 and len(compiled.chunks) == 1
    for batch in (observations, observations.where("offset >= 2")):
        records = subscriptions.events(batch, "silver_observations")
        keys = sorted(r.key for r in subscriptions.notifications(records, "silver_observations", compiled).collect())
        assert keys == (["P1", "P3"] if batch is observations else ["P3"])


def test_a_recompile_rechecks_only_the_rules_that_changed(monkeypatch):
    calls = []
    real = subscriptions.check_condition
    monkeypatch.setattr(subscriptions, "check_condition", lambda w, g: calls.append(w) or real(w, g))
    subscriptions._CHECKED.clear()
    rules = [rule(f"SELECT patient_id FROM silver_observations WHERE value_num > {i}", sid=f"s{i}") for i in range(5)]
    subscriptions.compile_rules(rules)
    changed = rule("SELECT patient_id FROM silver_observations WHERE value_num > 99", sid="s0", revision=2)
    subscriptions.compile_rules([changed] + rules[1:])
    assert calls[5:] == ["value_num > 99"], "only the changed rule is checked again"


@pytest.mark.parametrize(
    "where",
    [
        "true) THEN 1 END, secret('scope', 'key') AS s FROM silver_observations --",
        "value_num > 1)) , (http_request('GET', 'x')",
        "value_num > 1; DROP TABLE silver_observations",
    ],
)
def test_a_stored_condition_cant_break_out_of_its_expression(where):
    """Rules are built into SQL strings, so each WHERE must be one checked expression (written around the API)."""
    smuggled = Rule(subscription_id="x", name="x", revision=1, source="silver_observations", fields=["patient_id"],
                    topic="t", granted=GRANTED, where=where)  # fmt: skip
    good, failed = subscriptions.runnable([smuggled])
    assert good == [] and failed["x"].startswith("SQL_NOT_ALLOWED")


def test_literals_in_the_built_sql_are_escaped(observations):
    for sql in ("SELECT patient_id FROM silver_observations WHERE value_num < 8",
                "SELECT patient_id FROM silver_observations WHERE coalesce(value_num, 0) < 8"):  # fmt: skip
        r = rule(sql, topic="cat.subscribers.o'brien\\x")
        assert {n["topic"] for n in delivered(observations, [r])} == {"cat.subscribers.o'brien\\x"}
        assert {t for t, _, _ in reference(observations, [r])} == {"cat.subscribers.o'brien\\x"}


SIMPLE_CONDITIONS = [
    "value_num < 8",
    "8 > value_num",  # literal on the left
    "value_num >= 6.9 AND observation_cd_desc = 'Potassium'",
    "abnormal_flag <> 'LL'",  # NULL <> 'LL' is unknown: no match
    "abnormal_flag IS NULL",
    "abnormal_flag IS NOT NULL",
    "patient_id IN ('P1', 'P4', 'P9')",
    "abnormal_flag NOT IN ('HH')",  # NULL NOT IN (...) is unknown: no match
    "value_num BETWEEN 7 AND 14",
    "value_num > -1 AND value_num <= 13",
    "value_num = 7.2",
    "observation_cd_desc >= 'I'",  # string ranges compare like Spark (binary)
    "value_num IN (7.2, 13)",
    "value_num NOT IN (7.2)",
    "(abnormal_flag = 'LL') AND (value_num < 8)",
    # OR of AND groups (the shape real criteria take) stays on the driver, flattened to its disjuncts
    "value_num > 1 OR abnormal_flag = 'HH'",
    "value_num > 7 OR abnormal_flag = 'LL'",  # P1 matches both branches: delivered once
    "patient_id IN ('P1', 'P3') AND (observation_cd_desc = 'Hemoglobin' OR value_num > 6)",
    "(patient_id = 'P1' OR patient_id = 'P3') AND (abnormal_flag = 'LL' OR abnormal_flag = 'HH')",
    "abnormal_flag IS NULL OR value_num < 8",
    "(units = 'g/dL' AND value_num < 8) OR (units = 'mmol/L' AND abnormal_flag IN ('HH', 'LL'))",
]


@pytest.mark.parametrize("condition", SIMPLE_CONDITIONS)
def test_simple_rules_matched_on_the_driver_deliver_exactly_what_the_sql_editor_returns(spark, observations,
                                                                                        condition):  # fmt: skip
    sql = f"SELECT patient_id FROM silver_observations WHERE {condition}"
    assert simple(sql).entries, f"expected the driver for: {sql}"
    assert sorted(n["payload"]["patient_id"] for n in delivered(observations, [rule(sql)])) == editor(spark, sql)


@pytest.mark.parametrize("condition", SIMPLE_CONDITIONS + ["lower(units) = 'g/dl'"])
def test_the_driver_and_the_expression_path_send_identical_notifications(observations, condition):
    """Which path a rule takes is internal: the notification bytes are the same either way."""
    sql = f"SELECT patient_id, value_num, abnormal_flag, units FROM silver_observations WHERE {condition}"
    hub = sorted(engine.notifications(observations, "silver_observations", [rule(sql)]))
    assert hub == reference(observations, [rule(sql)])


@pytest.mark.parametrize(
    "condition",
    ["lower(units) = 'g/dl'", "value_num > 1 OR lower(units) = 'x'", "NOT (value_num > 7 OR units = 'x')",
     "units LIKE '%/L'", "value_num > 'x'", "value_num = patient_id", "coalesce(abnormal_flag, 'none') = 'none'"],
)  # fmt: skip
def test_anything_else_keeps_the_expression_path(condition):
    sql = f"SELECT patient_id FROM silver_observations WHERE {condition}"
    with pytest.raises(matching.RuleNotSimple):
        simple(sql)
    rs = engine.RuleSet("silver_observations")
    assert rs.place(subscriptions.runnable([rule(sql)])[0][0], "app") is False and len(rs.expressions) == 1


def test_a_thousand_simple_rules_are_matched_on_the_driver(observations):
    rules = [rule(f"SELECT patient_id FROM silver_observations WHERE value_num > {i % 20}", sid=f"s{i}", name=f"r{i}")
             for i in range(1000)]  # fmt: skip
    out = delivered(observations, rules)
    # value_num 7.2 matches thresholds 0..7 (8 of 20), 13.0 matches 0..12 (13), 6.9 matches 0..6 (7); 50 rules each.
    assert len(out) == (8 + 13 + 7) * 50


def test_simple_rules_are_indexed_by_a_string_equality_when_they_have_one():
    keyed = simple("SELECT patient_id FROM silver_observations WHERE patient_id IN ('P1', 'P1', 'P4') "
                   "AND value_num > 1")  # fmt: skip
    assert sorted(v for _, v in keyed._index) == ["P1", "P4"] and not keyed._scan, "one entry per value"
    numeric = simple("SELECT patient_id FROM silver_observations WHERE value_num > 1")
    assert numeric.index_size() == 0 and len(numeric._scan) == 1, "no string equality: compared with every record"


def test_a_record_matches_an_indexed_rule_once_even_if_several_values_could_index_it(observations):
    sql = ("SELECT patient_id FROM silver_observations WHERE observation_cd_desc = 'Hemoglobin' "
           "AND patient_id IN ('P1', 'P2')")  # fmt: skip
    assert sorted(n["payload"]["patient_id"] for n in delivered(observations, [rule(sql)])) == ["P1", "P2"]


def test_the_api_accepts_exactly_the_hubs_sources():
    """The API checks rules without importing the hub (its own deploy), so its source list is kept in step here."""
    from subscription_api import rules

    assert set(subscriptions.SOURCES) == rules.SOURCES


def test_the_index_key_is_the_most_selective_equality_not_the_first():
    """A dynamic patient rule names its message types and codes first; it is indexed on the patient list."""
    m = simple("SELECT patient_id FROM silver_observations WHERE units IN ('g/dL', 'mmol/L') "
               "AND observation_cd_desc = 'Hemoglobin' AND patient_id IN ('P1', 'P2')")  # fmt: skip
    assert set(m._columns) == {"patient_id"}
    m = simple("SELECT patient_id FROM silver_observations WHERE units = 'g/dL' AND observation_cd = '718-7'",
               granted=GRANTED | {"observation_cd"})  # fmt: skip
    assert set(m._columns) == {"observation_cd"}, "codes before other strings"


def test_each_branch_of_an_or_is_indexed_on_its_own_key():
    m = simple("SELECT patient_id FROM silver_observations WHERE patient_id = 'P1' "
               "OR (observation_cd = '718-7' AND units = 'g/dL')", granted=GRANTED | {"observation_cd"})  # fmt: skip
    assert sorted(m._index) == [("observation_cd", "718-7"), ("patient_id", "P1")]


def test_a_long_patient_list_is_one_index_entry_per_patient_per_branch(observations):
    patients = [f"MRN{i:06d}" for i in range(2_000)] + ["P1", "P3"]
    sql = (f"SELECT patient_id FROM silver_observations WHERE patient_id IN ({', '.join(repr(p) for p in patients)}) "
           "AND (observation_cd_desc = 'Hemoglobin' OR abnormal_flag = 'HH')")  # fmt: skip
    m = simple(sql)
    assert m.index_size() == 2 * len(patients) and set(m._columns) == {"patient_id"}
    assert sorted(n["payload"]["patient_id"] for n in delivered(observations, [rule(sql)])) == ["P1", "P3"]


def test_a_record_that_matches_several_branches_is_delivered_once_with_one_notification_id(observations):
    sql = "SELECT patient_id FROM silver_observations WHERE patient_id = 'P1' OR value_num < 8 OR abnormal_flag = 'LL'"
    ids = [n["notificationId"] for n in delivered(observations, [rule(sql)])]
    assert len(ids) == len(set(ids)) == 2  # P1 (all three branches) and P3 (value_num 6.9)


def test_too_many_branches_keep_the_expression_path():
    from subscription_api.rules import MAX_DISJUNCTS

    groups = " AND ".join(f"(value_num > {i} OR units = 'u{i}')" for i in range(7))  # 2**7 = 128 branches
    assert MAX_DISJUNCTS < 2**7
    with pytest.raises(matching.RuleNotSimple):
        simple(f"SELECT patient_id FROM silver_observations WHERE {groups}")


def test_the_converted_customer_criteria_are_simple_and_indexed():
    """docs/examples/criteria.md's rules (OR of AND groups, long IN lists) are matched on the driver, each group indexed
    on its most selective list: patients when there is one, else codes or points of care, never the message type."""
    from pathlib import Path

    doc = (Path(__file__).resolve().parents[1] / "docs/examples/criteria.md").read_text()
    for sql in re.findall(r"`(SELECT [^`]+ FROM silver_[^`]+)`", doc):
        r = parse(sql.replace(", ...", ""), set(subscriptions.SOURCES), "cat.sub_app_a")
        everything = frozenset(f.name for f in subscriptions.SOURCES[r.source])
        m = matching.Matcher()
        m.place(subscriptions.runnable([Rule(SID, "c", 1, r.source, r.where, list(r.fields), "t", everything)])[0][0],
                "app")  # fmt: skip
        assert not m._scan, sql
        assert "msg_type" not in m._columns, (sql, set(m._columns))
        if "patient_id IN" in sql:
            assert set(m._columns) == {"patient_id"}


def test_the_read_plan_does_not_grow_with_a_patient_list(observations):
    """Simple rules aren't in the batch's plan at all: the driver matches them (a 1.2 M-row broadcast index once made
    every batch re-ship it; docs/SCALABILITY.md). The read renders the fields they select, whatever their lists."""

    def plan(n):
        patients = ["P1"] + [f"MRN{i:06d}" for i in range(n)]
        listed = ", ".join(repr(p) for p in patients)
        r = rule(f"SELECT patient_id FROM silver_observations WHERE patient_id IN ({listed})")
        df = engine.frame(observations, "silver_observations", set(r.fields), [], subscriptions.CHUNK)
        return re.sub(r"#\d+L?", "#", df._jdf.queryExecution().optimizedPlan().toString())  # expression IDs vary

    small, large = plan(10), plan(5_000)
    assert "MRN004999" not in large
    assert large == small
