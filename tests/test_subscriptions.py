"""Hub matching: every app's SQL rule over a micro-batch, in one pass, with the semantics of the SQL editor; matches
become notifications routed to each app's own topic."""

import json

import pytest
from pyspark.sql import functions as F

from hl7rt import gold, subscriptions
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
    """gold_observations topic records (value = gold JSON), with Kafka coordinates and broker timestamp."""
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
    out = subscriptions.notifications(
        subscriptions.events(observations, "gold_observations"), "gold_observations", rules
    )
    return [json.loads(x.value) | {"topic": x.topic, "key": x.key} for x in out.collect()]


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
    sql = f"SELECT patient_id FROM gold_observations WHERE {condition}"
    spark.createDataFrame(
        OBS, gold.OBSERVATION_SCHEMA.__class__([f for f in gold.OBSERVATION_SCHEMA if f.name in OBS[0]])
    ).createOrReplaceTempView("gold_observations")
    editor = sorted(r.patient_id for r in spark.sql(sql).collect())

    hub = sorted(n["payload"]["patient_id"] for n in delivered(observations, [rule(sql)]))
    assert hub == editor


def test_all_rules_run_in_one_pass_and_route_to_their_own_topics(observations):
    a = rule("SELECT patient_id, value_num FROM gold_observations WHERE observation_cd_desc = 'Hemoglobin' "
             "AND value_num < 8", name="low-hgb")  # fmt: skip
    b = rule("SELECT patient_id FROM gold_observations WHERE value_num > 6.5", sid="22222222-2222-4222-8222-2",
             name="high", topic="cat.subscribers.app-b")  # fmt: skip
    out = delivered(observations, [a, b])

    assert sorted((n["topic"], n["key"]) for n in out) == [
        ("cat.subscribers.app-a", "P1"),
        ("cat.subscribers.app-b", "P1"), ("cat.subscribers.app-b", "P2"), ("cat.subscribers.app-b", "P3"),
    ]  # fmt: skip
    hgb = next(n for n in out if n["subscription"] == "low-hgb")
    assert hgb["payload"] == {"patient_id": "P1", "value_num": 7.2}  # nothing beyond the selected fields
    assert hgb["revision"] == 1 and hgb["source"] == "gold_observations"
    assert (hgb["sourcePartition"], hgb["sourceOffset"]) == (0, 0)


def test_many_rules_split_into_chunks_give_the_same_answer(observations, monkeypatch):
    rules = [rule(f"SELECT patient_id FROM gold_observations WHERE value_num > {i}", sid=f"{i:08d}-0000-4000-8000-"
                  f"{i:012d}", name=f"r{i}") for i in range(15)]  # fmt: skip
    whole = sorted((n["subscription"], n["key"]) for n in delivered(observations, rules))
    monkeypatch.setattr(subscriptions, "CHUNK", 4)
    assert sorted((n["subscription"], n["key"]) for n in delivered(observations, rules)) == whole


def test_nulls_are_kept_in_the_payload(observations):
    out = delivered(
        observations, [rule("SELECT patient_id, abnormal_flag FROM gold_observations WHERE patient_id = 'P2'")]
    )
    assert out[0]["payload"] == {"patient_id": "P2", "abnormal_flag": None}


def test_notification_ids_are_stable_per_revision_and_source_record(observations):
    sql = "SELECT patient_id FROM gold_observations WHERE value_num < 8"

    def ids(revision):
        return [n["notificationId"] for n in delivered(observations, [rule(sql, revision=revision)])]

    assert ids(1) == ids(1)  # a redelivery of the same record keeps its ID
    assert ids(2) != ids(1)


def test_the_hub_rechecks_each_rule_before_running_it():
    ok = rule("SELECT patient_id FROM gold_observations WHERE value_num > 1")
    granted = frozenset({"patient_id", "value_num"})
    ungranted = rule("SELECT patient_id FROM gold_observations WHERE units = 'x'", sid="u", granted=granted)
    smuggled = Rule(
        subscription_id="s",
        name="s",
        revision=1,
        source="gold_observations",
        fields=["patient_id"],
        topic="t",
        granted=GRANTED,
        where="http_request('GET', 'x') IS NOT NULL",
    )  # written around the API
    good, failed = subscriptions.runnable([ok, ungranted, smuggled])

    assert good == [ok]
    assert failed["u"].startswith("FIELD_DENIED") and failed["s"].startswith("SQL_NOT_ALLOWED")


def test_a_rule_that_fails_at_runtime_is_found_alone(spark, observations):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    good = rule("SELECT patient_id FROM gold_observations WHERE value_num > 1")
    bad = rule(
        "SELECT patient_id FROM gold_observations WHERE units > 1", sid="bad"
    )  # 'g/dL' can't be cast to a number
    records = subscriptions.events(observations, "gold_observations")

    errors = subscriptions.failing(records, [good, bad])
    assert list(errors) == ["bad"] and "CAST" in errors["bad"].upper()
    assert len(delivered(observations, [good])) == 3


def test_no_rules_means_no_output(observations):
    assert subscriptions.notifications(subscriptions.events(observations, "gold_observations"), "gold_observations",
                                       []) is None  # fmt: skip
