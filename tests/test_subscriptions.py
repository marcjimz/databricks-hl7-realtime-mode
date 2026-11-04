"""Hub matching: structured predicates become Spark column expressions (never SQL text) and matches become
notifications routed to each app's own topic."""

import json

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from hl7rt import gold, subscriptions
from hl7rt.subscriptions import PredicateError, Rule

OBS = [
    {"patient_id": "P1", "observation_cd_desc": "Hemoglobin", "value_num": 7.2, "units": "g/dL", "abnormal_flag": "LL"},
    {
        "patient_id": "P2",
        "observation_cd_desc": "Hemoglobin",
        "value_num": 13.0,
        "units": "g/dL",
        "abnormal_flag": None,
    },
    {
        "patient_id": "P3",
        "observation_cd_desc": "Potassium",
        "value_num": 6.9,
        "units": "mmol/L",
        "abnormal_flag": "HH",
    },
    {"patient_id": "P4", "observation_cd_desc": "Potassium", "value_num": None, "units": None, "abnormal_flag": None},
]


@pytest.fixture
def observations(spark):
    """gold_observations topic records (value = gold JSON), with Kafka coordinates and broker timestamp."""
    rows = [(json.dumps(r), 0, i, "2026-11-03T12:00:00Z") for i, r in enumerate(OBS)]
    return spark.createDataFrame(rows, "value string, partition int, offset long, ts string").select(
        F.col("value").cast("binary").alias("value"), "partition", "offset",
        F.to_timestamp("ts").alias("timestamp"),
    )  # fmt: skip


def matching(spark, predicate):
    schema = StructType([f for f in gold.OBSERVATION_SCHEMA if f.name in OBS[0]])
    rows = spark.createDataFrame([tuple(r[f.name] for f in schema) for r in OBS], schema)
    return sorted(
        r.patient_id for r in rows.where(subscriptions.compile_predicate(predicate, "gold_observations")).collect()
    )


@pytest.mark.parametrize(
    "predicate, expected",
    [
        ({"field": "value_num", "op": "lt", "value": 8}, ["P1", "P3"]),
        ({"field": "value_num", "op": "ge", "value": 6.9}, ["P1", "P2", "P3"]),
        ({"field": "observation_cd_desc", "op": "eq", "value": "Potassium"}, ["P3", "P4"]),
        ({"field": "abnormal_flag", "op": "ne", "value": "LL"}, ["P3"]),  # NULL <> 'LL' is unknown: no match
        ({"field": "abnormal_flag", "op": "is_null"}, ["P2", "P4"]),
        ({"field": "abnormal_flag", "op": "is_not_null"}, ["P1", "P3"]),
        ({"field": "patient_id", "op": "in", "value": ["P1", "P4", "P9"]}, ["P1", "P4"]),
        ({"field": "abnormal_flag", "op": "not_in", "value": ["HH"]}, ["P1"]),
        ({"field": "observation_cd_desc", "op": "contains", "value": "glob"}, ["P1", "P2"]),
        ({"field": "observation_cd_desc", "op": "starts_with", "value": "Pot"}, ["P3", "P4"]),
        ({"field": "units", "op": "ends_with", "value": "/L"}, ["P3"]),
        ({"field": "observation_cd_desc", "op": "contains", "value": "%"}, []),  # literal, not a wildcard
        (
            {"and": [{"field": "observation_cd_desc", "op": "eq", "value": "Hemoglobin"},
                     {"field": "value_num", "op": "lt", "value": 8}]},
            ["P1"],
        ),
        (
            {"or": [{"field": "abnormal_flag", "op": "eq", "value": "LL"},
                    {"field": "abnormal_flag", "op": "eq", "value": "HH"}]},
            ["P1", "P3"],
        ),
        ({"not": {"field": "value_num", "op": "gt", "value": 7}}, ["P3"]),  # NOT unknown is unknown: P4 excluded
    ],
)  # fmt: skip
def test_predicates_compile_to_spark_with_sql_null_semantics(spark, predicate, expected):
    assert matching(spark, predicate) == expected


@pytest.mark.parametrize(
    "predicate",
    [
        {"field": "nope", "op": "eq", "value": 1},
        {"field": "value_num", "op": "regex", "value": ".*"},
        {"field": "value_num", "op": "eq", "value": "1; DROP"},
        {"field": "value_num", "op": "gt"},
        {"field": "value_num", "op": "gt", "value": 1, "sql": "1=1"},
        {"and": [{"field": "value_num", "op": "gt", "value": 1}]},
        "value_num > (SELECT max(value_num) FROM gold_observations)",
    ],
)
def test_the_hub_rechecks_the_structure_and_never_takes_sql_text(predicate):
    with pytest.raises(PredicateError):
        subscriptions.compile_predicate(predicate, "gold_observations")


def test_every_gold_source_is_registered_with_typed_columns():
    cols = subscriptions.registry_columns()
    assert {s for s, _, _ in cols} == {"gold_encounters", "gold_observations", "gold_orders"}
    types = {(s, c): t for s, c, t in cols}
    assert types[("gold_observations", "value_num")] == "double"
    assert types[("gold_observations", "set_id")] == "int"
    assert types[("gold_observations", "observation_dt_tm")] == "timestamp"
    assert types[("gold_encounters", "patient_id")] == "string"


def rule(name="low-hgb", revision=1, fields=("patient_id", "value_num"), topic="cat.subscribers.app-a"):
    return Rule(
        subscription_id="11111111-1111-4111-8111-111111111111", name=name, revision=revision,
        source="gold_observations", fields=list(fields), topic=topic,
        predicate={"and": [{"field": "observation_cd_desc", "op": "eq", "value": "Hemoglobin"},
                           {"field": "value_num", "op": "lt", "value": 8}]},
    )  # fmt: skip


def test_matches_become_notifications_on_the_apps_topic_with_only_its_fields(observations):
    out = subscriptions.notifications(observations, "gold_observations", [rule()]).collect()

    assert len(out) == 1
    assert out[0].topic == "cat.subscribers.app-a"
    assert out[0].key == "P1"
    n = json.loads(out[0].value)
    assert n["subscription"] == "low-hgb" and n["revision"] == 1 and n["source"] == "gold_observations"
    assert n["payload"] == {"patient_id": "P1", "value_num": 7.2}  # nothing beyond the requested fields
    assert (n["sourcePartition"], n["sourceOffset"]) == (0, 0)


def test_notification_ids_are_stable_per_revision_and_source_record(observations):
    def ids(r):
        return [json.loads(x.value)["notificationId"] for x in
                subscriptions.notifications(observations, "gold_observations", [r]).collect()]  # fmt: skip

    assert ids(rule()) == ids(rule())  # a redelivery of the same record keeps its ID
    assert ids(rule(revision=2)) != ids(rule())


def test_each_rule_routes_to_its_own_topic(observations):
    k = Rule(subscription_id="22222222-2222-4222-8222-222222222222", name="high-k", revision=1,
             source="gold_observations", fields=["patient_id"], topic="cat.subscribers.app-b",
             predicate={"field": "value_num", "op": "gt", "value": 6.5})  # fmt: skip
    out = subscriptions.notifications(observations, "gold_observations", [rule(), k]).collect()
    assert sorted((r.topic, r.key) for r in out) == [
        ("cat.subscribers.app-a", "P1"),
        ("cat.subscribers.app-b", "P1"), ("cat.subscribers.app-b", "P2"), ("cat.subscribers.app-b", "P3"),
    ]  # fmt: skip


def test_no_rules_means_no_output(observations):
    assert subscriptions.notifications(observations, "gold_observations", []) is None
