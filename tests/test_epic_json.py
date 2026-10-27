"""Specs for the Epic JSON feed (the customer's existing bronze/silver), improved."""

from datetime import datetime

import pytest

from conftest import epic_payload
from hl7rt import epic_json

# The customer's silver column contract, in order. Downstream consumers depend on this.
CUSTOMER_SILVER_COLUMNS = [
    "account_id", "admit_dt_tm", "date_of_birth_dt", "ethnicity", "patient_alt_id", "patient_alt_id_namespace",
    "patient_class", "patient_facility", "patient_fhir_resource_id", "patient_id", "patient_id_namespace",
    "patient_point_of_care", "race", "sex", "visit_id", "msg_control_id", "msg_dt_tm", "msg_id",
    "msg_sending_facility", "msg_seq", "msg_type", "alt_observation_cd_system", "alt_observation_cd",
    "alt_observation_cd_desc", "observation_cd_system", "observation_cd", "observation_cd_desc", "order_cd_system",
    "order_cd", "order_cd_desc", "alt_order_cd", "alt_order_cd_desc", "alt_order_cd_system",
    "document_completion_status", "payload", "kafka_timestamp", "_ingested_at",
]  # fmt: skip

TIMESTAMP_COLUMNS = {"admit_dt_tm", "date_of_birth_dt", "msg_dt_tm"}


def _full_columns():
    cols = {c: f"v_{c}" for c in CUSTOMER_SILVER_COLUMNS[:34]}
    cols.update(admit_dt_tm="20261022083000", date_of_birth_dt="19800101", msg_dt_tm="20261022083015")
    return cols


def test_silver_keeps_customer_column_contract(bronze_df):
    out = epic_json.to_silver(bronze_df(epic_payload(_full_columns())))
    assert out.columns == CUSTOMER_SILVER_COLUMNS


def test_silver_types(bronze_df):
    out = epic_json.to_silver(bronze_df(epic_payload(_full_columns())))
    types = dict(out.dtypes)
    for c in CUSTOMER_SILVER_COLUMNS[:34]:
        assert types[c] == ("timestamp" if c in TIMESTAMP_COLUMNS else "string"), c


def test_silver_extracts_every_field(bronze_df):
    cols = _full_columns()
    row = epic_json.to_silver(bronze_df(epic_payload(cols))).first().asDict()
    for c in CUSTOMER_SILVER_COLUMNS[:34]:
        if c not in TIMESTAMP_COLUMNS:
            assert row[c] == cols[c], c
    assert row["admit_dt_tm"] == datetime(2026, 10, 22, 8, 30, 0)
    assert row["date_of_birth_dt"] == datetime(1980, 1, 1)


def test_missing_field_is_null(bronze_df):
    row = epic_json.to_silver(bronze_df(epic_payload({"msg_control_id": "C1"}))).first()
    assert row.msg_control_id == "C1"
    assert row.patient_id is None


def test_duplicate_field_first_occurrence_wins(bronze_df):
    # Matches the original get(filter(...), 0) semantics.
    payload = epic_payload({"msg_control_id": "C1"}).replace(
        '"columns": [',
        '"columns": [{"name": "patient_id", "value": "FIRST"}, {"name": "patient_id", "value": "SECOND"}, ',
    )
    assert epic_json.to_silver(bronze_df(payload)).first().patient_id == "FIRST"


def test_null_column_names_are_ignored(bronze_df):
    payload = epic_payload({"msg_control_id": "C1"}).replace(
        '"columns": [', '"columns": [{"name": null, "value": "x"}, '
    )
    assert epic_json.to_silver(bronze_df(payload)).first().msg_control_id == "C1"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("20261022083015", datetime(2026, 10, 22, 8, 30, 15)),
        ("202610220830", datetime(2026, 10, 22, 8, 30)),
        ("20261022", datetime(2026, 10, 22)),
        ("20261022083015.1234", datetime(2026, 10, 22, 8, 30, 15)),
        ("20261022083015-0500", datetime(2026, 10, 22, 13, 30, 15)),
        ("2026-10-22T08:30:15", datetime(2026, 10, 22, 8, 30, 15)),
        ("2026-10-22 08:30:15", datetime(2026, 10, 22, 8, 30, 15)),
        ("not-a-date", None),
        ("", None),
    ],
)
def test_hl7_timestamp_formats(spark, raw, expected):
    from pyspark.sql import functions as F

    got = spark.createDataFrame([(raw,)], "v string").select(epic_json.hl7_timestamp(F.col("v")).alias("t")).first().t
    assert got == expected


def test_invalid_payloads_are_quarantined_not_silvered(bronze_df):
    good = epic_payload({"msg_control_id": "C1"})
    df = bronze_df(good, "{not json", '{"schema": "EPIC"}', epic_payload({"patient_id": "P1"}))
    silver = epic_json.to_silver(df)
    quarantine = epic_json.to_quarantine(df)
    assert [r.msg_control_id for r in silver.collect()] == ["C1"]
    reasons = sorted(r.reason for r in quarantine.collect())
    assert reasons == ["invalid_json", "missing_columns", "missing_msg_control_id"]
    assert set(quarantine.columns) >= {"reason", "event_value", "kafka_timestamp", "offset"}


def test_payload_column_is_original_event_value(bronze_df):
    p = epic_payload({"msg_control_id": "C1"})
    assert epic_json.to_silver(bronze_df(p)).first().payload == p


def test_to_bronze_casts_kafka_record(spark):
    kafka = spark.createDataFrame(
        [
            (
                b"k",
                b"v",
                "real_time_mode_demo_catalog.hl7_realtime.epic_near_realtime_json_from_eventhub",
                2,
                7,
                datetime(2026, 10, 23, 12),
                1,
            )
        ],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, timestampType int",
    )
    out = epic_json.to_bronze(kafka)
    assert out.columns == [
        "event_key", "event_value", "topic", "partition", "offset", "kafka_timestamp", "kafka_timestamp_type",
        "_ingested_at",
    ]  # fmt: skip
    row = out.first()
    assert (row.event_key, row.event_value, row.partition, row.offset) == ("k", "v", 2, 7)
