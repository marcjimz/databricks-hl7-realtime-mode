"""Specs for the lakehouse mirror: topic records -> Delta rows with per-record latency telemetry."""

from datetime import datetime, timedelta

from pyspark.sql import functions as F

import hl7_samples as s
from hl7rt import hl7v2, lakehouse
from test_epic_json import CUSTOMER_SILVER_COLUMNS

T0 = datetime(2026, 10, 23, 12, 0, 0)


def _kafka(spark, rows, with_headers=False):
    """rows: (value:str, broker_ts:datetime[, sent_ms:int])"""
    data = []
    for i, r in enumerate(rows):
        headers = [("sent_ms", str(r[2]).encode())] if with_headers else []
        data.append((b"k", r[0].encode(), "t", 0, i, r[1], 1, headers))
    return spark.createDataFrame(
        data,
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, "
        "timestampType int, headers array<struct<key:string,value:binary>>",
    )


def _bronze_value(spark, msg, raw_ts):
    raw = _kafka(spark, [(msg, raw_ts)])
    return hl7v2.to_bronze_records(raw).first().value


def _silver_value(spark, msg, raw_ts, bronze_ts):
    bronze = _kafka(spark, [(_bronze_value(spark, msg, raw_ts), bronze_ts)])
    return hl7v2.to_silver_records(bronze, "hl7.silver", "hl7.quarantine").first().value


def test_raw_mirror_measures_producer_to_broker(spark):
    sent = int((T0 - timedelta(milliseconds=12)).replace(tzinfo=None).timestamp() * 1000)
    row = lakehouse.raw_rows(_kafka(spark, [(s.adt_a04(), T0, sent)], with_headers=True)).first()
    assert row.hl7 == s.adt_a04()
    assert row.hop_ms == 12.0


def test_bronze_mirror_hop(spark):
    v = _bronze_value(spark, s.adt_a04(), T0)
    row = lakehouse.bronze_rows(_kafka(spark, [(v, T0 + timedelta(milliseconds=20))])).first()
    assert (row.msg_type, row.msg_control_id) == ("ADT^A04", "CTRL0001")
    assert (row.hop_ms, row.e2e_ms) == (20.0, 20.0)


def test_silver_mirror_keeps_contract_and_measures(spark):
    v = _silver_value(spark, s.oru_r01(), T0, T0 + timedelta(milliseconds=20))
    out = lakehouse.silver_rows(_kafka(spark, [(v, T0 + timedelta(milliseconds=35))]))
    assert out.columns[: len(CUSTOMER_SILVER_COLUMNS)] == CUSTOMER_SILVER_COLUMNS
    row = out.first()
    assert row.msg_control_id == "CTRL0002" and len(row.observations) == 2
    assert (row.hop_ms, row.e2e_ms) == (15.0, 35.0)
    assert row.kafka_timestamp == T0 + timedelta(milliseconds=35)


def test_quarantine_mirror(spark):
    bronze = _kafka(spark, [(_bronze_value(spark, "garbage", T0), T0 + timedelta(milliseconds=5))])
    q = hl7v2.to_silver_records(bronze, "hl7.silver", "hl7.quarantine").first()
    assert q.topic == "hl7.quarantine"
    row = lakehouse.quarantine_rows(_kafka(spark, [(q.value, T0 + timedelta(milliseconds=9))])).first()
    assert (row.reason, row.hl7, row.hop_ms) == ("not_hl7", "garbage", 4.0)


def test_mirror_rows_have_ingest_time(spark):
    v = _bronze_value(spark, s.adt_a04(), T0)
    out = lakehouse.bronze_rows(_kafka(spark, [(v, T0)]))
    assert "_ingested_at" in out.columns
    assert out.select(F.col("_ingested_at").isNotNull()).first()[0]
