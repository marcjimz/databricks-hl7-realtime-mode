"""Specs for the lakehouse mirror: topic records -> Delta rows with per-record latency telemetry."""

from datetime import datetime, timedelta

from pyspark.sql import functions as F

import hl7_samples as s
from hl7rt import hl7v2, lakehouse, silver
from test_silver import TOPICS

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


def _silver_values(spark, msg, raw_ts, bronze_ts) -> dict[str, list[str]]:
    """table -> the silver topic values for one raw message (raw at raw_ts, bronze at bronze_ts)."""
    bronze = _kafka(spark, [(_bronze_value(spark, msg, raw_ts), bronze_ts)])
    out = {}
    for r in silver.to_silver_records(bronze, **TOPICS).collect():
        out.setdefault(r.topic.rsplit(".", 1)[1], []).append(r.value)
    return out


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


def test_silver_mirror_keeps_the_entity_shape_and_measures_bronze_to_silver(spark):
    values = _silver_values(spark, s.oru_r01(), T0, T0 + timedelta(milliseconds=20))
    out = lakehouse.silver_rows(_kafka(spark, [(values["silver_observations"][0], T0 + timedelta(milliseconds=35))]),
                                "silver_observations")  # fmt: skip
    assert out.columns[: len(silver.OBSERVATION_SCHEMA)] == silver.OBSERVATION_SCHEMA.fieldNames()
    row = out.first()
    assert (row.observation_cd, row.value_num, row.alt_observation_cd) == ("2823-3", 6.8, "K")
    assert (row.hop_ms, row.e2e_ms) == (15.0, 35.0)
    assert row.kafka_timestamp == T0 + timedelta(milliseconds=35)
    assert (row._meta.record_id, row._meta.source_ts) == ("0-0-1", T0 + timedelta(milliseconds=35))


def test_quarantine_mirror(spark):
    bronze = _kafka(spark, [(_bronze_value(spark, "garbage", T0), T0 + timedelta(milliseconds=5))])
    q = silver.to_silver_records(bronze, **TOPICS).first()
    assert q.topic == "real_time_mode_demo_catalog.hl7_realtime.quarantine_hl7"
    row = lakehouse.quarantine_rows(_kafka(spark, [(q.value, T0 + timedelta(milliseconds=9))])).first()
    assert (row.reason, row.hl7, row.hop_ms) == ("not_hl7", "garbage", 4.0)


def test_mirror_rows_have_ingest_time(spark):
    v = _bronze_value(spark, s.adt_a04(), T0)
    out = lakehouse.bronze_rows(_kafka(spark, [(v, T0)]))
    assert "_ingested_at" in out.columns
    assert out.select(F.col("_ingested_at").isNotNull()).first()[0]


def test_every_mirror_table_has_its_upsert_key_columns(spark):
    """Delivery is at-least-once; each table upserts on lakehouse.KEYS, so those columns must exist and be set."""
    values = {}
    for msg in (s.adt_a04(), s.oru_r01(), s.orm_o01(), "garbage"):
        for table, v in _silver_values(spark, msg, T0, T0 + timedelta(milliseconds=10)).items():
            values.setdefault(table, v)
    rows = {
        "raw_hl7": lakehouse.raw_rows(_kafka(spark, [(s.adt_a04(), T0, 0)], with_headers=True)),
        "bronze_hl7": lakehouse.bronze_rows(_kafka(spark, [(_bronze_value(spark, s.adt_a04(), T0), T0)])),
        "quarantine_hl7": lakehouse.quarantine_rows(_kafka(spark, [(values["quarantine_hl7"][0], T0)])),
    }
    for t in silver.SCHEMAS:
        if t != "quarantine_hl7":
            rows[t] = lakehouse.silver_rows(_kafka(spark, [(values[t][0], T0)]), t)
    assert set(rows) == set(lakehouse.KEYS)
    for table, df in rows.items():
        keys = lakehouse.KEYS[table]
        assert set(keys) <= set(df.columns), f"{table} lacks {set(keys) - set(df.columns)}"
        assert all(v is not None for v in df.select(*keys).first()), f"{table}: null upsert key"
