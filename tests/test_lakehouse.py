"""Specs for the lakehouse mirror: topic records -> Delta rows with per-record latency telemetry."""

from datetime import datetime, timedelta

from pyspark.sql import functions as F

import hl7_samples as s
from hl7rt import gold, hl7v2, lakehouse
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
    return (
        hl7v2.to_silver_records(
            bronze,
            "real_time_mode_demo_catalog.hl7_realtime.silver_hl7",
            "real_time_mode_demo_catalog.hl7_realtime.quarantine_hl7",
        )
        .first()
        .value
    )


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
    q = hl7v2.to_silver_records(
        bronze,
        "real_time_mode_demo_catalog.hl7_realtime.silver_hl7",
        "real_time_mode_demo_catalog.hl7_realtime.quarantine_hl7",
    ).first()
    assert q.topic == "real_time_mode_demo_catalog.hl7_realtime.quarantine_hl7"
    row = lakehouse.quarantine_rows(_kafka(spark, [(q.value, T0 + timedelta(milliseconds=9))])).first()
    assert (row.reason, row.hl7, row.hop_ms) == ("not_hl7", "garbage", 4.0)


def test_mirror_rows_have_ingest_time(spark):
    v = _bronze_value(spark, s.adt_a04(), T0)
    out = lakehouse.bronze_rows(_kafka(spark, [(v, T0)]))
    assert "_ingested_at" in out.columns
    assert out.select(F.col("_ingested_at").isNotNull()).first()[0]


def test_gold_mirror_measures_silver_to_gold(spark):
    silver = _kafka(spark, [(_silver_value(spark, s.oru_r01(), T0, T0 + timedelta(milliseconds=20)),
                             T0 + timedelta(milliseconds=35))])  # fmt: skip
    obs = gold.to_gold_records(silver, "e", "real_time_mode_demo_catalog.hl7_realtime.gold_observations", "o").first()
    out = lakehouse.gold_rows(_kafka(spark, [(obs.value, T0 + timedelta(milliseconds=50))]), gold.OBSERVATION_SCHEMA)
    assert out.columns[: len(gold.OBSERVATION_SCHEMA)] == gold.OBSERVATION_SCHEMA.fieldNames()
    row = out.first()
    assert (row.observation_cd, row.value_num) == ("2823-3", 6.8)
    assert (row.hop_ms, row.e2e_ms) == (15.0, 50.0)
    assert row.kafka_timestamp == T0 + timedelta(milliseconds=50)


def test_every_mirror_table_has_its_upsert_key_columns(spark):
    """Delivery is at-least-once; each table upserts on lakehouse.KEYS, so those columns must exist and be set."""
    raw_ts, bronze_ts, silver_ts = T0, T0 + timedelta(milliseconds=10), T0 + timedelta(milliseconds=20)
    silver = _kafka(spark, [(_silver_value(spark, s.oru_r01(), raw_ts, bronze_ts), silver_ts)])
    obs = gold.to_gold_records(silver, "e", "o", "r").first()
    rows = {
        "raw_hl7": lakehouse.raw_rows(_kafka(spark, [(s.adt_a04(), T0, 0)], with_headers=True)),
        "bronze_hl7": lakehouse.bronze_rows(_kafka(spark, [(_bronze_value(spark, s.adt_a04(), T0), T0)])),
        "silver_hl7": lakehouse.silver_rows(silver),
        "gold_observations": lakehouse.gold_rows(_kafka(spark, [(obs.value, T0)]), gold.OBSERVATION_SCHEMA),
    }
    for table, df in rows.items():
        keys = lakehouse.KEYS[table]
        assert set(keys) <= set(df.columns), f"{table} lacks {set(keys) - set(df.columns)}"
        assert all(v is not None for v in df.select(*keys).first()), f"{table}: null upsert key"


def test_gold_tables_carry_the_same_meta_as_subscription_views(spark):
    """So a subscription's SQL runs unchanged in a SQL editor against the gold Delta tables."""
    from hl7rt import subscriptions

    silver = _kafka(spark, [(_silver_value(spark, s.oru_r01(), T0, T0 + timedelta(milliseconds=20)), T0)])
    obs = gold.to_gold_records(silver, "e", "o", "r").first()
    table = lakehouse.gold_rows(_kafka(spark, [(obs.value, T0 + timedelta(milliseconds=40))]), gold.OBSERVATION_SCHEMA)
    view = subscriptions._view_schema("gold_observations")[subscriptions.META]
    assert table.schema[subscriptions.META].dataType.simpleString() == view.dataType.simpleString()  # names + types
    meta = table.select(subscriptions.META).first()[0]
    assert meta.record_id == "0-0-1" and meta.gold_ts == T0 + timedelta(milliseconds=40) and meta.raw_ts is not None
