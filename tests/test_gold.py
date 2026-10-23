"""Specs for gold: one silver record -> encounter / observation / order records, routed to their topics."""

import json
from datetime import datetime

from pyspark.sql import functions as F

import hl7_samples as s
from hl7rt import gold, hl7v2

TOPICS = {
    "encounters": "real_time_mode_demo_catalog.hl7_realtime.gold_encounters",
    "observations": "real_time_mode_demo_catalog.hl7_realtime.gold_observations",
    "orders": "real_time_mode_demo_catalog.hl7_realtime.gold_orders",
}
RAW_TS, BRONZE_TS, SILVER_TS = (datetime(2026, 10, 23, 12, 0, 0, ms * 1000) for ms in (0, 10, 25))


def _kafka(spark, *values, ts=RAW_TS):
    return spark.createDataFrame(
        [(b"MRN00042", v.encode(), "t", 0, i, ts, 1) for i, v in enumerate(values)],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, timestampType int",
    )


def _silver(spark, *messages):
    """Silver topic records (as Kafka rows) for the given raw messages; lineage timestamps RAW/BRONZE/SILVER_TS."""
    bronze = [r.value for r in hl7v2.to_bronze_records(_kafka(spark, *messages)).collect()]
    silver = hl7v2.to_silver_records(
        _kafka(spark, *bronze, ts=BRONZE_TS),
        "real_time_mode_demo_catalog.hl7_realtime.silver_hl7",
        "real_time_mode_demo_catalog.hl7_realtime.quarantine_hl7",
    )
    return _kafka(
        spark,
        *[r.value for r in silver.collect() if r.topic == "real_time_mode_demo_catalog.hl7_realtime.silver_hl7"],
        ts=SILVER_TS,
    )


def records(spark, *messages):
    out = gold.to_gold_records(_silver(spark, *messages), **TOPICS)
    assert out.columns == ["key", "value", "topic"]
    return [(r.topic, json.loads(r.value), r.key) for r in out.collect()]


def by_topic(spark, *messages):
    out = {}
    for topic, value, _ in records(spark, *messages):
        out.setdefault(topic, []).append(value)
    return out


# --- routing -------------------------------------------------------------------------------------------------


def test_adt_becomes_one_encounter(spark):
    assert {t: len(v) for t, v in by_topic(spark, s.adt_a04()).items()} == {
        "real_time_mode_demo_catalog.hl7_realtime.gold_encounters": 1
    }


def test_oru_becomes_one_observation_per_obx(spark):
    assert {t: len(v) for t, v in by_topic(spark, s.oru_r01()).items()} == {
        "real_time_mode_demo_catalog.hl7_realtime.gold_observations": 2
    }


def test_orm_becomes_one_order(spark):
    assert {t: len(v) for t, v in by_topic(spark, s.orm_o01()).items()} == {
        "real_time_mode_demo_catalog.hl7_realtime.gold_orders": 1
    }


def test_document_messages_have_no_gold_entity(spark):
    assert records(spark, s.mdm_t02()) == []


def test_key_is_kept_so_a_patients_records_stay_ordered(spark):
    assert {k for _, _, k in records(spark, s.adt_a04(), s.oru_r01())} == {b"MRN00042"}


# --- entities ------------------------------------------------------------------------------------------------


def test_encounter_fields(spark):
    (e,) = by_topic(spark, s.adt_a04())["real_time_mode_demo_catalog.hl7_realtime.gold_encounters"]
    assert list(e) == [f.name for f in gold.ENCOUNTER_SCHEMA]
    assert (e["event"], e["event_name"]) == ("A04", "registration")
    assert (e["visit_id"], e["patient_id"], e["account_id"]) == ("V000000042", "MRN00042", "ACCT0001")
    assert (e["patient_class"], e["patient_point_of_care"], e["patient_facility"]) == ("E", "ED", "FAC01")
    assert e["admit_dt_tm"].startswith("2026-10-23T08:00:00")
    assert (e["msg_type"], e["msg_control_id"]) == ("ADT^A04", "CTRL0001")


def test_encounter_event_names():
    assert gold.ADT_EVENTS == {"A01": "admission", "A03": "discharge", "A04": "registration", "A08": "update"}


def test_observation_fields(spark):
    k, g = by_topic(spark, s.oru_r01())["real_time_mode_demo_catalog.hl7_realtime.gold_observations"]
    assert list(k) == [f.name for f in gold.OBSERVATION_SCHEMA]
    assert (k["set_id"], k["observation_cd"], k["observation_cd_desc"]) == (1, "2823-3", "Potassium")
    assert (k["value"], k["value_num"], k["units"], k["reference_range"]) == ("6.8", 6.8, "mmol/L", "3.5-5.1")
    assert (k["abnormal_flag"], k["result_status"]) == ("HH", "F")
    assert k["observation_dt_tm"].startswith("2026-10-23T08:20:00")
    assert (k["order_cd"], k["order_cd_desc"]) == ("80048", "Basic Metabolic Panel")
    assert (k["patient_id"], k["visit_id"], k["patient_facility"]) == ("MRN00042", "V000000042", "FAC01")
    assert k["msg_control_id"] == "CTRL0002"
    assert (g["set_id"], g["observation_cd"], g["abnormal_flag"]) == (2, "2345-7", "N")


def test_non_numeric_observation_value_has_no_value_num(spark):
    msg = s.oru_r01()
    assert "|NM|2823-3^Potassium^LN^K^Potassium^L||6.8|" in msg
    msg = msg.replace(
        "|NM|2823-3^Potassium^LN^K^Potassium^L||6.8|", "|ST|2823-3^Potassium^LN^K^Potassium^L||hemolyzed|"
    )
    k, _ = by_topic(spark, msg)["real_time_mode_demo_catalog.hl7_realtime.gold_observations"]
    assert (k["value_type"], k["value"], k["value_num"]) == ("ST", "hemolyzed", None)


def test_order_fields(spark):
    (o,) = by_topic(spark, s.orm_o01())["real_time_mode_demo_catalog.hl7_realtime.gold_orders"]
    assert list(o) == [f.name for f in gold.ORDER_SCHEMA]
    assert (o["order_cd"], o["order_cd_desc"], o["order_cd_system"]) == ("85025", "Complete Blood Count", "CPT")
    assert (o["alt_order_cd"], o["alt_order_cd_desc"], o["alt_order_cd_system"]) == ("CBC", "CBC", "L")
    assert (o["patient_id"], o["visit_id"], o["msg_control_id"]) == ("MRN00042", "V000000042", "CTRL0003")


def test_every_entity_carries_lineage_timestamps(spark):
    for _, v, _ in records(spark, s.adt_a04(), s.oru_r01(), s.orm_o01()):
        assert v["raw_ts"].startswith("2026-10-23T12:00:00.000")
        assert v["bronze_ts"].startswith("2026-10-23T12:00:00.010")
        assert v["silver_ts"].startswith("2026-10-23T12:00:00.025")


def test_gold_json_roundtrips_through_schemas(spark):
    schemas = {
        "real_time_mode_demo_catalog.hl7_realtime.gold_encounters": gold.ENCOUNTER_SCHEMA,
        "real_time_mode_demo_catalog.hl7_realtime.gold_observations": gold.OBSERVATION_SCHEMA,
        "real_time_mode_demo_catalog.hl7_realtime.gold_orders": gold.ORDER_SCHEMA,
    }
    out = gold.to_gold_records(_silver(spark, s.adt_a04(), s.oru_r01(), s.orm_o01()), **TOPICS).collect()
    for r in out:
        back = spark.createDataFrame([(r.value,)], "v string").select(F.from_json("v", schemas[r.topic]).alias("x"))
        assert back.first().x.msg_control_id is not None


# --- plan guard ----------------------------------------------------------------------------------------------


def test_gold_parses_the_silver_json_once(spark):
    """The optimizer can re-inline from_json into every reference (seen 5x on the Epic silver)."""
    plan = gold.to_gold_records(_silver(spark, s.oru_r01()), **TOPICS)._jdf.queryExecution().optimizedPlan()
    assert plan.toString().count("from_json(") == 1
