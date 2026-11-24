"""Specs for silver: one bronze message -> rows of the entities apps subscribe to, each routed to its own topic, each
carrying the message's context (patient, visit, header) so a rule on one entity needs no join."""

import json
from datetime import datetime

from pyspark.sql import functions as F

import hl7_samples as s
from hl7rt import hl7v2, silver

PREFIX = "real_time_mode_demo_catalog.hl7_realtime."
TOPICS = {e: PREFIX + t for e, t in silver.TABLES.items()}
RAW_TS, BRONZE_TS = datetime(2026, 10, 23, 12, 0, 0), datetime(2026, 10, 23, 12, 0, 0, 10_000)


def _kafka(spark, *values, ts=RAW_TS):
    return spark.createDataFrame(
        [(b"MRN00042", v.encode(), "t", 0, i, ts, 1) for i, v in enumerate(values)],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, timestampType int",
    )


def _bronze(spark, *messages):
    """Bronze topic records (as Kafka rows) for raw messages, raw at RAW_TS, bronze at BRONZE_TS."""
    return _kafka(spark, *[r.value for r in hl7v2.to_bronze_records(_kafka(spark, *messages)).collect()], ts=BRONZE_TS)


def records(spark, *messages):
    out = silver.to_silver_records(_bronze(spark, *messages), **TOPICS)
    assert out.columns == ["key", "value", "topic"]
    return [(r.topic.removeprefix(PREFIX), json.loads(r.value), r.key) for r in out.collect()]


def by_table(spark, *messages):
    out = {}
    for table, value, _ in records(spark, *messages):
        out.setdefault(table, []).append(value)
    return out


def counts(spark, *messages):
    return {t: len(v) for t, v in by_table(spark, *messages).items()}


# --- routing ---------------------------------------------------------------------------------------------------


def test_adt_is_a_message_an_encounter_and_one_row_per_diagnosis(spark):
    assert counts(spark, s.adt_a04()) == {"silver_messages": 1, "silver_encounters": 1, "silver_diagnoses": 2}


def test_oru_is_a_message_and_one_observation_per_obx(spark):
    assert counts(spark, s.oru_r01()) == {"silver_messages": 1, "silver_observations": 2}


def test_orm_is_a_message_and_one_order(spark):
    assert counts(spark, s.orm_o01()) == {"silver_messages": 1, "silver_orders": 1}


def test_document_messages_are_messages_only(spark):
    assert counts(spark, s.mdm_t02()) == {"silver_messages": 1}


def test_unparseable_messages_go_to_quarantine_only(spark):
    (q,) = by_table(spark, "garbage")["quarantine_hl7"]
    assert counts(spark, "garbage") == {"quarantine_hl7": 1}
    assert (q["reason"], q["hl7"]) == ("not_hl7", "garbage")
    assert q["bronze_ts"].startswith("2026-10-23T12:00:00.010")


def test_key_is_kept_so_a_patients_records_stay_ordered(spark):
    assert {k for _, _, k in records(spark, s.adt_a04(), s.oru_r01())} == {b"MRN00042"}


def test_tables_are_the_topics_the_pipelines_write():
    assert silver.TABLES == {
        "messages": "silver_messages", "encounters": "silver_encounters", "observations": "silver_observations",
        "orders": "silver_orders", "diagnoses": "silver_diagnoses", "quarantine": "quarantine_hl7",
    }  # fmt: skip


# --- every entity carries the message's context ---------------------------------------------------------------


def test_every_entity_starts_with_the_message_context_and_ends_with_lineage(spark):
    for table, v, _ in records(spark, s.adt_a04(), s.oru_r01(), s.orm_o01()):
        if table == "quarantine_hl7":
            continue
        names = list(v)
        start = names.index("patient_id")
        assert names[start : start + len(silver.CONTEXT)] == silver.CONTEXT, table
        assert names[-len(silver.LINEAGE) :] == silver.LINEAGE, table
        assert (v["patient_id"], v["visit_id"], v["patient_point_of_care"]) == ("MRN00042", "V000000042", "ED")
        assert v["raw_ts"].startswith("2026-10-23T12:00:00.000")
        assert v["bronze_ts"].startswith("2026-10-23T12:00:00.010")


def test_message_fields(spark):
    (m,) = by_table(spark, s.adt_a04())["silver_messages"]
    assert list(m) == [f.name for f in silver.MESSAGE_SCHEMA]
    assert (m["msg_type"], m["msg_control_id"], m["msg_seq"], m["msg_sending_facility"]) == (
        "ADT^A04", "CTRL0001", "42", "FAC01",
    )  # fmt: skip
    assert (m["patient_id_namespace"], m["patient_alt_id"]) == ("EPIC", "ALT77")
    assert m["patient_alt_id_namespace"] == "LEGACY"
    assert (m["sex"], m["race"], m["ethnicity"], m["account_id"]) == ("F", "2106-3", "2186-5", "ACCT0001")
    assert m["date_of_birth_dt"].startswith("1980-01-01") and m["msg_dt_tm"].startswith("2026-10-23T08:30:15")
    assert m["payload"] == s.adt_a04()


def test_encounter_fields(spark):
    (e,) = by_table(spark, s.adt_a04())["silver_encounters"]
    assert list(e) == [f.name for f in silver.ENCOUNTER_SCHEMA]
    assert (e["event"], e["event_name"]) == ("A04", "registration")
    assert (e["patient_class"], e["patient_point_of_care"], e["patient_facility"]) == ("E", "ED", "FAC01")
    assert e["admit_dt_tm"].startswith("2026-10-23T08:00:00")


def test_encounter_event_names():
    assert silver.ADT_EVENTS == {
        "A01": "admission", "A02": "transfer", "A03": "discharge", "A04": "registration", "A08": "update",
    }  # fmt: skip


def test_observation_fields_include_the_alternate_code(spark):
    k, g = by_table(spark, s.oru_r01())["silver_observations"]
    assert list(k) == [f.name for f in silver.OBSERVATION_SCHEMA]
    assert (k["position"], k["set_id"], k["observation_cd"], k["observation_cd_desc"]) == (1, 1, "2823-3", "Potassium")
    assert (k["observation_cd_system"], k["alt_observation_cd"], k["alt_observation_cd_system"]) == ("LN", "K", "L")
    assert (k["value"], k["value_num"], k["units"], k["reference_range"]) == ("6.8", 6.8, "mmol/L", "3.5-5.1")
    assert (k["abnormal_flag"], k["result_status"]) == ("HH", "F")
    assert k["observation_dt_tm"].startswith("2026-10-23T08:20:00")
    assert (k["order_cd"], k["order_cd_desc"]) == ("80048", "Basic Metabolic Panel")
    assert (k["msg_type"], k["msg_control_id"]) == ("ORU^R01", "CTRL0002")
    assert (g["position"], g["observation_cd"], g["alt_observation_cd"], g["abnormal_flag"]) == (2, "2345-7", None, "N")


def test_non_numeric_observation_value_has_no_value_num(spark):
    msg = s.oru_r01()
    assert "|NM|2823-3^Potassium^LN^K^Potassium^L||6.8|" in msg
    msg = msg.replace(
        "|NM|2823-3^Potassium^LN^K^Potassium^L||6.8|", "|ST|2823-3^Potassium^LN^K^Potassium^L||hemolyzed|"
    )
    k, _ = by_table(spark, msg)["silver_observations"]
    assert (k["value_type"], k["value"], k["value_num"]) == ("ST", "hemolyzed", None)


def test_order_fields(spark):
    (o,) = by_table(spark, s.orm_o01())["silver_orders"]
    assert list(o) == [f.name for f in silver.ORDER_SCHEMA]
    assert (o["order_cd"], o["order_cd_desc"], o["order_cd_system"]) == ("85025", "Complete Blood Count", "CPT")
    assert (o["alt_order_cd"], o["alt_order_cd_desc"], o["alt_order_cd_system"]) == ("CBC", "CBC", "L")
    assert o["msg_control_id"] == "CTRL0003"


def test_diagnosis_fields(spark):
    d1, d2 = by_table(spark, s.adt_a04())["silver_diagnoses"]
    assert list(d1) == [f.name for f in silver.DIAGNOSIS_SCHEMA]
    assert (d1["position"], d1["set_id"], d1["diagnosis_cd"], d1["diagnosis_cd_desc"]) == (1, 1, "J18.9", "Pneumonia")
    assert (d1["diagnosis_cd_system"], d1["diagnosis_type"]) == ("ICD-10-CM", "ADMITTING")
    assert d1["diagnosis_dt_tm"].startswith("2026-10-23T08:05:00")
    assert (d2["position"], d2["diagnosis_cd"], d2["diagnosis_type"]) == (2, "B05.9", "WORKING")
    # The encounter's context is on each diagnosis: an "ADT, this point of care, this diagnosis" rule is one filter.
    assert (d1["msg_type"], d1["patient_class"], d1["patient_point_of_care"]) == ("ADT^A04", "E", "ED")


def test_a_missing_set_id_still_has_a_position(spark):
    msg = s.oru_r01().replace("OBX|1|NM|", "OBX||NM|")
    k, _ = by_table(spark, msg)["silver_observations"]
    assert (k["position"], k["set_id"]) == (1, None)


def test_silver_json_roundtrips_through_schemas(spark):
    schemas = {
        "silver_messages": silver.MESSAGE_SCHEMA, "silver_encounters": silver.ENCOUNTER_SCHEMA,
        "silver_observations": silver.OBSERVATION_SCHEMA, "silver_orders": silver.ORDER_SCHEMA,
        "silver_diagnoses": silver.DIAGNOSIS_SCHEMA, "quarantine_hl7": hl7v2.QUARANTINE_SCHEMA,
    }  # fmt: skip
    out = silver.to_silver_records(_bronze(spark, s.adt_a04(), s.oru_r01(), s.orm_o01(), "garbage"), **TOPICS)
    seen = set()
    for r in out.collect():
        table = r.topic.removeprefix(PREFIX)
        back = spark.createDataFrame([(r.value,)], "v string").select(F.from_json("v", schemas[table]).alias("x"))
        assert back.first().x.raw_partition == 0
        seen.add(table)
    assert seen == set(schemas)


# --- plan guard ------------------------------------------------------------------------------------------------


def test_silver_plan_splits_the_message_once_and_reads_bronze_json_once(spark):
    """Inlining the parse per field once produced 290 split() calls per record and a 379 KB plan that starved the
    RTM driver in GC; staged projections keep each segment split once, however many entities read it."""
    qe = silver.to_silver_records(_bronze(spark, s.oru_r01()), **TOPICS)._jdf.queryExecution()
    optimized, analyzed = qe.optimizedPlan().toString(), qe.analyzed().toString()
    assert optimized.count("split(regexp_replace(") == 1, "the full message is split more than once per record"
    assert optimized.count("from_json(") == 1, "the bronze envelope is parsed more than once"
    assert len(analyzed) < 60_000, f"analyzed plan is {len(analyzed):,} chars"
