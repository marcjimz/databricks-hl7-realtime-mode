"""Specs for native-Spark HL7 v2 parsing (bronze envelope, the parsed message, quarantine reasons). The entity rows
built from it are specified in test_silver.py."""

import json
from datetime import datetime

import pytest
from pyspark.sql import functions as F

import hl7_samples as s
from hl7rt import hl7v2
from hl7rt.generators.hl7v2 import segment


def parse(spark, *messages):
    df = spark.createDataFrame([(m,) for m in messages], "hl7 string")
    return hl7v2.parse(df, F.col("hl7")).select(F.col("message").alias("m"), F.col("reason").alias("r"))


def one(spark, msg):
    row = parse(spark, msg).first()
    return row.m.asDict(recursive=True), row.r


# --- builder ------------------------------------------------------------------


def test_builder_matches_golden_message():
    msh = segment(
        "MSH",
        {3: "EPIC", 4: "FAC01", 5: "DATABRICKS", 6: "DBX", 7: "20261023083015", 9: "ADT^A04^ADT_A01",
         10: "CTRL0001", 11: "P", 12: "2.5.1", 13: "42"},
    )  # fmt: skip
    pid = segment("PID", {1: "1", 3: "MRN00042^^^EPIC^MR", 5: "DOE^JANE", 7: "19800101", 8: "F"})
    assert "\r".join([msh, pid]) == s.GOLDEN_ADT_A04


# --- the parsed message -------------------------------------------------------


def test_message_has_its_fields_then_every_obx_and_dg1(spark):
    m, _ = one(spark, s.adt_a04())
    assert list(m) == [*hl7v2.MESSAGE_FIELDS, "observations", "diagnoses"]


def test_adt_patient_visit_and_message_fields(spark):
    m, reason = one(spark, s.adt_a04())
    assert reason is None
    assert m["msg_control_id"] == "CTRL0001"
    assert m["msg_type"] == "ADT^A04"
    assert m["msg_sending_facility"] == "FAC01"
    assert m["msg_seq"] == "42"
    assert m["msg_dt_tm"] == datetime(2026, 10, 23, 8, 30, 15)
    assert (m["patient_id"], m["patient_id_namespace"]) == ("MRN00042", "EPIC")  # first repetition
    assert (m["patient_alt_id"], m["patient_alt_id_namespace"]) == ("ALT77", "LEGACY")
    assert m["date_of_birth_dt"] == datetime(1980, 1, 1)
    assert (m["sex"], m["race"], m["ethnicity"]) == ("F", "2106-3", "2186-5")
    assert m["account_id"] == "ACCT0001"
    assert (m["patient_class"], m["patient_point_of_care"], m["patient_facility"]) == ("E", "ED", "FAC01")
    assert m["visit_id"] == "V000000042"
    assert m["admit_dt_tm"] == datetime(2026, 10, 23, 8, 0, 0)
    assert m["observations"] == []


def test_adt_lists_every_diagnosis(spark):
    m, _ = one(spark, s.adt_a04())
    d1, d2 = m["diagnoses"]
    assert (d1["set_id"], d1["diagnosis_cd"], d1["diagnosis_cd_desc"], d1["diagnosis_cd_system"]) == (
        "1", "J18.9", "Pneumonia", "ICD-10-CM",
    )  # fmt: skip
    assert (d1["diagnosis_type"], d1["diagnosis_dt_tm"]) == ("ADMITTING", datetime(2026, 10, 23, 8, 5, 0))
    assert (d2["diagnosis_cd"], d2["diagnosis_type"], d2["diagnosis_dt_tm"]) == ("B05.9", "WORKING", None)


def test_oru_lists_every_observation_with_its_alternate_code(spark):
    m, reason = one(spark, s.oru_r01())
    assert reason is None
    assert (m["order_cd"], m["order_cd_desc"], m["order_cd_system"]) == ("80048", "Basic Metabolic Panel", "CPT")
    obs = m["observations"]
    assert [o["set_id"] for o in obs] == ["1", "2"]
    k = obs[0]
    assert (k["observation_cd"], k["observation_cd_desc"], k["observation_cd_system"]) == ("2823-3", "Potassium", "LN")
    assert (k["alt_observation_cd"], k["alt_observation_cd_desc"], k["alt_observation_cd_system"]) == (
        "K", "Potassium", "L",
    )  # fmt: skip
    assert (k["value_type"], k["value"], k["units"], k["reference_range"]) == ("NM", "6.8", "mmol/L", "3.5-5.1")
    assert (k["abnormal_flag"], k["result_status"]) == ("HH", "F")
    assert k["observation_dt_tm"] == datetime(2026, 10, 23, 8, 20, 0)
    assert obs[1]["observation_cd"] == "2345-7" and obs[1]["abnormal_flag"] == "N"


def test_orm_order_fields(spark):
    m, _ = one(spark, s.orm_o01())
    assert (m["order_cd"], m["order_cd_desc"], m["order_cd_system"]) == ("85025", "Complete Blood Count", "CPT")
    assert (m["alt_order_cd"], m["alt_order_cd_desc"], m["alt_order_cd_system"]) == ("CBC", "CBC", "L")


def test_mdm_document_status(spark):
    m, _ = one(spark, s.mdm_t02())
    assert m["document_completion_status"] == "AU"


# --- wire format robustness ---------------------------------------------------


@pytest.mark.parametrize(
    "wrap",
    [
        lambda m: m,
        lambda m: m.replace("\r", "\n"),
        lambda m: m.replace("\r", "\r\n"),
        lambda m: "\x0b" + m + "\x1c\r",  # MLLP framing
        lambda m: m + "\r\r",
    ],
)
def test_segment_separators_and_mllp_framing(spark, wrap):
    m, reason = one(spark, wrap(s.oru_r01()))
    assert reason is None
    assert m["msg_control_id"] == "CTRL0002"
    assert len(m["observations"]) == 2


def test_escape_sequences_are_decoded(spark):
    msg = s.adt_a04().replace("ED^01^A^FAC01", "ED\\T\\1^01^A^FAC\\S\\01")
    m, _ = one(spark, msg)
    assert m["patient_point_of_care"] == "ED&1"
    assert m["patient_facility"] == "FAC^01"


def test_empty_values_are_null(spark):
    msg = s.adt_a04()
    assert "|19800101|F|" in msg
    m, _ = one(spark, msg.replace("|19800101|F|", "|19800101||"))
    assert m["sex"] is None


# --- quarantine ---------------------------------------------------------------


@pytest.mark.parametrize(
    "msg, reason",
    [
        ("not an hl7 message", "not_hl7"),
        ("", "not_hl7"),
        ("PID|1||MRN1", "not_hl7"),
        ("MSH|^~\\&|A|B|C|D|20261023||ADT^A04||P|2.5.1", "missing_msg_control_id"),
        ("MSH|^~\\&|A|B|C|D|20261023|||CTRL9|P|2.5.1", "missing_msg_type"),
        ("MSH|#~\\&|A|B|C|D|20261023||ADT#A04|CTRL9|P|2.5.1", "unsupported_encoding"),
    ],
)
def test_quarantine_reasons(spark, msg, reason):
    assert parse(spark, msg).first().r == reason


# --- bronze envelope (what the bronze RTM flow writes) ---------------------------


def _kafka(spark, *values):
    return spark.createDataFrame(
        [
            (
                f"k{i}".encode(),
                v.encode(),
                "real_time_mode_demo_catalog.hl7_realtime.raw_hl7",
                0,
                i,
                datetime(2026, 10, 23, 12),
                1,
            )
            for i, v in enumerate(values)
        ],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, timestampType int",
    )


def test_bronze_envelope(spark):
    out = hl7v2.to_bronze_records(_kafka(spark, s.adt_a04()))
    assert out.columns == ["key", "value"]
    env = json.loads(out.first().value)
    assert env["hl7"] == s.adt_a04()
    assert (env["msg_type"], env["msg_control_id"], env["msg_sending_facility"]) == ("ADT^A04", "CTRL0001", "FAC01")
    assert (env["raw_partition"], env["raw_offset"]) == (0, 0)
    assert env["raw_ts"].startswith("2026-10-23T12:00:00")
