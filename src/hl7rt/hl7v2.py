"""Raw HL7 v2 parsing as native Spark expressions (no UDFs, no JARs), shared by the RTM bronze/silver flows.

Wire format handled: CR, LF, or CRLF segment separators; MLLP framing bytes (0x0B / 0x1C); repetitions
(first one is used); components; HL7 escape sequences (\\F\\ \\S\\ \\T\\ \\R\\ \\E\\). Only the standard
encoding characters (MSH-2 = ^~\\&) are supported; anything else is quarantined.

parse() reads each message once into `message`: the header, patient, and visit fields, the first order, the document
status, and every OBX and DG1 as arrays. hl7rt.silver turns it into entity rows. Mapping (HL7 field -> column; the
names are the Epic silver contract's):

    PID-3.1/.4  patient_id / _namespace      PID-4.1/.4  patient_alt_id / _namespace
    PID-7       date_of_birth_dt             PID-8 sex   PID-10.1 race   PID-22.1 ethnicity   PID-18.1 account_id
    PV1-2       patient_class                PV1-3.1 patient_point_of_care   PV1-3.4 patient_facility
    PV1-19.1    visit_id                     PV1-44 admit_dt_tm
    MSH-4.1     msg_sending_facility         MSH-7 msg_dt_tm   MSH-9.1^9.2 msg_type   MSH-10 msg_control_id
    MSH-13      msg_seq
    OBR-4 (first OBR)  order_cd/_desc/_system (.1-.3), alt_order_* (.4-.6)
    TXA-17             document_completion_status
    each OBX   set_id (1), value_type (2), observation_cd/_desc/_system (3.1-.3), alt_observation_* (3.4-.6),
               value (5), units (6.1), reference_range (7), abnormal_flag (8), result_status (11),
               observation_dt_tm (14)
    each DG1   set_id (1), diagnosis_cd/_desc/_system (3.1-.3), diagnosis_dt_tm (5), diagnosis_type (6)
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hl7rt.dtm import hl7_timestamp

ENCODING = "^~\\&"

# HL7 escape sequences -> literal (\E\ last so a decoded backslash isn't re-read as an escape).
_ESCAPES = [(r"\\F\\", "|"), (r"\\S\\", "^"), (r"\\T\\", "&"), (r"\\R\\", "~"), (r"\\E\\", r"\\\\")]


def segments(msg: Column) -> Column:
    """array<string> of non-empty segments, MLLP framing removed."""
    cleaned = F.regexp_replace(msg, "[\\x0b\\x1c]", "")
    return F.filter(F.split(cleaned, "[\\r\\n]+"), lambda s: s != "")


def _split_fields(seg: Column, is_msh: bool = False) -> Column:
    fields = F.split(seg, r"\|")
    # MSH-1 is the field separator itself, so insert it to make MSH-n line up with element n+1.
    return F.array_insert(fields, 2, F.lit("|")) if is_msh else fields


def first_segment(segs: Column, name: str) -> Column:
    """Fields of the first `name` segment (element n+1 = field n); NULL if absent."""
    seg = F.try_element_at(F.filter(segs, lambda s: s.startswith(name + "|")), F.lit(1))
    return _split_fields(seg, is_msh=name == "MSH")


def all_segments(segs: Column, name: str) -> Column:
    """array of field arrays for every `name` segment, in message order."""
    return F.transform(F.filter(segs, lambda s: s.startswith(name + "|")), lambda s: _split_fields(s))


def _unescape(v: Column) -> Column:
    for pattern, literal in _ESCAPES:
        v = F.regexp_replace(v, pattern, literal)
    return v


def field(fields: Column, n: int, component: int | None = None) -> Column:
    """Field n (first repetition), optionally one component, unescaped; empty -> NULL."""
    v = F.try_element_at(fields, F.lit(n + 1))
    v = F.try_element_at(F.split(v, "~"), F.lit(1))
    if component is not None:
        v = F.try_element_at(F.split(v, r"\^"), F.lit(component))
    v = _unescape(v)
    return F.when(v != "", v)


# --- Silver message -------------------------------------------------------------------------------------------

OBSERVATION_FIELDS: list[tuple[str, str]] = [
    ("set_id", "string"),
    ("value_type", "string"),
    ("observation_cd", "string"),
    ("observation_cd_desc", "string"),
    ("observation_cd_system", "string"),
    ("alt_observation_cd", "string"),
    ("alt_observation_cd_desc", "string"),
    ("alt_observation_cd_system", "string"),
    ("value", "string"),
    ("units", "string"),
    ("reference_range", "string"),
    ("abnormal_flag", "string"),
    ("result_status", "string"),
    ("observation_dt_tm", "timestamp"),
]


def _observation(obx: Column) -> Column:
    return F.struct(
        field(obx, 1).alias("set_id"),
        field(obx, 2).alias("value_type"),
        field(obx, 3, 1).alias("observation_cd"),
        field(obx, 3, 2).alias("observation_cd_desc"),
        field(obx, 3, 3).alias("observation_cd_system"),
        field(obx, 3, 4).alias("alt_observation_cd"),
        field(obx, 3, 5).alias("alt_observation_cd_desc"),
        field(obx, 3, 6).alias("alt_observation_cd_system"),
        field(obx, 5).alias("value"),
        field(obx, 6, 1).alias("units"),
        field(obx, 7).alias("reference_range"),
        field(obx, 8).alias("abnormal_flag"),
        field(obx, 11).alias("result_status"),
        hl7_timestamp(field(obx, 14)).alias("observation_dt_tm"),
    )


DIAGNOSIS_FIELDS: list[tuple[str, str]] = [
    ("set_id", "string"),
    ("diagnosis_cd", "string"),
    ("diagnosis_cd_desc", "string"),
    ("diagnosis_cd_system", "string"),
    ("diagnosis_type", "string"),
    ("diagnosis_dt_tm", "timestamp"),
]


def _diagnosis(dg1: Column) -> Column:
    return F.struct(
        field(dg1, 1).alias("set_id"),
        field(dg1, 3, 1).alias("diagnosis_cd"),
        field(dg1, 3, 2).alias("diagnosis_cd_desc"),
        field(dg1, 3, 3).alias("diagnosis_cd_system"),
        field(dg1, 6).alias("diagnosis_type"),
        hl7_timestamp(field(dg1, 5)).alias("diagnosis_dt_tm"),
    )


# Working columns added by with_segments(). Each segment is split ONCE in its own projection; Spark won't
# collapse a projection into the next if that would duplicate an expensive expression, so every field read
# below is a cheap array lookup. (Inlining segments() per field produced a 379 KB plan with 290 split() calls
# per record and starved the RTM driver in GC; see docs/latency.md's history link, Phase 2.)
_SEGS = "_hl7_segs"
_SEGMENT_COLS = {"MSH": "_hl7_msh", "PID": "_hl7_pid", "PV1": "_hl7_pv1", "OBR": "_hl7_obr", "TXA": "_hl7_txa"}
_REPEATING = {"OBX": "_hl7_obx", "DG1": "_hl7_dg1"}
_WORK_COLS = [_SEGS, *_SEGMENT_COLS.values(), *_REPEATING.values()]


def with_segments(df: DataFrame, msg: Column) -> DataFrame:
    """Adds the working columns: all segments, the first MSH/PID/PV1/OBR/TXA, and every OBX and DG1 (as field
    arrays)."""
    df = df.withColumn(_SEGS, segments(msg))
    segs = F.col(_SEGS)
    return df.select(
        "*",
        *[first_segment(segs, name).alias(col) for name, col in _SEGMENT_COLS.items()],
        *[all_segments(segs, name).alias(col) for name, col in _REPEATING.items()],
    )


def _seg(name: str) -> Column:
    return F.col(_SEGMENT_COLS[name])


# The fields of `message` before its arrays (hl7rt.silver picks each entity's from these).
MESSAGE_FIELDS: list[str] = [
    "patient_id", "patient_id_namespace", "patient_alt_id", "patient_alt_id_namespace", "date_of_birth_dt", "sex",
    "race", "ethnicity", "account_id", "visit_id", "patient_class", "patient_point_of_care", "patient_facility",
    "admit_dt_tm", "msg_type", "msg_control_id", "msg_dt_tm", "msg_sending_facility", "msg_seq", "order_cd",
    "order_cd_desc", "order_cd_system", "alt_order_cd", "alt_order_cd_desc", "alt_order_cd_system",
    "document_completion_status",
]  # fmt: skip


def message_struct() -> Column:
    """One message: MESSAGE_FIELDS, then `observations` (every OBX) and `diagnoses` (every DG1), read from
    with_segments() columns."""
    msh, pid, pv1, obr, txa = _seg("MSH"), _seg("PID"), _seg("PV1"), _seg("OBR"), _seg("TXA")
    msg_type = F.concat_ws("^", field(msh, 9, 1), field(msh, 9, 2))
    return F.struct(
        # Patient / visit
        field(pid, 3, 1).alias("patient_id"),
        field(pid, 3, 4).alias("patient_id_namespace"),
        field(pid, 4, 1).alias("patient_alt_id"),
        field(pid, 4, 4).alias("patient_alt_id_namespace"),
        hl7_timestamp(field(pid, 7)).alias("date_of_birth_dt"),
        field(pid, 8).alias("sex"),
        field(pid, 10, 1).alias("race"),
        field(pid, 22, 1).alias("ethnicity"),
        field(pid, 18, 1).alias("account_id"),
        field(pv1, 19, 1).alias("visit_id"),
        field(pv1, 2).alias("patient_class"),
        field(pv1, 3, 1).alias("patient_point_of_care"),
        field(pv1, 3, 4).alias("patient_facility"),
        hl7_timestamp(field(pv1, 44)).alias("admit_dt_tm"),
        # Message
        F.when(msg_type != "", msg_type).alias("msg_type"),
        field(msh, 10).alias("msg_control_id"),
        hl7_timestamp(field(msh, 7)).alias("msg_dt_tm"),
        field(msh, 4, 1).alias("msg_sending_facility"),
        field(msh, 13).alias("msg_seq"),
        # Order (first OBR)
        field(obr, 4, 1).alias("order_cd"),
        field(obr, 4, 2).alias("order_cd_desc"),
        field(obr, 4, 3).alias("order_cd_system"),
        field(obr, 4, 4).alias("alt_order_cd"),
        field(obr, 4, 5).alias("alt_order_cd_desc"),
        field(obr, 4, 6).alias("alt_order_cd_system"),
        # Document
        field(txa, 17).alias("document_completion_status"),
        F.transform(F.col(_REPEATING["OBX"]), _observation).alias("observations"),
        F.transform(F.col(_REPEATING["DG1"]), _diagnosis).alias("diagnoses"),
    )


def reason() -> Column:
    """Why a message can't be silvered (NULL if it can), read from with_segments() columns."""
    head = F.try_element_at(F.col(_SEGS), F.lit(1))
    msh = _seg("MSH")
    return (
        F.when(head.isNull() | ~head.startswith("MSH|"), "not_hl7")
        .when(F.try_element_at(msh, F.lit(3)) != ENCODING, "unsupported_encoding")  # MSH-2, raw
        .when(field(msh, 10).isNull(), "missing_msg_control_id")
        .when(field(msh, 9, 1).isNull(), "missing_msg_type")
    )


def parse(df: DataFrame, msg: Column) -> DataFrame:
    """df + `message` (silver struct) + `reason` (quarantine reason or NULL)."""
    staged = with_segments(df, msg)
    return staged.select("*", message_struct().alias("message"), reason().alias("reason")).drop(*_WORK_COLS)


# --- Topic records (what the RTM flows write) -----------------------------------------------------------------

BRONZE_SCHEMA = StructType(
    [
        StructField("hl7", StringType()),
        StructField("msg_type", StringType()),
        StructField("msg_control_id", StringType()),
        StructField("msg_sending_facility", StringType()),
        StructField("raw_partition", IntegerType()),
        StructField("raw_offset", LongType()),
        StructField("raw_ts", TimestampType()),
    ]
)


def _type(kind: str):
    return TimestampType() if kind == "timestamp" else StringType()


OBSERVATION_SCHEMA = StructType([StructField(n, _type(k)) for n, k in OBSERVATION_FIELDS])
DIAGNOSIS_SCHEMA = StructType([StructField(n, _type(k)) for n, k in DIAGNOSIS_FIELDS])
QUARANTINE_SCHEMA = StructType(
    [
        StructField("reason", StringType()),
        StructField("hl7", StringType()),
        StructField("raw_partition", IntegerType()),
        StructField("raw_offset", LongType()),
        StructField("raw_ts", TimestampType()),
        StructField("bronze_ts", TimestampType()),
    ]
)


def to_bronze_records(raw: DataFrame) -> DataFrame:
    """Kafka records from the raw topic -> (key, value) for the bronze topic: the message plus an envelope."""
    staged = with_segments(raw.withColumn("_hl7", F.col("value").cast("string")), F.col("_hl7"))
    msh = _seg("MSH")
    msg_type = F.concat_ws("^", field(msh, 9, 1), field(msh, 9, 2))
    envelope = F.struct(
        F.col("_hl7").alias("hl7"),
        F.when(msg_type != "", msg_type).alias("msg_type"),
        field(msh, 10).alias("msg_control_id"),
        field(msh, 4, 1).alias("msg_sending_facility"),
        F.col("partition").alias("raw_partition"),
        F.col("offset").alias("raw_offset"),
        F.col("timestamp").alias("raw_ts"),
    )
    return staged.select(F.col("key"), F.to_json(envelope).alias("value"))
