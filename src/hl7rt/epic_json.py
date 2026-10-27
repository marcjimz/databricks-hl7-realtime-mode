"""The customer's Epic JSON feed (pre-parsed by an outside system), improved.

Payload on the topic:
    {"introspector": {...}, "schema": "...", "table": "...", "txType": "...",
     "columns": [{"name": "...", "value": "..."}, ...]}

Silver keeps the original column contract (names, order, types) so it is a drop-in replacement.
Instead of 34 `get(filter(columns, x -> x.name = '...'), 0)` scans per row, the columns array is
turned into a map once and each field is a single lookup. Rows that can't be normalized go to
quarantine with a reason instead of landing as all-null silver rows.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from hl7rt.dtm import hl7_timestamp

__all__ = ["FIELDS", "PAYLOAD_SCHEMA", "SPARK_CONF", "hl7_timestamp", "to_bronze", "to_quarantine", "to_silver"]

# map_from_entries over the *reversed* array with LAST_WIN keeps the first occurrence of a
# duplicated name, matching the original get(filter(...), 0) semantics. Set on the silver table.
SPARK_CONF = {"spark.sql.mapKeyDedupPolicy": "LAST_WIN"}

PAYLOAD_SCHEMA = StructType(
    [
        StructField("schema", StringType()),
        StructField("table", StringType()),
        StructField("txType", StringType()),
        StructField(
            "columns",
            ArrayType(StructType([StructField("name", StringType()), StructField("value", StringType())])),
        ),
    ]
)

# (name, type) in the customer's silver column order. "timestamp" fields accept HL7 DTM and ISO forms.
FIELDS: list[tuple[str, str]] = [
    # Patient / visit
    ("account_id", "string"),
    ("admit_dt_tm", "timestamp"),
    ("date_of_birth_dt", "timestamp"),
    ("ethnicity", "string"),
    ("patient_alt_id", "string"),
    ("patient_alt_id_namespace", "string"),
    ("patient_class", "string"),
    ("patient_facility", "string"),
    ("patient_fhir_resource_id", "string"),
    ("patient_id", "string"),
    ("patient_id_namespace", "string"),
    ("patient_point_of_care", "string"),
    ("race", "string"),
    ("sex", "string"),
    ("visit_id", "string"),
    # Message
    ("msg_control_id", "string"),
    ("msg_dt_tm", "timestamp"),
    ("msg_id", "string"),
    ("msg_sending_facility", "string"),
    ("msg_seq", "string"),
    ("msg_type", "string"),
    # Observation
    ("alt_observation_cd_system", "string"),
    ("alt_observation_cd", "string"),
    ("alt_observation_cd_desc", "string"),
    ("observation_cd_system", "string"),
    ("observation_cd", "string"),
    ("observation_cd_desc", "string"),
    # Order
    ("order_cd_system", "string"),
    ("order_cd", "string"),
    ("order_cd_desc", "string"),
    ("alt_order_cd", "string"),
    ("alt_order_cd_desc", "string"),
    ("alt_order_cd_system", "string"),
    # Document
    ("document_completion_status", "string"),
]

_CORRUPT = "_corrupt_record"


def to_bronze(kafka: DataFrame) -> DataFrame:
    """Raw Kafka record -> bronze row. Preserves all Kafka metadata."""
    return kafka.selectExpr(
        "CAST(key AS STRING) AS event_key",
        "CAST(value AS STRING) AS event_value",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
        "timestampType AS kafka_timestamp_type",
    ).withColumn("_ingested_at", F.current_timestamp())


def _with_fields(bronze: DataFrame) -> DataFrame:
    """Adds `_parsed` (payload struct) and `_m` (column name -> value), computed once per row.

    Malformed JSON surfaces in the corrupt-record column, so no second parse is needed to tell
    it apart from a payload without `columns`. The parse is wrapped in inline(array(struct(...)))
    on purpose: a generator is an optimizer barrier, so the validity filter can't be pushed below
    it and re-inline from_json/map_from_entries into the filter (measured: 5 parses per row
    without the barrier, 1 with it; ~2.7x the original throughput).
    """
    schema = StructType([*PAYLOAD_SCHEMA.fields, StructField(_CORRUPT, StringType())])
    parsed = F.from_json(F.col("event_value"), schema, {"columnNameOfCorruptRecord": _CORRUPT})
    named = F.filter(parsed["columns"], lambda x: x["name"].isNotNull())
    row = F.struct(
        *[F.col(c) for c in bronze.columns],
        parsed.alias("_parsed"),
        F.map_from_entries(F.reverse(named)).alias("_m"),
    )
    return bronze.select(F.inline(F.array(row)))


def _reason() -> Column:
    return (
        F.when(F.col("_parsed").isNull() | F.col(f"_parsed.{_CORRUPT}").isNotNull(), "invalid_json")
        .when(F.col("_parsed.columns").isNull(), "missing_columns")
        .when(F.try_element_at(F.col("_m"), F.lit("msg_control_id")).isNull(), "missing_msg_control_id")
    )


def to_silver(bronze: DataFrame) -> DataFrame:
    """Bronze -> silver with the customer's column contract. Invalid rows are excluded (see to_quarantine)."""

    def field(name: str, kind: str) -> Column:
        v = F.try_element_at(F.col("_m"), F.lit(name))
        return (hl7_timestamp(v) if kind == "timestamp" else v).alias(name)

    return (
        _with_fields(bronze)
        .where(_reason().isNull())
        .select(
            *[field(n, k) for n, k in FIELDS],
            F.col("event_value").alias("payload"),
            "kafka_timestamp",
            "_ingested_at",
        )
    )


def to_quarantine(bronze: DataFrame) -> DataFrame:
    """Rows that can't be normalized, with the reason and their Kafka coordinates for replay."""
    return (
        _with_fields(bronze)
        .withColumn("reason", _reason())
        .where(F.col("reason").isNotNull())
        .select("reason", "event_key", "event_value", "topic", "partition", "offset", "kafka_timestamp", "_ingested_at")
    )
