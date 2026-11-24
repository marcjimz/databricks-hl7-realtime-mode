"""Silver: each bronze message, parsed once (hl7v2.parse), as rows of the entities apps subscribe to, one topic each.

    every message  -> 1 row     silver_messages      header, patient, visit, and the raw payload
    ADT^*          -> 1 row     silver_encounters    event A01/A02/A03/A04/A08 -> admission/transfer/...
    each OBX       -> 1 row     silver_observations  numeric value_num for NM values (in practice ORU^R01)
    ORM^*          -> 1 row     silver_orders
    each DG1       -> 1 row     silver_diagnoses
    can't be parsed -> 1 row    quarantine_hl7       with the reason

Every entity row carries the message's context (CONTEXT: patient, visit, header), so a rule on one entity needs no
join: "ADT A01/A04/A08 at these points of care with diagnosis B05.9" is one filter on silver_diagnoses. A repeating
segment's row has its `position` in the message (1-based): set IDs are optional in HL7, positions always exist.

One RTM flow writes every topic: each bronze record becomes an array of (topic, value) records that is exploded, and the
Kafka sink routes by the `topic` column. Rows end with LINEAGE (the raw and bronze broker timestamps, for latency, and
the raw-topic coordinates, the upsert key in lakehouse.KEYS).
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from hl7rt import hl7v2

# entity -> its table (and topic, <catalog>.<schema>.<table>)
TABLES = {
    "messages": "silver_messages",
    "encounters": "silver_encounters",
    "observations": "silver_observations",
    "orders": "silver_orders",
    "diagnoses": "silver_diagnoses",
    "quarantine": "quarantine_hl7",
}
ADT_EVENTS = {"A01": "admission", "A02": "transfer", "A03": "discharge", "A04": "registration", "A08": "update"}

CONTEXT = [
    "patient_id", "patient_id_namespace", "visit_id", "account_id", "patient_class", "patient_point_of_care",
    "patient_facility", "msg_type", "msg_control_id", "msg_dt_tm", "msg_sending_facility", "msg_seq",
]  # fmt: skip
LINEAGE = ["raw_ts", "bronze_ts", "raw_partition", "raw_offset"]
_ORDER = ["order_cd", "order_cd_desc", "order_cd_system", "alt_order_cd", "alt_order_cd_desc", "alt_order_cd_system"]
_TIMESTAMPS = {"admit_dt_tm", "date_of_birth_dt", "msg_dt_tm", "observation_dt_tm", "diagnosis_dt_tm", "raw_ts",
               "bronze_ts"}  # fmt: skip
_TYPES = {"position": IntegerType(), "set_id": IntegerType(), "value_num": DoubleType(),
          "raw_partition": IntegerType(), "raw_offset": LongType()}  # fmt: skip


def _schema(names: list[str]) -> StructType:
    return StructType(
        [StructField(n, _TYPES.get(n, TimestampType() if n in _TIMESTAMPS else StringType())) for n in names]
    )


MESSAGE_SCHEMA = _schema(
    [*CONTEXT, "patient_alt_id", "patient_alt_id_namespace", "date_of_birth_dt", "sex", "race", "ethnicity",
     "admit_dt_tm", "document_completion_status", "payload", *LINEAGE]
)  # fmt: skip
ENCOUNTER_SCHEMA = _schema(["event", "event_name", *CONTEXT, "admit_dt_tm", *LINEAGE])
OBSERVATION_SCHEMA = _schema(
    [*CONTEXT, "order_cd", "order_cd_desc", "position", "set_id", "value_type", "observation_cd",
     "observation_cd_desc", "observation_cd_system", "alt_observation_cd", "alt_observation_cd_desc",
     "alt_observation_cd_system", "value", "value_num", "units", "reference_range", "abnormal_flag", "result_status",
     "observation_dt_tm", *LINEAGE]
)  # fmt: skip
ORDER_SCHEMA = _schema([*CONTEXT, *_ORDER, *LINEAGE])
DIAGNOSIS_SCHEMA = _schema(
    [*CONTEXT, "position", "set_id", "diagnosis_cd", "diagnosis_cd_desc", "diagnosis_cd_system", "diagnosis_type",
     "diagnosis_dt_tm", *LINEAGE]
)  # fmt: skip
# table -> schema, for the mirror and the subscription hub
SCHEMAS = {
    "silver_messages": MESSAGE_SCHEMA,
    "silver_encounters": ENCOUNTER_SCHEMA,
    "silver_observations": OBSERVATION_SCHEMA,
    "silver_orders": ORDER_SCHEMA,
    "silver_diagnoses": DIAGNOSIS_SCHEMA,
    "quarantine_hl7": hl7v2.QUARANTINE_SCHEMA,
}


def _record(topic: str, schema: StructType, cols: dict[str, Column]) -> Column:
    """(topic, json) for one row; columns follow the schema's order. Nulls are written explicitly so every record has
    the full, stable shape subscribers code against."""
    value = F.struct(*[cols[f.name].cast(f.dataType).alias(f.name) for f in schema])
    return F.struct(F.lit(topic).alias("topic"), F.to_json(value, {"ignoreNullFields": "false"}).alias("value"))


def _only_if(cond: Column, record: Column) -> Column:
    """[record] when cond holds, else [] (typed, so it can be concatenated)."""
    return F.filter(F.array(record), lambda _: F.coalesce(cond, F.lit(False)))


def _each(items: Column, record) -> Column:
    """record(item, position) for every element of an array (position 1-based)."""
    return F.transform(items, lambda item, i: record(item, i + 1))


def to_silver_records(bronze: DataFrame, messages: str, encounters: str, observations: str, orders: str,
                      diagnoses: str, quarantine: str) -> DataFrame:  # fmt: skip
    """Kafka records from the bronze topic -> (key, value, topic), one per entity row (or one quarantine row). The key
    (patient) is kept so each patient's records stay ordered within a partition."""
    b = bronze.select(F.col("key"), F.from_json(F.col("value").cast("string"), hl7v2.BRONZE_SCHEMA).alias("b"),
                      F.col("timestamp").alias("bronze_ts"))  # fmt: skip
    p = hl7v2.parse(b, F.col("b.hl7"))
    m = F.col("message")
    col = {n: m[n] for n in hl7v2.MESSAGE_FIELDS} | {
        "payload": F.col("b.hl7"),
        "raw_ts": F.col("b.raw_ts"),
        "bronze_ts": F.col("bronze_ts"),
        "raw_partition": F.col("b.raw_partition"),
        "raw_offset": F.col("b.raw_offset"),
    }
    family = F.substring_index(col["msg_type"], "^", 1)
    event = F.substring_index(col["msg_type"], "^", -1)
    event_names = F.create_map(*[F.lit(x) for kv in ADT_EVENTS.items() for x in kv])

    def observation(o: Column, position: Column) -> Column:
        return _record(observations, OBSERVATION_SCHEMA, col | {n: o[n] for n in hl7v2.OBSERVATION_SCHEMA.fieldNames()}
                       | {"position": position, "set_id": o["set_id"].try_cast("int"),
                          "value_num": F.when(o["value_type"] == "NM", o["value"].try_cast("double"))})  # fmt: skip

    def diagnosis(d: Column, position: Column) -> Column:
        return _record(diagnoses, DIAGNOSIS_SCHEMA, col | {n: d[n] for n in hl7v2.DIAGNOSIS_SCHEMA.fieldNames()}
                       | {"position": position, "set_id": d["set_id"].try_cast("int")})  # fmt: skip

    entities = F.concat(
        F.array(_record(messages, MESSAGE_SCHEMA, col)),
        _only_if(family == "ADT", _record(encounters, ENCOUNTER_SCHEMA, col | {
            "event": event, "event_name": F.try_element_at(event_names, event)})),
        _each(m["observations"], observation),
        _only_if(family == "ORM", _record(orders, ORDER_SCHEMA, col)),
        _each(m["diagnoses"], diagnosis),
    )  # fmt: skip
    held = F.array(
        _record(quarantine, hl7v2.QUARANTINE_SCHEMA, col | {"reason": F.col("reason"), "hl7": col["payload"]})
    )
    out = F.when(F.col("reason").isNull(), entities).otherwise(held)
    return p.select(F.col("key"), F.explode(out).alias("r")).select("key", "r.value", "r.topic")
