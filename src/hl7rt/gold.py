"""Gold: break each silver message into the entities downstream teams subscribe to, one topic per entity.

    ADT^*      -> 1 encounter    (gold_encounters topic)    event A01/A03/A04/A08 -> admission/discharge/...
    any OBX    -> 1 observation per OBX (gold_observations topic; in practice ORU^R01), numeric value_num
    ORM^*      -> 1 order        (gold_orders topic)
    other      -> nothing (e.g. MDM documents stay in silver)

One RTM flow writes all three topics: each silver record becomes an array of (topic, value) records that
is exploded, and the Kafka sink routes by the `topic` column. Every entity carries raw_ts / bronze_ts /
silver_ts (broker timestamps) so the lakehouse mirror can measure each hop, plus its raw-topic coordinates.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from hl7rt import hl7v2

ADT_EVENTS = {"A01": "admission", "A03": "discharge", "A04": "registration", "A08": "update"}

# Broker timestamps (latency) and the raw-topic coordinates (the upsert key for replays; see lakehouse.KEYS).
_LINEAGE = ["raw_ts", "bronze_ts", "silver_ts", "raw_partition", "raw_offset"]
_PATIENT = ["patient_id", "visit_id", "patient_facility"]
_MESSAGE = ["msg_type", "msg_control_id", "msg_dt_tm", "msg_sending_facility"]
_TIMESTAMPS = {"admit_dt_tm", "msg_dt_tm", "observation_dt_tm", *_LINEAGE}
_TYPES = {"set_id": IntegerType(), "value_num": DoubleType(), "raw_partition": IntegerType(), "raw_offset": LongType()}


def _schema(names: list[str]) -> StructType:
    return StructType(
        [StructField(n, _TYPES.get(n, TimestampType() if n in _TIMESTAMPS else StringType())) for n in names]
    )


ENCOUNTER_SCHEMA = _schema(
    ["event", "event_name", *_PATIENT, "account_id", "patient_class", "patient_point_of_care", "admit_dt_tm",
     *_MESSAGE, *_LINEAGE]
)  # fmt: skip
OBSERVATION_SCHEMA = _schema(
    [*_PATIENT, "order_cd", "order_cd_desc", "set_id", "value_type", "observation_cd", "observation_cd_desc",
     "observation_cd_system", "value", "value_num", "units", "reference_range", "abnormal_flag", "result_status",
     "observation_dt_tm", *_MESSAGE, *_LINEAGE]
)  # fmt: skip
ORDER_SCHEMA = _schema(
    [*_PATIENT, "order_cd", "order_cd_desc", "order_cd_system", "alt_order_cd", "alt_order_cd_desc",
     "alt_order_cd_system", *_MESSAGE, *_LINEAGE]
)  # fmt: skip


def _record(topic: str, schema: StructType, cols: dict[str, Column]) -> Column:
    """(topic, json) for one entity; columns follow the schema's order. Nulls are written explicitly so every
    gold record has the full, stable shape subscribers code against."""
    value = F.struct(*[cols[f.name].cast(f.dataType).alias(f.name) for f in schema])
    return F.struct(F.lit(topic).alias("topic"), F.to_json(value, {"ignoreNullFields": "false"}).alias("value"))


def _only_if(cond: Column, record: Column) -> Column:
    """[record] when cond holds, else [] (typed, so it can be concatenated)."""
    return F.filter(F.array(record), lambda _: F.coalesce(cond, F.lit(False)))


def to_gold_records(silver: DataFrame, encounters: str, observations: str, orders: str) -> DataFrame:
    """Kafka records from the silver topic -> (key, value, topic) for the three gold topics. The key (patient)
    is kept so each patient's records stay ordered within a partition."""
    s = silver.select(
        F.col("key"),
        F.from_json(F.col("value").cast("string"), hl7v2.SILVER_SCHEMA).alias("s"),
        F.col("timestamp").alias("silver_ts"),
    )
    col = {n: F.col(f"s.{n}") for n in hl7v2.SILVER_SCHEMA.fieldNames()} | {"silver_ts": F.col("silver_ts")}
    family = F.substring_index(col["msg_type"], "^", 1)
    event = F.substring_index(col["msg_type"], "^", -1)
    event_names = F.create_map(*[F.lit(x) for kv in ADT_EVENTS.items() for x in kv])

    encounter = _record(
        encounters,
        ENCOUNTER_SCHEMA,
        col | {"event": event, "event_name": F.try_element_at(event_names, event)},
    )
    order = _record(orders, ORDER_SCHEMA, col)

    def observation(o: Column) -> Column:
        return _record(
            observations,
            OBSERVATION_SCHEMA,
            col
            | {n: o[n] for n in hl7v2.OBSERVATION_SCHEMA.fieldNames()}
            | {"set_id": o["set_id"].try_cast("int"),
               "value_num": F.when(o["value_type"] == "NM", o["value"].try_cast("double"))},
        )  # fmt: skip

    out = F.concat(
        _only_if(family == "ADT", encounter),
        F.transform(col["observations"], observation),
        _only_if(family == "ORM", order),
    )
    return s.select(F.col("key"), F.explode(out).alias("g")).select("key", "g.value", "g.topic")
