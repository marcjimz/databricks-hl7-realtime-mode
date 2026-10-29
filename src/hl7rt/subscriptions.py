"""Self-service subscriptions: a rule over gold topics -> records for <catalog>.<schema>.sub_<name>.

Two ways to write a rule, both in src/subscriptions/:
- <name>.sql: plain SQL over gold_encounters / gold_observations / gold_orders, settings in a `-- key: value`
  header. Each gold view carries `_meta` (record id + broker timestamps); the query selects it (for a join, each
  side's, as `_meta_<side>`), and the platform turns it into record_id / gold_ts / raw_ts. `_template.sql`.
- <name>.yml: `when` (stateless filter), optionally `then` + `within` (a time-bounded join). `_template.yml`.
The same query runs in Real-Time Mode or micro-batch.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from hl7rt import gold

SOURCES = {
    "gold_encounters": gold.ENCOUNTER_SCHEMA,
    "gold_observations": gold.OBSERVATION_SCHEMA,
    "gold_orders": gold.ORDER_SCHEMA,
}
MODES = ("rtm", "microbatch")
DESTINATIONS = ("eventhubs", "delta")
TIME = "msg_dt_tm"  # event time used for `within` and watermarks
_INTERVAL = re.compile(r"\d+ (second|minute|hour)s?")
_JOIN = re.compile(r"\bJOIN\b", re.IGNORECASE)
_SQL_HEADER = re.compile(r"^--\s*(name|mode|key|destinations|lateness)\s*:\s*(.+?)\s*$")
META = "_meta"  # lineage struct on every gold view: record_id, gold_ts, raw_ts

# Spark settings Real-Time Mode requires for stream-stream joins (docs: structured-streaming/real-time/setup).
# They must be pipeline configuration (read at startup), not flow spark_conf; tests/test_topic_names.py checks it.
RTM_JOIN_CONF = {
    "spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled": "true",
    "spark.sql.streaming.join.stateFormatVersion": "4",
    "spark.sql.streaming.join.stateFormatV4.enabled": "true",
    "spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion": "2",
    "spark.sql.streaming.realTimeMode.controlMessage.enabled": "true",
}


@dataclass(frozen=True)
class Event:
    source: str
    filter: str = "true"


@dataclass(frozen=True)
class Rule:
    name: str
    mode: str
    key: str
    when: Event
    output: tuple[str, ...]
    then: Event | None = None
    within: str | None = None
    lateness: str = "5 minutes"
    destinations: tuple[str, ...] = DESTINATIONS
    sql: str | None = None

    @property
    def table(self) -> str:
        return f"sub_{self.name}"

    @property
    def is_join(self) -> bool:
        return self.then is not None or bool(self.sql and _JOIN.search(self.sql))

    @property
    def sources(self) -> tuple[str, ...]:
        """Gold tables the rule reads (a SQL rule only reads the ones it names)."""
        if self.sql:
            return tuple(s for s in SOURCES if re.search(rf"\b{s}\b", self.sql))
        return (self.when.source,) + ((self.then.source,) if self.then else ())


def _event(spec: dict, where: str) -> Event:
    if not isinstance(spec, dict) or spec.get("source") not in SOURCES:
        raise ValueError(f"{where}.source must be one of {sorted(SOURCES)}")
    return Event(spec["source"], spec.get("filter", "true"))


def parse(spec: dict) -> Rule:
    """Validate a rule dict (as read from YAML)."""
    if spec.get("mode") not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    when = _event(spec.get("when"), "when")
    then = _event(spec["then"], "then") if spec.get("then") else None
    within = spec.get("within")
    if then and not (isinstance(within, str) and _INTERVAL.fullmatch(within)):
        raise ValueError("a `then` event needs `within`, e.g. '60 minutes'")
    destinations = tuple(spec.get("destinations", DESTINATIONS))
    if not set(destinations) <= set(DESTINATIONS):
        raise ValueError(f"destinations must be within {DESTINATIONS}")
    if spec["mode"] == "rtm" and "eventhubs" not in destinations:
        raise ValueError("mode rtm writes Event Hubs (Delta lands via the mirror); add `eventhubs` to destinations")
    rule = Rule(
        name=spec["name"], mode=spec["mode"], key=spec["key"], when=when, output=tuple(spec["output"]), then=then,
        within=within, lateness=spec.get("lateness", "5 minutes"), destinations=destinations,
    )  # fmt: skip
    for col in [*rule.output, f"when.{rule.key}"] + ([f"then.{rule.key}"] if then else []):
        _field(rule, col)
    return rule


def parse_sql(text: str) -> Rule:
    """Validate a SQL rule: `-- key: value` header lines (name, mode, key, destinations, lateness) + one query."""
    spec, body = {}, []
    for line in text.splitlines():
        m = _SQL_HEADER.match(line.strip())
        if m:
            spec[m.group(1)] = m.group(2)
        elif not line.strip().startswith("--"):
            body.append(line)
    sql = "\n".join(body).strip().rstrip(";")
    if spec.get("mode") not in MODES:
        raise ValueError(f"-- mode: must be one of {MODES}")
    if not spec.get("key"):
        raise ValueError("-- key: is required (the column records are keyed by, e.g. patient_id)")
    destinations = tuple(d.strip() for d in spec.get("destinations", ",".join(DESTINATIONS)).split(","))
    if not set(destinations) <= set(DESTINATIONS):
        raise ValueError(f"-- destinations: must be within {DESTINATIONS}")
    rule = Rule(name=spec.get("name", ""), mode=spec["mode"], key=spec["key"], when=Event(next(iter(SOURCES))),
                output=(), lateness=spec.get("lateness", "5 minutes"), destinations=destinations, sql=sql)  # fmt: skip
    if not rule.sources:
        raise ValueError(f"the query must read at least one of {sorted(SOURCES)} (gold_ tables)")
    if not re.search(rf"\b{META}\w*\b", sql):
        raise ValueError(f"the query must SELECT {META} (for a join, each side's, e.g. e.{META} AS {META}_e)")
    return rule


def load(path: str | Path) -> Rule:
    path = Path(path)
    if path.suffix == ".sql":
        try:
            rule = parse_sql(path.read_text())
        except ValueError as e:
            raise ValueError(f"{path}: {e}") from e
        if rule.name != path.stem:
            raise ValueError(f"{path}: -- name: {rule.name!r} must match the file name {path.stem!r}")
        return rule
    spec = yaml.safe_load(path.read_text())
    if spec.get("name") != path.stem:
        raise ValueError(f"{path}: name {spec.get('name')!r} must match the file name {path.stem!r}")
    try:
        return parse(spec)
    except (KeyError, ValueError) as e:
        raise ValueError(f"{path}: {e}") from e


def _field(rule: Rule, col: str) -> tuple[str, StructField]:
    """(side, field) for an output column. Unqualified names resolve to `then`, else `when`."""
    side, _, name = col.rpartition(".")
    sides = [side] if side else (["then", "when"] if rule.is_join else ["when"])
    for s in sides:
        event = rule.then if s == "then" else rule.when
        if event and name in SOURCES[event.source].fieldNames():
            return s, SOURCES[event.source][name]
    raise ValueError(f"output column {col!r} isn't in {[getattr(rule, s).source for s in sides if getattr(rule, s)]}")


def _alias(col: str) -> str:
    return col.replace(".", "_")


_JOIN_FIELDS = [("when_msg_control_id", None), ("then_msg_control_id", None), ("when_dt_tm", TimestampType()),
                ("then_dt_tm", TimestampType()), ("matched_after_ms", DoubleType())]  # fmt: skip
_LINEAGE = [("gold_ts", TimestampType()), ("raw_ts", TimestampType())]


def output_schema(rule: Rule, spark=None) -> StructType:
    """Schema of a subscription record (for the lakehouse mirror). A SQL rule needs `spark` to analyze its query."""
    if rule.sql:
        empty = {s: spark.createDataFrame([], _view_schema(s)) for s in rule.sources}
        cols = _sql(rule, empty).schema
        values = [f for f in cols if not f.name.startswith(META)]
        return StructType([StructField("record_id", StringType()), *values, *[StructField(n, t) for n, t in _LINEAGE]])
    fields = [StructField("record_id", StringType())]
    fields += [StructField(_alias(c), _field(rule, c)[1].dataType) for c in rule.output]
    extra = (_JOIN_FIELDS if rule.is_join else []) + _LINEAGE
    return StructType(fields + [StructField(n, t or SOURCES[rule.when.source]["msg_control_id"].dataType)
                                for n, t in extra])  # fmt: skip


def _events(topic: DataFrame, source: str) -> DataFrame:
    e = topic.select(F.from_json(F.col("value").cast("string"), SOURCES[source]).alias("e"),
                     F.col("timestamp").alias("gold_ts"))  # fmt: skip
    return e.select("e.*", "gold_ts")


def _ms(later: Column, earlier: Column) -> Column:
    return (F.unix_micros(later) - F.unix_micros(earlier)) / 1000.0


def _record_id(event: Event, side: str | None = None) -> Column:
    """Raw-topic coordinates of the event (+ OBX set id): identical when an at-least-once hop re-sends it."""
    col = (lambda c: F.col(f"{side}.{c}")) if side else F.col
    parts = [col("raw_partition"), col("raw_offset")] + ([col("set_id")] if event.source == "gold_observations" else [])
    return F.concat_ws("-", *parts)


def _view_schema(source: str) -> StructType:
    meta = StructType([StructField("record_id", StringType()), *[StructField(n, t) for n, t in _LINEAGE]])
    return StructType([*SOURCES[source], StructField("gold_ts", TimestampType()), StructField(META, meta)])


def _with_meta(events: DataFrame, source: str) -> DataFrame:
    meta = F.struct(_record_id(Event(source)).alias("record_id"), F.col("gold_ts"), F.col("raw_ts"))
    return events.withColumn(META, meta)


def _sql(rule: Rule, views: dict[str, DataFrame]) -> DataFrame:
    """Run the rule's SQL with each gold table bound to its view (spark.sql DataFrame parameters)."""
    escaped = rule.sql.replace("{", "{{").replace("}", "}}")
    query = re.sub(rf"\b({'|'.join(views)})\b", r"{\1}", escaped)
    spark = next(iter(views.values())).sparkSession
    return spark.sql(query, **views)


def _build_sql(rule: Rule, read) -> DataFrame:
    views = {}
    for source in rule.sources:
        v = _with_meta(_events(read(source), source), source)
        views[source] = v.withWatermark(TIME, rule.lateness) if rule.is_join else v
    out = _sql(rule, views)
    metas = [c for c in out.columns if c.startswith(META)]
    if not metas:
        raise ValueError(f"{rule.name}: the query must SELECT {META}")
    if rule.key not in out.columns:
        raise ValueError(f"{rule.name}: key column {rule.key!r} must be in the SELECT")
    latest = (
        (lambda f: F.greatest(*[F.col(f"{m}.{f}") for m in metas]))
        if len(metas) > 1
        else (lambda f: F.col(f"{metas[0]}.{f}"))
    )
    value = F.struct(
        F.concat_ws("|", *[F.col(f"{m}.record_id") for m in metas]).alias("record_id"),
        *[F.col(c) for c in out.columns if not c.startswith(META)],
        latest("gold_ts").alias("gold_ts"),
        latest("raw_ts").alias("raw_ts"),
    )
    return out.select(F.col(rule.key).alias("key"), F.to_json(value).alias("value"))


def build(rule: Rule, read) -> DataFrame:
    """(key, value) records for the subscription topic. `read(table)` returns that gold topic's Kafka stream."""
    if rule.sql:
        return _build_sql(rule, read)
    when = _events(read(rule.when.source), rule.when.source).where(F.expr(rule.when.filter))
    if not rule.is_join:
        out = when.select(_record_id(rule.when).alias("record_id"), *[F.col(c).alias(_alias(c)) for c in rule.output],
                          "gold_ts", "raw_ts")  # fmt: skip
        return out.select(F.col(_alias(rule.key)).alias("key"), F.to_json(F.struct("*")).alias("value"))

    then = _events(read(rule.then.source), rule.then.source).where(F.expr(rule.then.filter))
    a = when.withWatermark(TIME, rule.lateness).alias("when")
    b = then.withWatermark(TIME, rule.lateness).alias("then")
    at, bt = F.col(f"when.{TIME}"), F.col(f"then.{TIME}")
    joined = a.join(
        b,
        (F.col(f"when.{rule.key}") == F.col(f"then.{rule.key}"))
        & (bt >= at)
        & (bt <= at + F.expr(f"INTERVAL {rule.within}")),
        "inner",
    )
    cols = [F.col(f"{_field(rule, c)[0]}.{c.rpartition('.')[2]}").alias(_alias(c)) for c in rule.output]
    out = joined.select(
        F.col(f"when.{rule.key}").alias("_key"),
        F.concat_ws("|", _record_id(rule.when, "when"), _record_id(rule.then, "then")).alias("record_id"),
        *cols,
        F.col("when.msg_control_id").alias("when_msg_control_id"),
        F.col("then.msg_control_id").alias("then_msg_control_id"),
        at.alias("when_dt_tm"),
        bt.alias("then_dt_tm"),
        _ms(bt, at).alias("matched_after_ms"),
        F.greatest("when.gold_ts", "then.gold_ts").alias("gold_ts"),
        F.greatest("when.raw_ts", "then.raw_ts").alias("raw_ts"),
    )
    value = F.struct(*[c for c in out.columns if c != "_key"])
    return out.select(F.col("_key").alias("key"), F.to_json(value).alias("value"))
