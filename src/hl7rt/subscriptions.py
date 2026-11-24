"""Subscription hub matching: every app's SQL rule, applied to a micro-batch of one silver topic, in one pass.

A rule is Databricks SQL (`SELECT <fields> FROM <source> WHERE <condition>`), compiled by Databricks against the app's
own views when it was registered (src/subscription_api). The hub evaluates each stored condition per record with Spark,
so the same SQL in the SQL editor selects the same records. Before it does, it re-checks each condition
(subscription_api.rules.check_condition): one filter, no functions that reach outside the record, granted columns only.

All of a source's rules run in one projection per batch: an array of the matching rules per record, then explode. That
is one scan and one plan however many rules there are, in chunks small enough to stay under the JVM's 64 KB method
limit. A rule that fails at runtime (a cast of bad data, say) is found and reported FAILED alone; the others keep
running.

Each match becomes a notification on the app's own topic, carrying only the fields its rule selects.
"""

import time
from dataclasses import dataclass, replace

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from hl7rt import silver
from subscription_api.rules import RuleError, check_condition, clauses, identifier, literal

SOURCES: dict[str, StructType] = {
    "silver_encounters": silver.ENCOUNTER_SCHEMA,
    "silver_observations": silver.OBSERVATION_SCHEMA,
    "silver_orders": silver.ORDER_SCHEMA,
    "silver_diagnoses": silver.DIAGNOSIS_SCHEMA,
}
CHUNK = 200  # rules per projection


@dataclass(frozen=True)
class Rule:
    """One enabled subscription revision, as the hub runs it."""

    subscription_id: str
    name: str
    revision: int
    source: str
    where: str | None
    fields: list[str]
    topic: str
    granted: frozenset[str]


_CHECKED: dict[tuple, tuple[str | None, str | None]] = {}  # rule definition + grant -> (error, canonical WHERE)


def _check(r: Rule) -> tuple[str | None, str | None]:
    """The hub's own check of one rule, cached by revision (a rule set changes a rule at a time): (error, the WHERE
    regenerated from its parsed tree, which is what the hub evaluates)."""
    key = (r.subscription_id, r.revision, r.where, tuple(r.fields), r.granted)
    if key not in _CHECKED:
        if len(_CHECKED) > 20_000:  # bounded: old revisions drop out
            _CHECKED.clear()
        try:
            if not set(r.fields) <= r.granted:
                raise RuleError("FIELDS_DENIED", f"{sorted(set(r.fields) - r.granted)} not granted to this app")
            _CHECKED[key] = (None, check_condition(r.where, set(r.granted)))
        except RuleError as e:
            _CHECKED[key] = (str(e), None)
    return _CHECKED[key]


def runnable(rules: list[Rule]) -> tuple[list[Rule], dict[str, str]]:
    """The rules that pass the hub's own check (with their canonical WHERE), and the error for each that doesn't
    (subscription id -> message)."""
    good, failed = [], {}
    for r in rules:
        error, where = _check(r)
        if error:
            failed[r.subscription_id] = error
        else:
            good.append(replace(r, where=where))
    return good, failed


def events(topic_records: DataFrame, source: str) -> DataFrame:
    """A micro-batch of `source`'s topic as records, with the coordinates notifications carry."""
    return topic_records.select(
        F.from_json(F.col("value").cast("string"), SOURCES[source]).alias("e"),
        F.col("partition").alias("_partition"), F.col("offset").alias("_offset"),
        F.col("timestamp").alias("_source_ts"),
    ).select("e.*", "_partition", "_offset", "_source_ts")  # fmt: skip


def _condition(r: Rule):
    return F.expr(r.where) if r.where else F.lit(True)


@dataclass(frozen=True)
class Compiled:
    """A source's rules, checked and prepared once per registry change (not once per batch): simple rules as data (one
    fixed plan however many there are), the rest as SQL expressions in chunks."""

    generation: int | None
    rules: list[Rule]
    failed: dict[str, str]  # subscription id -> why the hub won't run it
    chunks: list[tuple[Column, Column]]  # expression rules, per chunk: (matching rules per record, their metadata)
    table: DataFrame | None = None  # indexed data rules: a row per (rule, key value of its string = or IN clause)
    scan: DataFrame | None = None  # data rules with no string = or IN clause: compared with every record
    data_rules: int = 0
    compile_ms: int = 0


_KIND = {"StringType": "string", "IntegerType": "number", "LongType": "number", "DoubleType": "number",
         "FloatType": "number", "ShortType": "number", "DecimalType": "number"}  # fmt: skip
_CLAUSE = "struct<col: string, op: string, str: string, num: double, strs: array<string>, nums: array<double>>"
_TABLE = (
    f"i int, topic string, sid string, name string, rev int, fields array<string>, clauses array<{_CLAUSE}>, "
    "key_col string, key_val string"
)
# Every clause of a data rule must hold. A NULL comparison is unknown, so the rule doesn't match: SQL's AND semantics.
_ALL_HOLD = """forall(clauses, c -> CASE c.op
  WHEN 'eq_s' THEN element_at(_s, c.col) = c.str  WHEN 'ne_s' THEN element_at(_s, c.col) <> c.str
  WHEN 'lt_s' THEN element_at(_s, c.col) < c.str  WHEN 'le_s' THEN element_at(_s, c.col) <= c.str
  WHEN 'gt_s' THEN element_at(_s, c.col) > c.str  WHEN 'ge_s' THEN element_at(_s, c.col) >= c.str
  WHEN 'eq_n' THEN element_at(_n, c.col) = c.num  WHEN 'ne_n' THEN element_at(_n, c.col) <> c.num
  WHEN 'lt_n' THEN element_at(_n, c.col) < c.num  WHEN 'le_n' THEN element_at(_n, c.col) <= c.num
  WHEN 'gt_n' THEN element_at(_n, c.col) > c.num  WHEN 'ge_n' THEN element_at(_n, c.col) >= c.num
  WHEN 'in_s' THEN array_contains(c.strs, element_at(_s, c.col))
  WHEN 'nin_s' THEN NOT array_contains(c.strs, element_at(_s, c.col))
  WHEN 'in_n' THEN array_contains(c.nums, element_at(_n, c.col))
  WHEN 'nin_n' THEN NOT array_contains(c.nums, element_at(_n, c.col))
  WHEN 'null' THEN element_at(_a, c.col) IS NULL  WHEN 'not_null' THEN element_at(_a, c.col) IS NOT NULL
END)"""


def kinds(source: str) -> dict[str, str]:
    """Column -> 'string' or 'number' for the columns a data rule may compare (others keep the expression path)."""
    return {f.name: _KIND[type(f.dataType).__name__] for f in SOURCES[source] if type(f.dataType).__name__ in _KIND}


def _chunk_sql(chunk: list[Rule]) -> tuple[str, str]:
    """One chunk of rules as two SQL expressions: the matching rules per record, and their metadata. One expression per
    chunk, not a dozen Column calls per rule: each Column call is a round trip to the driver, and building 1,000 rules
    that way took about 70 s. Built only from checked parts: the canonical WHERE (one expression, from its parsed tree),
    granted field names as quoted identifiers, and escaped literals."""
    hits = ", ".join(
        f"CASE WHEN ({r.where or 'true'}) THEN named_struct('i', {i}, 'payload', to_json(named_struct("
        + ", ".join(f"{literal(f)}, {identifier(f)}" for f in r.fields)
        + "), map('ignoreNullFields', 'false'))) END"
        for i, r in enumerate(chunk)
    )
    meta = ", ".join(
        f"named_struct('topic', {literal(r.topic)}, 'sid', {literal(r.subscription_id)}, 'name', {literal(r.name)}, "
        f"'rev', {int(r.revision)})"
        for r in chunk
    )
    return f"array_compact(array({hits}))", f"array({meta})"


def compile_rules(rules: list[Rule], generation: int | None = None, chunk: int | None = None, spark=None) -> Compiled:
    """Check and prepare a source's rules. With a SparkSession, simple rules become data (the fast path); without one,
    every rule is an expression."""
    chunk = chunk or CHUNK
    started = time.monotonic()
    good, failed = runnable(rules)
    table, data, expressions = None, [], []
    for r in good:
        cl = clauses(r.where, kinds(r.source)) if spark is not None else None
        (data if cl is not None else expressions).append((r, cl))
    scan = None
    if data:
        indexed, unindexed = [], []
        for i, (r, cl) in enumerate(data):
            row = (i, r.topic, r.subscription_id, r.name, int(r.revision), list(r.fields),
                   [{"col": c["col"], "op": c["op"], "str": c.get("str"), "num": c.get("num"), "strs": c.get("strs"),
                     "nums": c.get("nums")} for c in cl])  # fmt: skip
            key = next((c for c in cl if c["op"] in ("eq_s", "in_s")), None)
            if key is None:
                unindexed.append(row + (None, None))
            else:  # one row per key value: a record only meets the rules that name one of its values
                for value in sorted(set([key["str"]] if key["op"] == "eq_s" else key["strs"])):
                    indexed.append(row + (key["col"], value))
        table = spark.createDataFrame(indexed, _TABLE) if indexed else None
        scan = spark.createDataFrame(unindexed, _TABLE) if unindexed else None
    chunks = []
    exprs = [r for r, _ in expressions]
    for start in range(0, len(exprs), chunk):
        hits, meta = _chunk_sql(exprs[start : start + chunk])
        chunks.append((F.expr(hits), F.expr(meta)))
    elapsed = int((time.monotonic() - started) * 1000)
    return Compiled(generation, good, failed, chunks, table, scan, len(data), elapsed)


def _envelope(matched: DataFrame, source: str) -> DataFrame:
    """(topic, key, value) from matches with topic, sid, name, rev, payload (a JSON string), and the source record's
    coordinates."""
    return matched.select(
        F.col("topic"),
        F.col("patient_id").alias("key"),
        F.to_json(
            F.struct(
                # Stable across redeliveries: the subscription revision plus the source record's coordinates.
                F.sha2(F.concat_ws(":", "sid", "rev", "_partition", "_offset"), 256).alias("notificationId"),
                F.col("sid").alias("subscriptionId"),
                F.col("name").alias("subscription"),
                F.col("rev").alias("revision"),
                F.lit(source).alias("source"),
                F.col("_partition").alias("sourcePartition"),
                F.col("_offset").alias("sourceOffset"),
                F.col("_source_ts").alias("source_ts"),
                F.parse_json("payload").alias("payload"),
            )
        ).alias("value"),
    )


def _data_matches(records: DataFrame, source: str, table: DataFrame | None, scan: DataFrame | None) -> DataFrame:
    """Data rules, in one fixed plan. Indexed rules: each record's string values join the rules that name them (a
    broadcast equi-join), so a record meets only the few rules that could match it. The rest meet every record. Either
    way, every clause of a candidate rule must then hold."""
    cols = [f.name for f in SOURCES[source]]
    k = kinds(source)

    def pairs(names, value):
        return ", ".join(f"{literal(c)}, {value(identifier(c))}" for c in names)

    strings = [c for c in cols if k.get(c) == "string"]
    numbers = [c for c in cols if k.get(c) == "number"]

    # Per column, its JSON member as to_json writes it ("name":value): a payload reads the same on both paths.
    def fragment(c):
        js = f"to_json(named_struct({literal(c)}, {identifier(c)}), map('ignoreNullFields', 'false'))"
        return f"substring({js}, 2, char_length({js}) - 2)"

    def map_of(names, value):
        return F.expr(f"map({pairs(names, value)})" if names else "map()")

    with_maps = records.select(
        "*",
        map_of(strings, lambda c: c).alias("_s"),
        map_of(numbers, lambda c: f"CAST({c} AS DOUBLE)").alias("_n"),
        map_of(cols, lambda c: f"CAST({c} AS STRING)").alias("_a"),
        F.expr("map(" + ", ".join(f"{literal(c)}, {fragment(c)}" for c in cols) + ")").alias("_j"),
    )
    candidates = []
    if table is not None:
        keyed = with_maps.select("*", F.explode("_s").alias("_k", "_v"))
        t = F.broadcast(table)
        candidates.append(keyed.join(t, (keyed["_k"] == t["key_col"]) & (keyed["_v"] == t["key_val"])).drop("_k", "_v"))
    if scan is not None:
        candidates.append(with_maps.crossJoin(F.broadcast(scan)))
    out = None
    for c in candidates:
        matched = c.where(F.expr(_ALL_HOLD)).select(
            "topic", "sid", "name", "rev", "patient_id", "_partition", "_offset", "_source_ts",
            F.expr("concat('{', array_join(transform(fields, f -> element_at(_j, f)), ','), '}')").alias("payload"),
        )  # fmt: skip
        out = matched if out is None else out.unionByName(matched)
    return out


def notifications(records: DataFrame, source: str, rules: "list[Rule] | Compiled") -> DataFrame | None:
    """Kafka records (topic, key, value) for every rule's matches in a batch of `events(...)`, or None."""
    compiled = rules if isinstance(rules, Compiled) else compile_rules(rules)
    parts = []
    if compiled.table is not None or compiled.scan is not None:
        parts.append(_data_matches(records, source, compiled.table, compiled.scan))
    for hits, metas in compiled.chunks:
        meta = F.element_at(metas, F.col("h.i") + 1)
        parts.append(
            records.select("patient_id", "_partition", "_offset", "_source_ts", F.explode(hits).alias("h")).select(
                meta["topic"].alias("topic"), meta["sid"].alias("sid"), meta["name"].alias("name"),
                meta["rev"].alias("rev"), "patient_id", "_partition", "_offset", "_source_ts",
                F.col("h.payload").alias("payload"),
            )
        )  # fmt: skip
    if not parts:
        return None
    matched = parts[0]
    for p in parts[1:]:
        matched = matched.unionByName(p)
    return _envelope(matched, source)


def failing(records: DataFrame, rules: list[Rule]) -> dict[str, str]:
    """After a batch failed: which rules fail on this batch's records, with Spark's error (subscription id -> message).
    Evaluates each rule over every record, so only when a batch failed."""
    errors = {}
    for r in rules:
        try:
            records.select(F.count(F.when(_condition(r), 1))).collect()
        except Exception as e:  # noqa: BLE001 - Spark's error is the answer
            errors[r.subscription_id] = str(e).strip().splitlines()[0][:1000]
    return errors
