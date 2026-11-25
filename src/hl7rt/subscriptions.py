"""Subscription rules as the hub runs them: every app's SQL rule over a micro-batch of one source, a silver topic or a
Delta table the hub was given instead (hl7.subscriptions.tables: add_table_source, table_records).

A rule is Databricks SQL (`SELECT <fields> FROM <source> WHERE <condition>`), compiled by Databricks against the app's
own views when it was registered (src/subscription_api). The hub evaluates each stored condition per record, so the
same SQL in the SQL editor selects the same records. Before it does, it re-checks each condition
(subscription_api.rules.check_condition): one filter, no functions that reach outside the record, granted columns only.

The hub engine (hl7rt.engine) matches simple rules (an OR of AND groups of column-literal comparisons: _groups) on the
driver (hl7rt.matching) and the rest as Spark expressions, a chunk of rules per expression (matches_sql), in the same
read of the batch. This module also keeps the expression path whole (compile_rules, notifications): every rule as one
projection per chunk, then explode. That is the reference the engine is tested against, byte for byte
(tests/test_matching.py, tests/test_subscriptions.py). A rule that fails at runtime (a cast of bad data, say) is found
and reported FAILED alone (failing); the others keep running.

Each match becomes a notification on the app's own topic, carrying only the fields its rule selects.
"""

import time
from dataclasses import dataclass, replace

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from hl7rt import silver
from subscription_api.rules import RuleError, check_condition, disjuncts, identifier, literal, source_tables

SILVER: dict[str, StructType] = {
    "silver_encounters": silver.ENCOUNTER_SCHEMA,
    "silver_observations": silver.OBSERVATION_SCHEMA,
    "silver_orders": silver.ORDER_SCHEMA,
    "silver_diagnoses": silver.DIAGNOSIS_SCHEMA,
    "silver_documents": silver.DOCUMENT_SCHEMA,
}  # the built-in sources: the silver entity topics
# Every source the hub knows: silver's, and the Delta tables it was given (add_table_source, at startup). Changed in
# place, never rebound: modules that imported it see the same dict.
SOURCES: dict[str, StructType] = dict(SILVER)
TABLES: set[str] = set()  # the sources read from a Delta table: their rows already have their columns
CHUNK = 200  # rules per projection


def table_sources(spec: str) -> dict[str, str]:
    """hl7.subscriptions.tables as source name -> table, named as the API names HL7_SOURCE_TABLES
    (subscription_api.rules.source_tables), so the hub reads what the API lets rules name. ValueError if malformed."""
    return source_tables(spec)


def add_table_source(name: str, schema: StructType):
    """A Delta table as the source `name`, with its table's schema (the hub, at startup): its rows are read as they are
    (table_records gives them their coordinates). It replaces a silver source of the same name."""
    SOURCES[name] = schema
    TABLES.add(name)
    _GROUPS_OF.clear()  # a rule's groups depend on its source's column types (kinds)


def reset_sources():
    """Only the built-in silver sources again (tests)."""
    SOURCES.clear()
    SOURCES.update(SILVER)
    TABLES.clear()
    _GROUPS_OF.clear()


def table_read_options(max_files) -> dict[str, str]:
    """The hub's read of a table source: a new checkpoint starts at the table's latest version (its history isn't
    replayed), commits that change or delete rows (retention deletes, OPTIMIZE) are skipped rather than failing the
    stream (so a corrected row isn't notified again), and a batch is at most `max_files` new files (it's collected
    once)."""
    return {"startingVersion": "latest", "skipChangeCommits": "true", "maxFilesPerTrigger": str(max_files)}


def table_records(rows: DataFrame, partition: str, offset: str, ts: str) -> DataFrame:
    """A table source's rows with the coordinates notifications carry, each a SQL expression over the table's columns
    or Delta's _metadata (hl7.subscriptions.table_partition, table_offset, table_ts): an int partition, a stable 64-bit
    offset unique within it (notification IDs and bindings are built from the pair), and the source timestamp."""
    return rows.select(
        "*", F.expr(partition).cast("int").alias("_partition"), F.expr(offset).cast("long").alias("_offset"),
        F.expr(ts).cast("timestamp").alias("_source_ts"),
    )  # fmt: skip


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


def events(batch: DataFrame, source: str) -> DataFrame:
    """A micro-batch of `source` as records, with the coordinates notifications carry: a topic's records parsed from
    their JSON value (Kafka partition, offset, and broker timestamp), or a table's rows as they are (table_records)."""
    if source in TABLES:  # no JSON to parse: the table's columns, as registered
        columns = [F.col(identifier(f.name)) for f in SOURCES[source]]
        return batch.select(*columns, "_partition", "_offset", "_source_ts")
    return batch.select(
        F.from_json(F.col("value").cast("string"), SOURCES[source]).alias("e"),
        F.col("partition").alias("_partition"), F.col("offset").alias("_offset"),
        F.col("timestamp").alias("_source_ts"),
    ).select("e.*", "_partition", "_offset", "_source_ts")  # fmt: skip


def _condition(r: Rule):
    return F.expr(r.where) if r.where else F.lit(True)


@dataclass(frozen=True)
class Compiled:
    """A source's rules, checked and prepared once (not once per batch): every rule as a SQL expression, in chunks."""

    generation: int | None
    rules: list[Rule]
    failed: dict[str, str]  # subscription id -> why the hub won't run it
    chunks: list[tuple[Column, Column]]  # per chunk: (matching rules per record, their metadata)
    compile_ms: int = 0


_KIND = {"StringType": "string", "IntegerType": "number", "LongType": "number", "DoubleType": "number",
         "FloatType": "number", "ShortType": "number", "DecimalType": "number"}  # fmt: skip


def kinds(source: str) -> dict[str, str]:
    """Column -> 'string' or 'number' for the columns a simple rule may compare (others keep the expression path)."""
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


def matches_sql(chunk: list[Rule], first: int = 0) -> str:
    """One chunk of rules as one SQL expression: the indexes (from `first`) of the rules a record matches. The engine's
    expression path: payloads are rendered once per field for every rule (hl7rt.render), so only the verdict is per
    rule. Built only from the canonical WHERE (one checked expression, from its parsed tree)."""
    return "array_compact(array(" + ", ".join(
        f"CASE WHEN ({r.where or 'true'}) THEN {first + i} END" for i, r in enumerate(chunk)) + "))"  # fmt: skip


def compile_rules(rules: list[Rule], generation: int | None = None, chunk: int | None = None) -> Compiled:
    """Check and prepare a source's rules, every one as an expression (the reference path)."""
    chunk = chunk or CHUNK
    started = time.monotonic()
    good, failed = runnable(rules)
    chunks = []
    for start in range(0, len(good), chunk):
        hits, meta = _chunk_sql(good[start : start + chunk])
        chunks.append((F.expr(hits), F.expr(meta)))
    return Compiled(generation, good, failed, chunks, int((time.monotonic() - started) * 1000))


_GROUPS_OF: dict[tuple[str, str | None], list[list[dict]] | None] = {}  # (source, canonical WHERE) -> its groups


def _groups(r: Rule) -> list[list[dict]] | None:
    """A rule's AND groups (rules.disjuncts), cached by its WHERE: a registry change recompiles one rule, not all."""
    key = (r.source, r.where)
    if key not in _GROUPS_OF:
        if len(_GROUPS_OF) > 20_000:  # bounded: old revisions drop out
            _GROUPS_OF.clear()
        _GROUPS_OF[key] = disjuncts(r.where, kinds(r.source))
    return _GROUPS_OF[key]


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


def notifications(records: DataFrame, source: str, rules: "list[Rule] | Compiled") -> DataFrame | None:
    """Kafka records (topic, key, value) for every rule's matches in a batch of `events(...)`, or None: the reference
    path, every rule a Spark expression."""
    compiled = rules if isinstance(rules, Compiled) else compile_rules(rules)
    parts = []
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
