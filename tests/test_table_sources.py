"""Subscribing over a Delta table instead of the silver topics (hl7.subscriptions.tables), e.g. a customer's existing
streaming table on the Epic JSON contract (hl7rt.epic_json). The hub's read gives each row its coordinates from SQL
expressions (subscriptions.table_records); after that the rows go through the same engine as a topic's records, so a
table source delivers exactly the reference path's bytes, routed per app. Local Spark and the suite's throwaway
PostgreSQL; the Delta read itself (its options) runs only on Databricks."""

import json

import pytest
from pyspark.sql import functions as F

from hl7rt import engine, subscriptions
from hl7rt.engine import Engine
from hl7rt.registry import Pool, Registry
from subscription_api.rules import parse
from test_hub_engine import FakeProducer

EPIC = "epic_realtime"  # the source cat.epic.epic_realtime (hl7.subscriptions.tables) is named after
# A table on the Epic contract's columns (a subset), as the hub's read sees it.
COLUMNS = {
    "patient_id": "string", "msg_type": "string", "msg_seq": "string", "observation_cd": "string",
    "observation_cd_desc": "string", "patient_point_of_care": "string", "msg_dt_tm": "timestamp", "payload": "string",
    "kafka_timestamp": "timestamp", "_ingested_at": "timestamp",
}  # fmt: skip
ROWS = [
    ("P1", "ORU^R01", "11", "2823-3", "Potassium", "ED", "2026-11-24 11:59:00", '{"a":1}', "2026-11-24 12:00:00",
     "2026-11-24 12:00:01"),
    ("P2", "ORU^R01", "12", "718-7", "Hemoglobin", "ED", "2026-11-24 11:59:30", '{"a":2}', "2026-11-24 12:00:00",
     "2026-11-24 12:00:02"),
    ("P3", "ADT^A04", "13", None, None, "ICU", None, '{"a":3}', "2026-11-24 12:00:00", "2026-11-24 12:00:03"),
    ("P4", "ORU^R01", "14", "2823-3", "Potassium", "ICU", "2026-11-24 11:58:00", None, "2026-11-24 12:00:00",
     "2026-11-24 12:00:04"),
]  # fmt: skip
PARTITION, OFFSET, TS = "pmod(hash(patient_id), 2)", "CAST(msg_seq AS BIGINT) * 1000", "_ingested_at"
FIELDS = ["patient_id", "msg_type", "observation_cd", "observation_cd_desc", "patient_point_of_care", "msg_dt_tm"]


@pytest.fixture
def table(spark):
    """The table's rows, as `spark.read.table` would give them (the schema the hub registers at startup)."""
    spark.conf.set("spark.sql.ansi.enabled", "true")
    rows = spark.createDataFrame(ROWS, ", ".join(f"{c} string" for c in COLUMNS))
    return rows.select(*[F.col(c).cast(t) for c, t in COLUMNS.items()])


@pytest.fixture
def epic(table):
    """epic_realtime registered as the hub does at startup; the built-in sources again afterwards."""
    subscriptions.add_table_source(EPIC, table.schema)
    yield
    subscriptions.reset_sources()


@pytest.fixture
def batch(table, epic):
    """A micro-batch of the table: its rows with the coordinates the hub's read adds."""
    return subscriptions.table_records(table, PARTITION, OFFSET, TS)


def test_table_sources_are_named_after_their_tables():
    assert subscriptions.table_sources("") == {} == subscriptions.table_sources(" , ")
    assert subscriptions.table_sources(" cat.epic.epic_realtime, ,cat.lab.Lab_Feed ") == {
        "epic_realtime": "cat.epic.epic_realtime", "lab_feed": "cat.lab.Lab_Feed"}  # fmt: skip


@pytest.mark.parametrize("spec", ["epic_realtime", "epic.epic_realtime", "a.b.c.d", "cat..epic_realtime",
                                  "cat.a.epic_realtime, cat.b.EPIC_REALTIME"])  # fmt: skip
def test_a_table_source_is_one_three_part_name_per_source(spec):
    with pytest.raises(ValueError):
        subscriptions.table_sources(spec)


def test_a_table_is_read_from_the_live_edge_bounded_and_without_change_commits():
    """A new checkpoint doesn't replay the table's history; a retention delete or OPTIMIZE doesn't fail the stream (a
    corrected row isn't notified again); each batch is at most `max_files` new files (the hub collects it once)."""
    assert subscriptions.table_read_options("100") == {
        "startingVersion": "latest", "skipChangeCommits": "true", "maxFilesPerTrigger": "100"}  # fmt: skip


def test_a_table_sources_rows_are_read_as_they_are_with_their_coordinates(batch):
    records = subscriptions.events(batch, EPIC)
    assert records.columns == [*subscriptions.SOURCES[EPIC].fieldNames(), "_partition", "_offset", "_source_ts"]
    plan = records._jdf.queryExecution().optimizedPlan().toString().lower()
    assert "from_json" not in plan and "jsontostructs" not in plan, "the table's columns, not a JSON value"
    rows = {r.patient_id: r for r in records.collect()}
    assert (rows["P1"].observation_cd, rows["P1"]._offset, str(rows["P1"]._source_ts)) == (
        "2823-3", 11_000, "2026-11-24 12:00:01")  # fmt: skip
    assert {type(r._partition) for r in rows.values()} == {int} and {r._partition for r in rows.values()} <= {0, 1}


def test_a_table_source_is_typed_by_its_registered_schema(epic):
    kinds = subscriptions.kinds(EPIC)
    assert kinds["patient_id"] == kinds["msg_type"] == "string"
    assert "msg_dt_tm" not in kinds, "a timestamp keeps the expression path"
    assert EPIC in subscriptions.TABLES and EPIC not in subscriptions.SILVER


def test_registering_a_table_source_is_undone_by_a_reset(table):
    subscriptions.add_table_source(EPIC, table.schema)
    subscriptions.reset_sources()
    assert subscriptions.SOURCES == subscriptions.SILVER and not subscriptions.TABLES


def rule(sql, sid, name, topic="cat.subscribers.app-a"):
    r = parse(sql, {EPIC}, "cat.sub_app_a")
    return subscriptions.Rule(sid, name, 1, r.source, r.where, list(r.fields), topic, frozenset(FIELDS))


def reference(batch, rules):
    out = subscriptions.notifications(subscriptions.events(batch, EPIC), EPIC, subscriptions.compile_rules(rules))
    return sorted((x.topic, x.key, x.value) for x in out.collect()) if out is not None else []


RULES = [
    ("SELECT patient_id, observation_cd, msg_dt_tm FROM epic_realtime WHERE msg_type = 'ORU^R01' "
     "AND observation_cd IN ('2823-3')", "k-ed"),  # simple: the driver
    ("SELECT patient_id, patient_point_of_care FROM epic_realtime WHERE patient_point_of_care = 'ICU' "
     "OR patient_id = 'P1'", "icu-or-p1"),  # simple, two branches
    ("SELECT patient_id, observation_cd_desc FROM epic_realtime WHERE contains(observation_cd_desc, 'glob')",
     "hgb"),  # a function: Spark
    ("SELECT patient_id, msg_dt_tm FROM epic_realtime WHERE msg_dt_tm > TIMESTAMP '2026-11-24 11:58:30'",
     "recent"),  # a timestamp: Spark
]  # fmt: skip


def test_the_driver_and_the_expression_path_send_identical_notifications_for_a_table(batch):
    rules = [rule(sql, f"0000000{i}-0000-4000-8000-000000000000", name) for i, (sql, name) in enumerate(RULES)]
    out = sorted(engine.notifications(batch, EPIC, rules))
    assert out == reference(batch, rules) and len(out) == 8
    first = json.loads(next(v for _, k, v in out if k == "P1" and "k-ed" in v))
    assert first["payload"] == {"patient_id": "P1", "observation_cd": "2823-3", "msg_dt_tm": "2026-11-24T11:59:00.000Z"}
    assert (first["source"], first["sourceOffset"], first["source_ts"]) == (EPIC, 11_000, "2026-11-24T12:00:01.000Z")


# -- the hub engine on a table source: rules registered by apps, decided, bound, and published per app ----------------
@pytest.fixture(scope="module")
def granted(pg):
    """app-a and app-b may subscribe to epic_realtime (onboarding grants a table source like a silver one)."""
    owner = pg()
    owner.run("UPDATE subscription_registry.apps SET allowed_sources = allowed_sources || CAST(:s AS text), "
              "allowed_fields = CAST(:f AS text[]) WHERE app_role IN ('app_a', 'app_b')", s=EPIC, f=FIELDS)  # fmt: skip
    owner.close()
    return pg


def subscribe(pg, role, name, sql):
    r = parse(sql, {EPIC}, f"cat.sub_{role}")
    conn = pg(role)
    sid = conn.run("SELECT subscription_api.subscribe(:n, :s, :q, :w, CAST(:f AS text[]), NULL, 'realtime')",
                   n=name, s=r.source, q=r.query, w=r.where, f=list(r.fields))[0][0]["subscriptionId"]  # fmt: skip
    conn.close()
    return sid


def test_a_table_sources_batch_through_the_hub_gives_the_oracles_bytes_per_app(granted, batch):
    from subscription_api.app import Notification

    for i, (sql, name) in enumerate(RULES):
        subscribe(granted, "app_a" if i % 2 == 0 else "app_b", name, sql)
    produced = []
    hub = Engine(Registry(Pool(lambda: granted("owner"), 2), "edp_shared", "realtime", sources=[EPIC]),
                 lambda app: FakeProducer(app, produced, set()), chunk=2, lock_wait_s=0.3, warm=False)  # fmt: skip
    try:
        stats = hub.process(batch, 1, EPIC, "q-table")
        assert hub.sets[EPIC].matcher.entries and hub.sets[EPIC].expressions, "both paths in play"
    finally:
        hub.close()
    reg = Registry(Pool(lambda: granted("owner"), 1), "edp_shared", "realtime", sources=[EPIC])
    rules = [r.rule for r in reg.refresh()[0] if r.enabled]
    reg.close()
    assert sorted(produced) == reference(batch, rules) and stats["delivered"] == len(produced) == 8
    assert {t for t, _, _ in produced} == {"cat.subscribers.app-a", "cat.subscribers.app-b"}
    for _, _, value in produced:
        assert set(json.loads(value)) == set(Notification.model_fields)
        assert Notification.model_validate(json.loads(value)).source == EPIC
    owner = granted()
    assert owner.run("SELECT hub, tier, batch_id FROM subscription_registry.hub_heartbeat WHERE source = :s",
                     s=EPIC) == [["edp_shared", "realtime", 1]]  # fmt: skip
    owner.close()
