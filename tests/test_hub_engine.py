"""The hub engine (src/hl7rt/engine.py) end to end, with local Spark and the suite's throwaway PostgreSQL: rules
registered by apps through the registry, one read per batch, simple rules matched on the driver and the rest as Spark
expressions, each app's notifications bound to the batch before they're published to its own topic (fake producers),
retries decided from the binding, and every hub (edp_shared, the team hub bu_demo) reading and writing only its own."""

import json
import threading
import time

import pytest
from pyspark.sql import functions as F

from hl7rt import delivery, engine, eventhubs, subscriptions
from hl7rt.engine import Engine
from hl7rt.registry import HubAlreadyRunning, Pool, Registry

OBS = "silver_observations"
RECORDS = [
    {"patient_id": "P1", "observation_cd_desc": "Hemoglobin", "value_num": 7.2},
    {"patient_id": "P2", "observation_cd_desc": "Hemoglobin", "value_num": 13.0},
    {"patient_id": "P3", "observation_cd_desc": "Potassium", "value_num": 6.9},
    {"patient_id": "P4", "observation_cd_desc": "Potassium", "value_num": None},
]


@pytest.fixture
def batch(spark):
    """A micro-batch of the silver_observations topic (value = silver JSON), with Kafka coordinates."""
    spark.conf.set("spark.sql.ansi.enabled", "true")
    rows = [(json.dumps(r), 0, 100 + i, "2026-11-23T12:00:00Z") for i, r in enumerate(RECORDS)]
    return spark.createDataFrame(rows, "value string, partition int, offset long, ts string").select(
        F.col("value").cast("binary").alias("value"), "partition", "offset", F.to_timestamp("ts").alias("timestamp"),
    )  # fmt: skip


class FakeProducer:
    """confluent-kafka's Producer as the engine uses it: every message acked on the next poll (or failed)."""

    def __init__(self, app, log, broken):
        self.app, self.log, self.broken, self._cbs, self.warmed = app, log, broken, [], []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.log.append((topic, key, value))
        self._cbs.append(on_delivery)

    def poll(self, timeout=0):
        cbs, self._cbs = self._cbs, []
        for cb in cbs:
            cb("broker down" if self.app in self.broken else None, None)
        return len(cbs)

    def __len__(self):
        return len(self._cbs)

    def list_topics(self, topic=None, timeout=None):
        self.warmed.append(topic)


class Hub:
    """One hub pipeline's engine on the test registry, with a fake producer per app."""

    def __init__(self, pg, hub="edp_shared", tier="realtime", role="owner", mode="park", broken=()):
        self.produced, self.broken, self.producers = [], set(broken), {}
        self.registry = Registry(Pool(lambda: pg(role), 2), hub, tier, sources=[OBS])
        self.engine = Engine(self.registry, self._producer, mode=mode, chunk=2, lock_wait_s=0.3, warm=False)

    def _producer(self, app):
        self.producers[app] = FakeProducer(app, self.produced, self.broken)
        return self.producers[app]

    def process(self, batch, batch_id, query="q1"):
        self.produced.clear()
        self.engine._last_refresh = 0.0  # every batch sees the latest rules
        stats = self.engine.process(batch, batch_id, OBS, query)
        return sorted(self.produced), stats

    def close(self):
        self.engine.close()


@pytest.fixture
def hubs():
    made = []
    yield lambda *a, **kw: made.append(Hub(*a, **kw)) or made[-1]
    for h in made:
        h.close()


def subscribe(pg, role, name, where, fields, tier="realtime"):
    conn = pg(role)
    r = conn.run(
        "SELECT subscription_api.subscribe(:n, :s, :q, :w, CAST(:f AS text[]), NULL, :tier)",
        n=name, s=OBS, q=f"SELECT {', '.join(fields)} FROM {OBS} WHERE {where}", w=where, f=list(fields), tier=tier,
    )[0][0]  # fmt: skip
    conn.close()
    return r["subscriptionId"]


def unsubscribe(pg, role, name):
    conn = pg(role)
    conn.run("SELECT subscription_api.unsubscribe(:n)", n=name)
    conn.close()


def oracle(pg, batch, hub="edp_shared", tier="realtime"):
    """The reference: every rule this hub runs on the Spark expression path (subscriptions.notifications)."""
    reg = Registry(Pool(lambda: pg("owner"), 1), hub, tier, sources=[OBS])
    rows, _ = reg.refresh()
    reg.close()
    rules = [r.rule for r in rows if r.enabled]
    out = subscriptions.notifications(subscriptions.events(batch, OBS), OBS, subscriptions.compile_rules(rules))
    return sorted((x.topic, x.key, x.value) for x in out.collect()) if out is not None else []


def owner_query(pg, sql, **params):
    conn = pg("owner")
    rows = conn.run(sql, **params)
    conn.close()
    return rows


MIXED = [
    ("app_a", "eng-low", "value_num < 8", ("patient_id", "value_num")),  # simple: the driver
    ("app_a", "eng-potassium", "lower(observation_cd_desc) = 'potassium'", ("patient_id", "observation_cd_desc")),
    ("app_b", "eng-p13", "patient_id IN ('P1', 'P3')", ("patient_id",)),  # simple
    ("app_b", "eng-two", "contains(patient_id, '2') OR patient_id = 'P4'", ("patient_id",)),  # a function: Spark
    ("app_a", "eng-every", "value_num IS NOT NULL", ("patient_id",)),  # simple, no index key: every record
]


def test_mixed_simple_and_expression_rules_in_one_read_give_the_oracles_bytes_per_app(pg, hubs, batch):
    sids = {name: subscribe(pg, role, name, where, fields) for role, name, where, fields in MIXED}
    hub = hubs(pg)
    produced, stats = hub.process(batch, 1)
    assert produced == oracle(pg, batch) and len(produced) == 11
    assert {t for t, _, _ in produced} == {"cat.subscribers.app-a", "cat.subscribers.app-b"}
    rule_set = hub.engine.sets[OBS]
    assert set(rule_set.matcher.entries) == {sids["eng-low"], sids["eng-p13"], sids["eng-every"]}
    assert set(rule_set.expressions) == {sids["eng-potassium"], sids["eng-two"]}
    assert stats["delivered"] == 11 and stats.get("parked", 0) == 0


def test_rules_are_placed_at_refresh_and_active_once_a_batch_decided_with_them(pg, hubs, batch):
    sid = subscribe(pg, "app_a", "eng-low", "value_num < 8", ("patient_id", "value_num"))
    hub = hubs(pg)
    hub.engine.start()
    hub.engine.refresh(force=True)
    states = owner_query(pg, "SELECT state, placed_at IS NOT NULL FROM subscription_registry.status "
                             "WHERE subscription_id = CAST(:s AS uuid)", s=sid)  # fmt: skip
    assert states in ([["PLACED", True]], [["ACTIVE", True]])  # ACTIVE: an earlier test's hub decided with it
    hub.process(batch, 2)
    assert owner_query(
        pg,
        "SELECT hub, state, placed_at IS NOT NULL FROM subscription_registry.status "
        "WHERE subscription_id = CAST(:s AS uuid)",
        s=sid,
    ) == [["edp_shared", "ACTIVE", True]]


def test_a_retried_batch_decides_from_its_binding_after_the_rules_changed(pg, hubs, batch):
    for role, name, where, fields in MIXED:
        subscribe(pg, role, name, where, fields)
    hub = hubs(pg)
    first, _ = hub.process(batch, 7, query="q-retry")
    subscribe(pg, "app_a", "eng-low", "value_num < 1", ("patient_id", "value_num"))  # simple: now matches nothing
    subscribe(pg, "app_a", "eng-potassium", "lower(observation_cd_desc) = 'hemoglobin'",
              ("patient_id", "observation_cd_desc"))  # fmt: skip
    retried, _ = hub.process(batch, 7, query="q-retry")
    assert retried == first, "the retry delivers what the first attempt decided"
    replays = owner_query(pg, "SELECT live_same, digest_match FROM subscription_registry.replays r JOIN "
                              "subscription_registry.bindings b USING (binding_id) WHERE b.query_id = 'q-retry' "
                              "ORDER BY b.app")  # fmt: skip
    assert replays == [[False, True], [True, True]], "app-a rebuilt from its bound revisions; app-b unchanged"
    after, _ = hub.process(batch, 8, query="q-retry")
    assert after == oracle(pg, batch) and after != first, "the next batch runs the new revisions"
    for role, name, where, fields in MIXED[:2]:  # back as they were, for the other tests
        subscribe(pg, role, name, where, fields)


def test_a_rule_that_fails_on_the_data_is_failed_alone(pg, hubs, batch):
    for role, name, where, fields in MIXED:
        subscribe(pg, role, name, where, fields)
    bad = subscribe(pg, "app_a", "eng-bad", "observation_cd_desc > 1", ("patient_id",))  # 'Hemoglobin' isn't a number
    hub = hubs(pg)
    produced, _ = hub.process(batch, 1, query="q-fail")
    (state, error), = owner_query(pg, "SELECT state, error FROM subscription_registry.status "
                                      "WHERE subscription_id = CAST(:s AS uuid)", s=bad)  # fmt: skip
    assert state == "FAILED" and "CAST" in error.upper()
    unsubscribe(pg, "app_a", "eng-bad")
    assert produced == oracle(pg, batch), "every other rule delivered"
    assert bad not in hub.engine.sets[OBS].expressions
    again, _ = hub.process(batch, 2, query="q-fail")
    assert again == produced


def test_bindings_stay_bounded(pg, hubs, batch):
    subscribe(pg, "app_a", "eng-low", "value_num < 8", ("patient_id", "value_num"))
    hub = hubs(pg)
    for b in range(10, 15):
        hub.process(batch, b, query="q-bounded")
    assert owner_query(pg, "SELECT DISTINCT batch_id FROM subscription_registry.bindings WHERE query_id = 'q-bounded' "
                           "ORDER BY 1") == [[13], [14]]  # fmt: skip


def test_a_team_hub_runs_only_its_apps_rules_as_its_own_identity(pg, hubs, batch):
    subscribe(pg, "app_a", "eng-low", "value_num < 8", ("patient_id", "value_num"))
    sid = subscribe(pg, "app_c", "eng-team", "value_num < 8", ("patient_id", "value_num"))
    hub = hubs(pg, "bu_demo", role="hub_sp")
    produced, _ = hub.process(batch, 1, query="q-team")
    assert {t for t, _, _ in produced} == {"cat.subscribers.app-c"} and len(produced) == 2
    assert set(hub.engine.sets[OBS].matcher.entries) == {sid}
    assert owner_query(pg, "SELECT hub, state FROM subscription_registry.status WHERE subscription_id = "
                           "CAST(:s AS uuid)", s=sid) == [["bu_demo", "ACTIVE"]]  # fmt: skip
    beats = owner_query(pg, "SELECT hub, tier, batch_id FROM subscription_registry.hub_heartbeat WHERE source = :s",
                        s=OBS)  # fmt: skip
    assert ["bu_demo", "realtime", 1] in beats


def test_every_apps_deferred_rules_run_on_edp_shared_and_nowhere_else(pg, hubs, batch):
    subscribe(pg, "app_c", "eng-later", "value_num < 8", ("patient_id",), tier="deferred")
    with pytest.raises(RuntimeError, match="TIER_NOT_RUN_HERE"):
        hubs(pg, "bu_demo", "deferred", role="hub_sp").process(batch, 1)
    produced, _ = hubs(pg, tier="deferred").process(batch, 1, query="q-deferred")
    assert {(t, k) for t, k, _ in produced} == {("cat.subscribers.app-c", "P1"), ("cat.subscribers.app-c", "P3")}


def test_a_second_engine_on_the_same_hub_and_lane_is_refused(pg, hubs, batch):
    hubs(pg).process(batch, 1, query="q-one")
    with pytest.raises(HubAlreadyRunning, match="HUB_ALREADY_RUNNING"):
        hubs(pg).process(batch, 1, query="q-two")


def test_one_apps_broken_destination_parks_only_its_notifications_and_a_restart_delivers_them(pg, hubs, batch,
                                                                                            monkeypatch):  # fmt: skip
    monkeypatch.setattr(delivery, "PARK_COLD_S", 0.2)
    monkeypatch.setattr(delivery, "PROBE_S", 0.05)
    monkeypatch.setattr(delivery, "SETTLE_S", 0.1)
    monkeypatch.setattr(delivery, "SEND_WAIT_S", 0.5)
    for role, name, where, fields in MIXED:
        subscribe(pg, role, name, where, fields)
    broken = hubs(pg, broken={"app_b"})
    produced, stats = broken.process(batch, 1, query="q-park")
    assert stats["parked"] == 4 and stats["delivered"] == 7
    parked = owner_query(pg, "SELECT app, count(*) FROM subscription_registry.pending WHERE hub = 'edp_shared' "
                             "GROUP BY app")  # fmt: skip
    assert parked == [["app_b", 4]]
    broken.close()  # the pipeline stops before its drainer delivered them; its drainer ends with it
    for t in threading.enumerate():
        if t.name == "drain-app_b":
            t.join(5)
    fixed = hubs(pg)
    fixed.engine.start()  # a new process resumes what's parked first
    deadline = time.monotonic() + 10
    while owner_query(pg, "SELECT count(*) FROM subscription_registry.pending WHERE hub = 'edp_shared'") != [[0]]:
        assert time.monotonic() < deadline, "the restarted hub drained app-b's parked notifications"
        time.sleep(0.1)
    assert sorted(fixed.produced) == sorted(x for x in oracle(pg, batch) if x[0] == "cat.subscribers.app-b")


def test_each_apps_producer_is_warmed_once_it_has_a_placed_rule(pg, batch):
    subscribe(pg, "app_a", "eng-low", "value_num < 8", ("patient_id", "value_num"))
    made = {}

    def producer(app):
        made[app] = FakeProducer(app, [], set())
        return made[app]

    e = Engine(Registry(Pool(lambda: pg("owner"), 1), "edp_shared", "realtime", sources=[OBS]), producer,
               lock_wait_s=0.3)  # fmt: skip
    try:
        e.start()
        e.refresh(force=True)
        deadline = time.monotonic() + 5
        while "app_a" not in made or not made["app_a"].warmed:
            assert time.monotonic() < deadline
            time.sleep(0.02)
        assert made["app_a"].warmed == ["cat.subscribers.app-a"]
    finally:
        e.close()


def test_the_hub_reads_and_writes_with_the_hung_request_guard():
    """A hung Event Hubs connection held a read or a write until the 60 s request timeout (benchmarks: batches spent
    10-70 s reading silver; a hung write failed the flow). Reads and the per-app producers give up after 5 s."""
    opts = eventhubs.hub_read_options("ehns", "cred", "cat.hl7.silver_observations", 20000)
    assert opts["kafka.request.timeout.ms"] == eventhubs.HUNG_REQUEST_GUARD["kafka.request.timeout.ms"] == "5000"
    assert (opts["subscribe"], opts["startingOffsets"], opts["maxOffsetsPerTrigger"]) == (
        "cat.hl7.silver_observations", "latest", "20000")  # fmt: skip
    cfg = delivery.producer_config("ehns", None)
    assert cfg["request.timeout.ms"] == 5000


def test_the_hubs_notifications_follow_the_published_contract(batch):
    from hl7rt.subscriptions import Rule
    from subscription_api.app import Notification

    rules = [Rule("s1", "r", 1, OBS, "value_num < 8", ["patient_id", "value_num"], "t", frozenset({"patient_id",
             "value_num"})), Rule("s2", "x", 1, OBS, "lower(observation_cd_desc) = 'potassium'", ["patient_id"], "t",
             frozenset({"patient_id", "observation_cd_desc"}))]  # fmt: skip
    out = engine.notifications(batch, OBS, rules)
    assert out
    for _, _, value in out:
        assert set(json.loads(value)) == set(Notification.model_fields)
        Notification.model_validate(json.loads(value))
