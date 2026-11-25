"""The hub's registry client (src/hl7rt/registry.py): a bounded pool of Lakebase connections shared by the hub's
flows (pg8000 connections aren't thread-safe), the rules one (hub, tier, lane) runs, read only when they changed, and
everything the hub writes, keyed by its hub: status, heartbeat, progress, captured revisions, batch bindings, parked
notifications and delivery circuits, and the session lock that keeps one engine per (hub, lane).

Most tests run against the suite's throwaway PostgreSQL with src/registry/registry.sql applied (tests/conftest.py):
app-a and app-b on edp_shared, app-c on the team hub bu_demo, whose pipeline connects as `hub_sp` (row-level security
applies to it; edp_shared connects as the owner and filters by hub itself)."""

import threading
import time

import pg8000.exceptions
import pytest

from hl7rt import registry as registry_module
from hl7rt.delivery import Msg
from hl7rt.registry import HubAlreadyRunning, Pool, Registry, lane_of
from hl7rt.subscriptions import SOURCES


# -- the pool ------------------------------------------------------------------------------------------------------
class OneAtATime:
    """A fake connection that counts two threads using it at once."""

    overlaps = 0

    def __init__(self):
        self.busy = threading.Lock()

    def run(self, sql, **params):
        if not self.busy.acquire(blocking=False):
            OneAtATime.overlaps += 1
            return []
        try:
            time.sleep(0.002)
            return []
        finally:
            self.busy.release()

    def close(self):
        pass


def test_concurrent_flows_never_share_a_connection_and_open_at_most_the_pools_size():
    opened = []
    pool = Pool(lambda: opened.append(OneAtATime()) or opened[-1], size=2)
    OneAtATime.overlaps = 0

    def flow():
        for _ in range(25):
            pool.run("SELECT 1")

    threads = [threading.Thread(target=flow) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert OneAtATime.overlaps == 0 and len(opened) <= 2


def test_a_dropped_connection_is_reopened_on_the_next_call():
    """Lakebase drops connections (idle close, compute restart). The hub must reconnect, not fail every batch until the
    pipeline restarts (seen live: a "network error" failed the flow three times, then the whole update)."""

    class Dropped:
        def run(self, sql, **params):
            raise pg8000.exceptions.InterfaceError("network error")

        def close(self):
            pass

    class Fine:
        def run(self, sql, **params):
            return [[1]]

        def close(self):
            pass

    opened = [Dropped(), Fine()]
    pool = Pool(lambda: opened.pop(0), size=1)
    assert pool.run("SELECT 1") == [[1]] and opened == [], "the dropped connection was replaced by a new one"


def test_a_statement_error_returns_the_connection_to_the_pool():
    class Bad:
        def run(self, sql, **params):
            raise pg8000.exceptions.DatabaseError({"M": "boom"})

        def close(self):
            pass

    opened = []
    pool = Pool(lambda: opened.append(Bad()) or opened[-1], size=1)
    for _ in range(3):
        with pytest.raises(pg8000.exceptions.DatabaseError):
            pool.run("SELECT 1")
    assert len(opened) == 1


def test_a_lane_is_the_tier_or_the_tier_and_its_sources():
    assert lane_of("realtime", list(SOURCES)) == "realtime"
    assert lane_of("realtime", ["silver_orders"]) == "realtime:silver_orders"
    assert lane_of("deferred", ["silver_orders", "silver_diagnoses"]) == "deferred:silver_diagnoses,silver_orders"


def test_a_hub_reading_every_configured_table_runs_in_its_tiers_lane():
    from pyspark.sql.types import StringType, StructField, StructType

    from hl7rt import subscriptions

    try:
        subscriptions.add_table_source("epic_realtime", StructType([StructField("patient_id", StringType())]))
        assert lane_of("realtime", ["epic_realtime"]) == "realtime"
        assert lane_of("realtime", list(SOURCES)) == f"realtime:{','.join(sorted(SOURCES))}"
    finally:
        subscriptions.reset_sources()


# -- against the registry --------------------------------------------------------------------------------------------
OBS = "silver_observations"


def subscribe(conn, name, where="value_num > 6", fields=("patient_id", "value_num"), tier="realtime", source=OBS):
    return conn.run(
        "SELECT subscription_api.subscribe(:n, :s, :q, :w, CAST(:f AS text[]), NULL, :tier)",
        n=name, s=source, q=f"SELECT {', '.join(fields)} FROM {source} WHERE {where}", w=where, f=list(fields),
        tier=tier,
    )[0][0]  # fmt: skip


def unsubscribe(conn, name):
    return conn.run("SELECT subscription_api.unsubscribe(:n)", n=name)[0][0]


_OPEN = []


@pytest.fixture(autouse=True)
def _close_registries():
    yield
    while _OPEN:
        _OPEN.pop().close()


def registry(pg, hub="edp_shared", tier="realtime", role="owner", sources=(OBS,)):
    reg = Registry(Pool(lambda: pg(role), size=2), hub, tier, sources=list(sources))
    _OPEN.append(reg)
    return reg


@pytest.fixture(scope="module")
def rules(pg):
    """Rules of every app: app-a and app-b realtime (edp_shared), app-c realtime (bu_demo), app-c deferred."""
    a, b, c = pg("app_a"), pg("app_b"), pg("app_c")
    out = {
        "a": subscribe(a, "reg-a"), "b": subscribe(b, "reg-b", where="patient_id = 'P1'", fields=("patient_id",)),
        "c": subscribe(c, "reg-c"), "c-later": subscribe(c, "reg-c-later", tier="deferred"),
    }  # fmt: skip
    for conn in (a, b, c):
        conn.close()
    return {k: v["subscriptionId"] for k, v in out.items()}


def placed(reg):
    changed, removed = reg.refresh()
    return {row.rule.subscription_id: row for row in changed}, removed


def test_edp_shared_reads_only_the_realtime_rules_of_its_own_apps(pg, rules):
    rows, _ = placed(registry(pg))
    assert set(rows) == {rules["a"], rules["b"]}
    assert rows[rules["a"]].owner == "app_a" and rows[rules["a"]].rule.topic == "cat.subscribers.app-a"
    assert rows[rules["a"]].rule.granted == frozenset({"patient_id", "value_num", "observation_cd_desc"})


def test_a_team_hub_reads_only_its_apps_realtime_rules_as_its_own_identity(pg, rules):
    rows, _ = placed(registry(pg, "bu_demo", role="hub_sp"))
    assert set(rows) == {rules["c"]}


def test_every_apps_deferred_rules_run_on_edp_shared(pg, rules):
    rows, _ = placed(registry(pg, tier="deferred"))
    assert set(rows) == {rules["c-later"]}, "app-c is on bu_demo, but its deferred rule runs on edp_shared"


def test_a_hub_checks_it_is_onboarded_and_runs_its_tier_before_it_starts(pg, rules):
    registry(pg).check()
    registry(pg, "bu_demo", role="hub_sp").check()
    with pytest.raises(RuntimeError, match="TIER_NOT_RUN_HERE"):
        registry(pg, "bu_demo", "deferred", role="hub_sp").check()
    with pytest.raises(RuntimeError, match="HUB_NOT_ONBOARDED"):
        registry(pg, "nowhere").check()
    with pytest.raises(RuntimeError, match="HUB_NOT_ONBOARDED"):  # row-level security: it can't see edp_shared
        registry(pg, "edp_shared", role="hub_sp").check()


def test_after_the_first_load_only_changed_rules_are_returned(pg, rules):
    reg = registry(pg)
    first, _ = placed(reg)
    assert reg.refresh() == ([], []), "nothing changed: one cheap query"
    a = pg("app_a")
    subscribe(a, "reg-a", where="value_num > 7")
    changed, _ = placed(reg)
    assert set(changed) == {rules["a"]} and changed[rules["a"]].rule.revision == first[rules["a"]].rule.revision + 1
    assert changed[rules["a"]].rule.where == "value_num > 7"
    subscribe(a, "reg-a")  # back, so other tests see revision 3 with the original rule
    a.close()


def test_a_cancelled_rule_is_returned_disabled_and_a_moved_app_is_removed(pg, rules):
    reg = registry(pg)
    placed(reg)
    b = pg("app_b")
    unsubscribe(b, "reg-b")
    changed, _ = placed(reg)
    assert not changed[rules["b"]].enabled
    subscribe(b, "reg-b", where="patient_id = 'P1'", fields=("patient_id",))
    b.close()
    owner = pg("owner")
    owner.run("UPDATE subscription_registry.apps SET hub = 'bu_demo' WHERE app_role = 'app_b'")
    _, removed = reg.refresh()
    assert removed == [rules["b"]], "app-b's rules now run on bu_demo, not here"
    owner.run("UPDATE subscription_registry.apps SET hub = 'edp_shared' WHERE app_role = 'app_b'")
    owner.close()


def test_a_change_committed_out_of_order_is_not_missed(pg, rules, monkeypatch):
    """Generations are taken when a write starts; a slow write can commit after a later one was read. The client
    compares the registry's sum of generations with its own and reloads everything when they differ."""
    monkeypatch.setattr(registry_module, "LOOKBACK", 0)  # the late write's number is below what the client re-reads
    reg = registry(pg)
    placed(reg)
    owner = pg("owner")
    # a write that took an old generation number commits only now (below the client's lookback window)
    owner.run("ALTER TABLE subscription_registry.subscriptions DISABLE TRIGGER USER")
    owner.run("UPDATE subscription_registry.subscriptions SET where_sql = 'value_num > 9', revision = revision + 1, "
              "query = 'SELECT patient_id, value_num FROM silver_observations WHERE value_num > 9', generation = 1 "
              "WHERE subscription_id = CAST(:s AS uuid)", s=rules["a"])  # fmt: skip
    owner.run("ALTER TABLE subscription_registry.subscriptions ENABLE TRIGGER USER")
    changed, _ = placed(reg)
    assert changed[rules["a"]].rule.where == "value_num > 9"
    a = pg("app_a")
    subscribe(a, "reg-a", where="value_num > 6")
    a.close()
    owner.close()


def statuses(pg, sid):
    owner = pg("owner")
    rows = owner.run("SELECT hub, revision, state, error, placed_at IS NOT NULL FROM subscription_registry.status "
                     "WHERE subscription_id = CAST(:s AS uuid)", s=sid)  # fmt: skip
    owner.close()
    return rows


def test_verdicts_are_keyed_by_hub_and_placed_at_survives_active(pg, rules):
    reg = registry(pg)
    rev = placed(reg)[0][rules["a"]].rule.revision
    reg.report([(rules["a"], rev)], "PLACED")
    reg.report([(rules["a"], rev)], "ACTIVE")
    reg.report([(rules["a"], rev)], "ACTIVE")  # once per change
    assert statuses(pg, rules["a"]) == [["edp_shared", rev, "ACTIVE", None, True]]
    team = registry(pg, "bu_demo", role="hub_sp")
    team.report([(rules["c"], 1)], "FAILED", {rules["c"]: "CAST_INVALID_INPUT"})
    assert statuses(pg, rules["c"]) == [["bu_demo", 1, "FAILED", "CAST_INVALID_INPUT", False]]


def test_a_team_hub_cannot_write_another_hubs_rows(pg, rules):
    sp = pg("hub_sp")
    with pytest.raises(pg8000.exceptions.DatabaseError):
        sp.run("INSERT INTO subscription_registry.hub_heartbeat (hub, tier, source, last_batch_at, batch_id, rules) "
               "VALUES ('edp_shared', 'realtime', :s, now(), 1, 1)", s=OBS)  # fmt: skip
    sp.close()


def test_heartbeat_and_progress_are_keyed_by_hub_tier_and_source(pg, rules):
    reg, team = registry(pg), registry(pg, "bu_demo", role="hub_sp")
    for r, n in ((reg, 2), (team, 1)):
        r.heartbeat(OBS, 41, n)
        r.heartbeat(OBS, 42, n)
        r.progress(OBS, 42, n, {"refresh": 1, "collect": 20, "match": 3, "bind": 2, "deliver": 9, "status": 1},
                   rows=100, delivered=7, parked=0, age_ms=900, spark_ms={"triggerExecution": 1200})  # fmt: skip
    owner = pg("owner")
    beats = owner.run("SELECT hub, tier, source, batch_id, rules FROM subscription_registry.hub_heartbeat "
                      "WHERE source = :s ORDER BY hub", s=OBS)  # fmt: skip
    assert beats == [["bu_demo", "realtime", OBS, 42, 1], ["edp_shared", "realtime", OBS, 42, 2]]
    prog = owner.run("SELECT hub, collect_ms, rows, delivered, age_ms, spark_ms FROM "
                     "subscription_registry.hub_progress WHERE source = :s AND batch_id = 42 ORDER BY hub",
                     s=OBS)  # fmt: skip
    assert prog == [["bu_demo", 20, 100, 7, 900, {"triggerExecution": 1200}],
                    ["edp_shared", 20, 100, 7, 900, {"triggerExecution": 1200}]]  # fmt: skip
    owner.close()


def test_a_revision_is_captured_once_as_it_ran(pg, rules):
    reg = registry(pg)
    rows, _ = placed(reg)
    row = rows[rules["a"]]
    reg.capture([(row.rule, row.owner)])
    reg.capture([(row.rule, row.owner)])
    (got,) = reg.captured({rules["a"]: row.rule.revision})
    assert got == (row.rule, row.owner)
    assert reg.captured({rules["a"]: 999}) == []


def binding(app, batch_id, query="q1", n=1, revisions=None, source=OBS):
    return {"binding_id": f"edp_shared:realtime:{app}:{source}:{query}:{batch_id}", "app": app, "source": source,
            "query_id": query, "batch_id": batch_id, "offsets": {"0": [0, 9]},
            "revisions": revisions or {"s1": 1}, "n": n, "digest": f"d{batch_id}"}  # fmt: skip


def test_a_batch_is_bound_once_and_a_retry_finds_its_binding(pg, rules):
    reg = registry(pg)
    assert reg.bound(OBS, "q1", 5) == {}
    reg.bind([binding("app_a", 5), binding("app_b", 5, n=0)])
    reg.bind([binding("app_a", 5, n=99)])  # a retry never overwrites the first decision
    got = reg.bound(OBS, "q1", 5)
    assert set(got) == {"app_a", "app_b"} and got["app_a"]["n"] == 1 and got["app_a"]["revisions"] == {"s1": 1}
    reg.replayed(got["app_a"]["binding_id"], live_same=False, digest_match=True, n=1)


def test_bindings_stay_bounded_to_the_last_two_batches_of_a_flow(pg, rules):
    reg = registry(pg)
    for b in range(10, 16):
        reg.bind([binding("app_a", b, query="q2")])
        reg.prune(OBS, "q2", b)
    reg.bind([binding("app_a", 3, query="q-old-checkpoint")])
    reg.prune(OBS, "q2", 16)
    owner = pg("owner")
    left = owner.run("SELECT query_id, batch_id FROM subscription_registry.bindings WHERE hub = 'edp_shared' "
                     "AND source = :s AND query_id LIKE 'q%' AND query_id <> 'q1' ORDER BY batch_id",
                     s=OBS)  # fmt: skip
    assert left == [["q2", 15]], "a new checkpoint's query replaces the old one's bindings"
    owner.close()


def msg(app, i, nid=None):
    return Msg(app, f"cat.subscribers.{app}", f"P{i}", f'{{"n":{i}}}', [], nid or f"n{i}", "b1")


def test_parked_notifications_keep_their_order_and_leave_once_delivered(pg, rules):
    store = registry(pg).store()
    store.park([msg("app_a", i) for i in range(5)] + [msg("app_b", 9)])
    head = store.head("app_a", 3)
    assert [(t, k, v, n, b) for _, t, k, v, n, b in head] == [
        ("cat.subscribers.app_a", f"P{i}", f'{{"n":{i}}}', f"n{i}", "b1") for i in range(3)]  # fmt: skip
    assert sorted(store.apps_with_pending()) == ["app_a", "app_b"]
    store.delete_seqs([s for s, *_ in head])
    store.delete_nids("app_a", ["n3"])
    assert [n for *_, n, _b in store.head("app_a", 10)] == ["n4"]
    store.delete_nids("app_a", ["n4"])
    store.delete_nids("app_b", ["n9"])
    assert store.apps_with_pending() == []


def test_a_team_hubs_store_sees_only_its_own_parked_notifications(pg, rules):
    registry(pg).store().park([msg("app_a", 1, nid="shared-1")])
    team = registry(pg, "bu_demo", role="hub_sp").store()
    team.park([msg("app_c", 1, nid="team-1")])
    assert team.apps_with_pending() == ["app_c"]
    team.delete_nids("app_c", ["team-1"])
    registry(pg).store().delete_nids("app_a", ["shared-1"])


def test_a_circuit_change_is_recorded_with_its_history(pg, rules):
    store = registry(pg).store()
    store.set_state("app_a", "OPEN", 3)
    store.set_state("app_a", "CLOSED", 0)
    owner = pg("owner")
    assert owner.run("SELECT state, pending FROM subscription_registry.app_delivery WHERE hub = 'edp_shared' "
                     "AND app = 'app_a'") == [["CLOSED", 0]]  # fmt: skip
    assert owner.run("SELECT state FROM subscription_registry.app_delivery_log WHERE hub = 'edp_shared' "
                     "AND app = 'app_a' ORDER BY at") == [["OPEN"], ["CLOSED"]]  # fmt: skip
    owner.close()


def test_a_drainer_lock_is_held_by_one_session(pg, rules):
    one, two = registry(pg).drain_store(), registry(pg).drain_store()
    assert one.try_lock("drain:edp_shared:realtime:app_a")
    assert not two.try_lock("drain:edp_shared:realtime:app_a")
    one.unlock("drain:edp_shared:realtime:app_a")
    assert two.try_lock("drain:edp_shared:realtime:app_a")
    two.unlock("drain:edp_shared:realtime:app_a")


def test_one_engine_per_hub_and_lane(pg, rules):
    first = registry(pg)
    first.lock(wait_s=0)
    with pytest.raises(HubAlreadyRunning, match="HUB_ALREADY_RUNNING"):
        registry(pg).lock(wait_s=0.3)
    registry(pg, tier="deferred").lock(wait_s=0)  # another lane of the same hub is another pipeline
    first.unlock()
    registry(pg).lock(wait_s=0)


def test_a_lost_session_lock_is_taken_again(pg, rules):
    reg = registry(pg)
    reg.lock(wait_s=0)
    reg._lock_conn.close()  # Lakebase dropped the lock's connection: the lock went with it
    reg.keep_lock()
    with pytest.raises(HubAlreadyRunning):
        registry(pg).lock(wait_s=0)
    reg.unlock()


def test_the_registry_client_uses_the_registry_connect_by_default(monkeypatch):
    seen = []
    monkeypatch.setattr(registry_module, "connect", lambda endpoint, database: seen.append((endpoint, database)))
    Pool.lakebase("projects/p/branches/production/endpoints/primary", "db", 1).open()
    assert seen == [("projects/p/branches/production/endpoints/primary", "db")]
