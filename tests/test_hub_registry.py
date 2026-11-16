"""The hub's registry client is shared by one flow per silver source, whose batches run on concurrent threads. pg8000
connections aren't thread-safe ("portal does not exist" on Lakebase), so every call must hold the client's lock."""

import threading
import time

from hl7rt.registry import Registry


class OneAtATime:
    """A fake connection that fails if two threads use it at once."""

    def __init__(self):
        self.busy = threading.Lock()
        self.overlaps = 0

    def run(self, sql, **params):
        if not self.busy.acquire(blocking=False):
            self.overlaps += 1
            return [[0]]
        try:
            time.sleep(0.002)
            return [[0, 0]] if "max(generation)" in sql else []
        finally:
            self.busy.release()


def test_concurrent_flows_never_share_the_connection_at_once():
    registry = Registry("projects/p/branches/production/endpoints/primary", "db")
    conn = OneAtATime()
    registry._db = lambda: conn  # no Lakebase in unit tests

    def flow(source):
        for i in range(25):
            registry.refresh()
            registry.rules(source)
            registry.report(f"id-{source}-{i}", 1, "ACTIVE")
            registry.heartbeat(source, i, 0)

    sources = ("silver_encounters", "silver_observations", "silver_orders")
    threads = [threading.Thread(target=flow, args=(s,)) for s in sources]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert conn.overlaps == 0


def test_a_dropped_connection_is_reopened_on_the_next_call(monkeypatch):
    """Lakebase drops connections (idle close, compute restart). The hub must reconnect, not fail every batch until the
    pipeline restarts (seen live: a "network error" failed the flow three times, then the whole update)."""
    import pg8000.exceptions

    from hl7rt import registry as registry_module

    class Dropped:
        def run(self, sql, **params):
            raise pg8000.exceptions.InterfaceError("network error")

        def close(self):
            pass

    class Fine:
        def run(self, sql, **params):
            return [[0, 0]] if "max(generation)" in sql else []

        def close(self):
            pass

    opened = [Dropped(), Fine()]
    monkeypatch.setattr(registry_module, "connect", lambda endpoint, database: opened.pop(0))
    registry = Registry("projects/p/branches/production/endpoints/primary", "db")

    registry.refresh()
    registry.heartbeat("silver_observations", 1, 0)
    assert opened == [], "the dropped connection was replaced by a new one"


class FakeRegistry:
    """The subscriptions table as the hub reads it: rows (id, name, revision, enabled, source, where, fields, topic,
    granted fields, generation, tier), and every query the client ran."""

    def __init__(self):
        self.rows, self.queries = {}, []

    def put(self, sid, source, generation, enabled=True, where="value_num > 1", tier="realtime"):
        self.rows[sid] = (sid, sid, generation, enabled, source, where, ["patient_id"], "t", ["patient_id"], generation,
                          tier)  # fmt: skip

    def run(self, sql, **params):
        self.queries.append((sql, params))
        if "max(generation)" in sql:
            gens = [r[9] for r in self.rows.values()]
            return [[max(gens, default=0), str(sum(gens))]]
        rows = list(self.rows.values())
        if "generation >" in sql:
            rows = [r for r in rows if r[9] > params["g"]]
        return rows


def _registry(db, tier="realtime"):
    registry = Registry("projects/p/branches/production/endpoints/primary", "db", tier)
    registry._db = lambda: db
    return registry


def test_after_the_first_load_only_changed_subscriptions_are_read():
    """Hundreds of apps editing patient lists: a change reads that subscription, not the whole registry."""
    db = FakeRegistry()
    for i in range(500):
        db.put(f"s{i}", "silver_observations", i + 1)
    registry = _registry(db)
    assert registry.refresh() and len(registry.rules("silver_observations")) == 500
    assert registry.refresh() is False, "nothing changed: one cheap query"
    db.put("s7", "silver_observations", 501, where="value_num > 2")
    db.queries.clear()
    assert registry.refresh()
    (_, signature), (delta, params) = db.queries
    assert "generation >" in delta and len([r for r in db.rows.values() if r[9] > params["g"]]) < 200
    assert next(r.where for r in registry.rules("silver_observations") if r.subscription_id == "s7") == "value_num > 2"


def test_a_change_committed_out_of_order_is_not_missed():
    """Generations are taken when a write starts; a slow write can commit after a later one was read. The client
    compares the registry's sum of generations with its own, and reloads everything when they differ."""
    db = FakeRegistry()
    for i in range(1_000):
        db.put(f"s{i}", "silver_observations", i + 1)
    registry = _registry(db)
    registry.refresh()
    db.put("late", "silver_observations", 3, where="value_num > 9")  # took number 3 long ago, committed only now
    db.put("s999", "silver_observations", 1_001)
    assert registry.refresh()
    assert any(r.subscription_id == "late" for r in registry.rules("silver_observations"))


def test_each_source_has_its_own_generation_so_one_sources_change_doesnt_rebuild_the_others():
    db = FakeRegistry()
    db.put("a", "silver_observations", 1)
    db.put("b", "silver_encounters", 2)
    registry = _registry(db)
    registry.refresh()
    before = registry.source_generation("silver_encounters")
    db.put("a", "silver_observations", 3, where="value_num > 5")
    registry.refresh()
    assert registry.source_generation("silver_encounters") == before
    assert registry.source_generation("silver_observations") != before
    db.put("a", "silver_encounters", 4)  # the rule now reads encounters: both sources change
    obs = registry.source_generation("silver_observations")
    registry.refresh()
    assert registry.source_generation("silver_observations") != obs
    assert sorted(r.subscription_id for r in registry.rules("silver_encounters")) == ["a", "b"]


def test_a_cancelled_subscription_leaves_its_sources_rules_and_moves_its_generation():
    db = FakeRegistry()
    db.put("a", "silver_observations", 1)
    registry = _registry(db)
    registry.refresh()
    db.put("a", "silver_observations", 2, enabled=False)
    registry.refresh()
    assert registry.rules("silver_observations") == [] and registry.cancelled("silver_observations") == [("a", 2)]
    assert registry.source_generation("silver_observations") == (2, 2, 1)


def test_each_hub_runs_only_its_own_tier_and_keys_its_heartbeat_by_it():
    db = FakeRegistry()
    db.put("now", "silver_observations", 1)
    db.put("later", "silver_observations", 2, tier="deferred")
    realtime, deferred = _registry(db), _registry(db, "deferred")
    realtime.refresh(), deferred.refresh()
    assert [r.subscription_id for r in realtime.rules("silver_observations")] == ["now"]
    assert [r.subscription_id for r in deferred.rules("silver_observations")] == ["later"]
    gen = realtime.source_generation("silver_observations")
    db.put("later", "silver_observations", 3, tier="deferred", where="value_num > 3")
    realtime.refresh()
    assert realtime.source_generation("silver_observations") == gen, "the other tier's change doesn't rebuild this one"
    assert (realtime.hub_key("silver_observations"), deferred.hub_key("silver_observations")) == (
        "silver_observations",
        "deferred:silver_observations",
    )
