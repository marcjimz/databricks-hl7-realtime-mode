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
            return [[0]] if "max(generation)" in sql else []
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
            return [[0]] if "max(generation)" in sql else []

        def close(self):
            pass

    opened = [Dropped(), Fine()]
    monkeypatch.setattr(registry_module, "connect", lambda endpoint, database: opened.pop(0))
    registry = Registry("projects/p/branches/production/endpoints/primary", "db")

    registry.refresh()
    registry.heartbeat("silver_observations", 1, 0)
    assert opened == [], "the dropped connection was replaced by a new one"
