"""The hub's registry client is shared by one flow per gold source, whose batches run on concurrent threads. pg8000
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

    sources = ("gold_encounters", "gold_observations", "gold_orders")
    threads = [threading.Thread(target=flow, args=(s,)) for s in sources]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert conn.overlaps == 0
