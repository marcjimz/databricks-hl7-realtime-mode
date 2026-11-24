"""hl7rt.delivery with a fake producer and an in-memory Store: a failing app's notifications are parked in order while
the healthy apps keep delivering; the drainer replays them in order once the destination is back; block mode holds
the batch until everything is acked and never raises; each app's wait for acks follows how long its acks take."""

import threading
import time
import uuid

import pytest

from hl7rt import delivery


class FakeProducer:
    def __init__(self):
        self.broken, self.deleted, self.delivered, self._queue, self.lock = set(), set(), [], [], threading.Lock()
        self.slow = {}  # topic -> seconds before its acks arrive (delivered, late)

    def __len__(self):
        with self.lock:
            return len(self._queue)

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if topic in self.deleted:  # what confluent-kafka does once metadata says the topic is gone
            raise RuntimeError('KafkaError{code=_UNKNOWN_TOPIC,val=-188,str="Unable to produce message"}')
        with self.lock:
            self._queue.append((time.monotonic() + self.slow.get(topic, 0), topic, value, on_delivery))

    def poll(self, timeout=0):
        now = time.monotonic()
        with self.lock:
            due = [e for e in self._queue if e[0] <= now]
            self._queue = [e for e in self._queue if e[0] > now]
        for _, topic, value, cb in due:
            if topic in self.broken:
                cb("UNKNOWN_TOPIC_OR_PART", None)
            else:
                self.delivered.append((topic, value))
                cb(None, None)
        time.sleep(min(timeout, 0.01))


class Tables:
    """What a Store keeps in Lakebase (B2's pending and app_delivery tables), shared by every session."""

    def __init__(self):
        self.pending, self.seq, self.states, self.locks, self.lock = [], 0, [], {}, threading.Lock()


class FakeStore:
    """delivery.Store in memory. One FakeStore is one database session: like Postgres advisory locks, its locks are
    re-entrant within the session and exclusive across sessions."""

    def __init__(self, tables=None, fail_states=False):
        self.t, self.fail_states, self.threads = tables or Tables(), fail_states, []

    def _use(self):
        self.threads.append(threading.current_thread().name)

    def park(self, msgs):
        self._use()
        with self.t.lock:
            for m in msgs:
                self.t.seq += 1
                self.t.pending.append((self.t.seq, m.app, m.topic, m.key, m.value, m.nid, m.binding))

    def head(self, app, n):
        self._use()
        with self.t.lock:
            return [(s, t, k, v, nid, b) for s, a, t, k, v, nid, b in self.t.pending if a == app][:n]

    def delete_seqs(self, seqs):
        self._use()
        with self.t.lock:
            self.t.pending = [r for r in self.t.pending if r[0] not in set(seqs)]

    def delete_nids(self, app, nids):
        self._use()
        with self.t.lock:
            self.t.pending = [r for r in self.t.pending if not (r[1] == app and r[5] in set(nids))]

    def apps_with_pending(self):
        self._use()
        with self.t.lock:
            return sorted({r[1] for r in self.t.pending})

    def set_state(self, app, state, pending):
        self._use()
        if self.fail_states:
            raise ConnectionError("Lakebase: network error")
        with self.t.lock:
            self.t.states.append((app, state, pending))

    def try_lock(self, key):
        self._use()
        with self.t.lock:
            if self.t.locks.get(key, self) is not self:
                return False
            self.t.locks[key] = self
            return True

    def unlock(self, key):
        self._use()
        with self.t.lock:
            if self.t.locks.get(key) is self:
                del self.t.locks[key]


def msgs(app, topic, n, start=0):
    return [delivery.Msg(app, topic, None, f"{app}-{i}", [], f"{app}-{i}", "b") for i in range(start, start + n)]


@pytest.fixture
def lane():
    return f"test:{uuid.uuid4().hex[:8]}"  # drainer claims are process-wide: each test has its own lane


@pytest.fixture(autouse=True)
def quick(monkeypatch):
    for name, value in (("PARK_MIN_S", 0.3), ("PARK_MAX_S", 0.3), ("PARK_COLD_S", 0.3), ("PROBE_S", 0.1),
                        ("DRAIN_MIN_S", 0.0), ("BACKOFF_MAX_S", 0.2)):  # fmt: skip
        monkeypatch.setattr(delivery, name, value)


def wait_for(cond, s=5):
    end = time.time() + s
    while time.time() < end and not cond():
        time.sleep(0.02)
    return cond()


def park(producer_for, store, lane, drain_store=None):
    return delivery.Delivery(producer_for, store, drain_store, "park", lane)


def test_a_failing_app_is_parked_in_order_and_the_healthy_app_keeps_delivering(lane):
    p, s = FakeProducer(), FakeStore()
    d = park(lambda app: p, s, lane)
    p.broken.add("hub-a")
    t = time.monotonic()
    stats = d.publish(msgs("a", "hub-a", 3) + msgs("b", "hub-b", 3))
    assert time.monotonic() - t < 1 and stats == {"delivered": 3, "parked": 3}
    assert d.state["a"] == "OPEN" and [v for _, v in p.delivered] == ["b-0", "b-1", "b-2"]

    t = time.monotonic()
    d.publish(msgs("a", "hub-a", 2, start=3) + msgs("b", "hub-b", 2, start=3))  # A open: parked without waiting
    assert time.monotonic() - t < 0.2
    assert [r[4] for r in s.t.pending] == ["a-0", "a-1", "a-2", "a-3", "a-4"]

    p.broken.clear()  # A's hub is back: the drainer replays in order, then A is back in line
    assert wait_for(lambda: d.state["a"] == "CLOSED")
    assert [v for _, v in p.delivered if v.startswith("a")] == [f"a-{i}" for i in range(5)] and not s.t.pending
    d.publish(msgs("a", "hub-a", 1, start=5))
    assert p.delivered[-1] == ("hub-a", "a-5")
    assert ("a", "OPEN", 0) in s.t.states and s.t.states[-1][:2] == ("a", "CLOSED")


def test_block_holds_the_batch_until_the_destination_is_back(lane):
    p, s = FakeProducer(), FakeStore()
    d = delivery.Delivery(lambda app: p, s, None, "block", lane)
    p.broken.add("hub-a")
    threading.Timer(1.0, p.broken.clear).start()
    t = time.monotonic()
    stats = d.publish(msgs("a", "hub-a", 4))
    assert time.monotonic() - t >= 1.0 and stats["delivered"] == 4 and stats["attempts"] >= 2
    assert sorted(v for _, v in p.delivered) == [f"a-{i}" for i in range(4)] and not s.t.pending
    assert ("a", "BLOCKED", 4) in s.t.states and d.state["a"] == "CLOSED"


def test_block_waits_out_a_deleted_hub_without_raising(lane):
    p, s = FakeProducer(), FakeStore()
    d = delivery.Delivery(lambda app: p, s, None, "block", lane)
    p.deleted.add("hub-a")
    threading.Timer(1.0, p.deleted.clear).start()  # recreated
    stats = d.publish(msgs("a", "hub-a", 3))
    assert stats["delivered"] == 3 and sorted(v for _, v in p.delivered) == ["a-0", "a-1", "a-2"]
    assert d.state["a"] == "CLOSED"


def test_block_never_raises_when_the_store_or_the_producer_fails(lane):
    p, s = FakeProducer(), FakeStore(fail_states=True)  # every state write fails (Lakebase down)
    made = []

    def producer_for(app):  # the producer can't be built at first either (a token error, say)
        made.append(app)
        if len(made) < 3:
            raise RuntimeError("no token")
        return p

    d = delivery.Delivery(producer_for, s, None, "block", lane)
    p.broken.add("hub-a")
    threading.Timer(0.8, p.broken.clear).start()
    stats = d.publish(msgs("a", "hub-a", 2))
    assert stats["delivered"] == 2 and stats["attempts"] >= 3 and d.errors > 0
    assert sorted(v for _, v in p.delivered) == ["a-0", "a-1"]


def test_park_parks_a_deleted_hub_and_drains_once_it_is_recreated(lane):
    p, s = FakeProducer(), FakeStore()
    d = park(lambda app: p, s, lane)
    p.deleted.add("hub-a")
    stats = d.publish(msgs("a", "hub-a", 2) + msgs("b", "hub-b", 2))
    assert stats == {"delivered": 2, "parked": 2} and d.state["a"] == "OPEN"
    time.sleep(0.3)  # the drainer's probes hit the refused produce and keep going
    d.publish(msgs("a", "hub-a", 1, start=2))
    p.deleted.clear()
    assert wait_for(lambda: d.state["a"] == "CLOSED")
    assert [v for _, v in p.delivered if v.startswith("a")] == ["a-0", "a-1", "a-2"] and not s.t.pending


def test_park_keeps_delivering_when_state_writes_fail(lane):
    p, s = FakeProducer(), FakeStore(fail_states=True)
    d = park(lambda app: p, s, lane)
    p.broken.add("hub-a")
    assert d.publish(msgs("a", "hub-a", 2) + msgs("b", "hub-b", 2)) == {"delivered": 2, "parked": 2}
    p.broken.clear()
    assert wait_for(lambda: d.state["a"] == "CLOSED") and not s.t.pending and d.errors > 0


def test_each_app_produces_on_its_own_producer(lane):
    pa, pb, s = FakeProducer(), FakeProducer(), FakeStore()
    d = park({"a": pa, "b": pb}.get, s, lane)
    pa.deleted.add("hub-a")  # A's producer refuses; B's is untouched
    stats = d.publish(msgs("a", "hub-a", 2) + msgs("b", "hub-b", 3))
    assert stats == {"delivered": 3, "parked": 2}
    assert [v for _, v in pb.delivered] == ["b-0", "b-1", "b-2"] and not pa.delivered


def test_a_message_parked_in_flight_and_delivered_late_is_not_sent_twice(lane):
    p, s = FakeProducer(), FakeStore()
    d = park(lambda app: p, s, lane)
    p.slow["hub-b"] = 0.6  # acks arrive after the 0.3 s wait: parked while still in flight
    stats = d.publish(msgs("b", "hub-b", 3))
    assert stats == {"delivered": 0, "parked": 3} and d.state["b"] == "OPEN"
    p.slow.clear()
    d.publish(msgs("b", "hub-b", 2, start=3))  # B open: parked behind them
    assert wait_for(lambda: d.state["b"] == "CLOSED")
    assert [v for _, v in p.delivered] == [f"b-{i}" for i in range(5)] and not s.t.pending  # each once, in order


def test_two_processes_never_replay_the_same_parked_rows(lane):
    p, tables = FakeProducer(), Tables()
    d1, d2 = park(lambda app: p, FakeStore(tables), lane), park(lambda app: p, FakeStore(tables), lane)
    p.broken.add("hub-b")
    d1.publish(msgs("b", "hub-b", 3))
    d2.publish(msgs("b", "hub-b", 3, start=3))  # another process: its own session, the same tables
    assert d1.state["b"] == d2.state["b"] == "OPEN" and len(tables.pending) == 6
    p.broken.clear()
    assert wait_for(lambda: not tables.pending)
    assert sorted(v for _, v in p.delivered) == [f"b-{i}" for i in range(6)]  # each once


def test_two_deliveries_on_one_session_never_replay_the_same_parked_rows(lane):
    """A Postgres advisory lock is re-entrant within its session: two Delivery objects sharing one drain connection
    would both get it. The process-wide claim keeps it to one drainer per (lane, app)."""
    p, drain = FakeProducer(), FakeStore()
    d1, d2 = park(lambda app: p, FakeStore(drain.t), lane, drain), park(lambda app: p, FakeStore(drain.t), lane, drain)
    p.broken.add("hub-b")
    d1.publish(msgs("b", "hub-b", 3))
    d2.publish(msgs("b", "hub-b", 3, start=3))
    p.broken.clear()
    assert wait_for(lambda: not drain.t.pending)
    time.sleep(0.3)
    assert sorted(v for _, v in p.delivered) == [f"b-{i}" for i in range(6)]  # each once


def test_a_batch_racing_the_drainer_closing_the_circuit_loses_nothing(lane, monkeypatch):
    p, s = FakeProducer(), FakeStore()
    d = park(lambda app: p, s, lane)
    d.state["b"] = "DRAINING"  # B's notifications in this batch are held back, to be parked behind the backlog
    real = d._send

    def send_while_the_drainer_closes(msgs, wait):  # pending empties and the drainer closes B meanwhile
        d.state["b"] = "CLOSED"
        return real(msgs, wait)

    monkeypatch.setattr(d, "_send", send_while_the_drainer_closes)
    d.publish(msgs("a", "hub-a", 1) + msgs("b", "hub-b", 2))
    assert wait_for(lambda: sorted(v for _, v in p.delivered) == ["a-0", "b-0", "b-1"])


def test_drainers_use_the_shared_store_and_batches_their_own(lane):
    p, batch_store = FakeProducer(), FakeStore()
    drain_store = FakeStore(batch_store.t)
    d = park(lambda app: p, batch_store, lane, drain_store)
    p.broken.update({"hub-a", "hub-b"})
    d.publish(msgs("a", "hub-a", 2) + msgs("b", "hub-b", 2))
    p.broken.clear()
    assert wait_for(lambda: d.state["a"] == d.state["b"] == "CLOSED")
    assert drain_store.threads and all(t.startswith("drain-") for t in drain_store.threads)
    assert not any(t.startswith("drain-") for t in batch_store.threads)


def test_a_restarted_hub_drains_what_the_last_one_parked_before_anything_new(lane):
    p, tables = FakeProducer(), Tables()
    old = park(lambda app: p, FakeStore(tables), lane)
    p.broken.add("hub-b")
    old.publish(msgs("b", "hub-b", 3))  # parked, then the process stops before draining
    assert len(tables.pending) == 3
    old.state["b"] = "STOPPED"  # the old process dies: its drainer exits and its session's lock goes with it
    assert wait_for(lambda: not old._drainers["b"].is_alive())
    p.broken.clear()
    new = park(lambda app: p, FakeStore(tables), lane)  # the restarted process: its circuits start CLOSED in memory
    new.resume()
    new.publish(msgs("b", "hub-b", 2, start=3))  # behind the leftovers, not ahead of them
    assert wait_for(lambda: new.state["b"] == "CLOSED" and not tables.pending)
    assert [v for _, v in p.delivered] == [f"b-{i}" for i in range(5)]


# -- how long a batch waits for an app's acks ------------------------------------------------------------------------
def test_the_wait_is_three_times_the_apps_p99_ack_time_within_bounds(monkeypatch):
    monkeypatch.undo()  # the real bounds: 2-10 s, a cold app 10 s
    assert delivery.park_after_s([]) == 10.0  # cold: no acks seen yet (token, metadata, connection still to come)
    assert delivery.park_after_s([0.05] * 200) == 2.0
    assert delivery.park_after_s([1.0] * 197 + [3.0] * 3) == 9.0  # p99 of 200 is the 198th
    assert delivery.park_after_s([4.0] * 200) == 10.0


def test_a_cold_app_is_not_parked_for_a_slow_first_ack(lane, monkeypatch):
    monkeypatch.setattr(delivery, "PARK_MIN_S", 0.2)
    monkeypatch.setattr(delivery, "PARK_MAX_S", 1.0)
    monkeypatch.setattr(delivery, "PARK_COLD_S", 1.0)
    p, s = FakeProducer(), FakeStore()
    d = park(lambda app: p, s, lane)
    p.slow["hub-new"] = 0.5  # its first acks take longer than the 0.2 s floor (a fixed wait parked every new app once)
    assert d.publish(msgs("new", "hub-new", 3)) == {"delivered": 3, "parked": 0}
    assert d.state.get("new", "CLOSED") == "CLOSED"


def test_a_slow_but_healthy_app_is_not_parked_once_its_acks_are_learned(lane, monkeypatch):
    monkeypatch.setattr(delivery, "PARK_MIN_S", 0.2)
    monkeypatch.setattr(delivery, "PARK_MAX_S", 1.0)
    monkeypatch.setattr(delivery, "PARK_COLD_S", 1.0)
    p, s = FakeProducer(), FakeStore()
    d = park(lambda app: p, s, lane)
    for i in range(3):  # fast acks: its wait drops to the floor
        assert d.publish(msgs("a", "hub-a", 5, start=5 * i)) == {"delivered": 5, "parked": 0}
    assert d.park_after("a") == 0.2
    p.slow["hub-a"] = 0.4  # now each ack takes 0.4 s, every time (scaled from 4 s): parked once, past its old wait
    assert d.publish(msgs("a", "hub-a", 2, start=15)) == {"delivered": 0, "parked": 2}
    assert wait_for(lambda: d.state["a"] == "CLOSED" and not s.t.pending)  # drained, at 0.4 s an ack
    assert d.park_after("a") == 1.0  # 3 x 0.4 s, capped
    for i in range(3):  # learned: slow, but healthy, so never parked again
        assert d.publish(msgs("a", "hub-a", 2, start=17 + 2 * i)) == {"delivered": 2, "parked": 0}
    assert sorted(v for _, v in p.delivered) == sorted(f"a-{i}" for i in range(23))


def test_producer_config_adds_acks_all_and_the_5s_guard_to_the_event_hubs_settings():
    class Credential:
        def get_token(self, scope):
            return type("T", (), {"token": f"t:{scope}", "expires_on": 123})()

    c = delivery.producer_config("ns", Credential())
    assert c["bootstrap.servers"] == "ns.servicebus.windows.net:9093" and c["sasl.mechanism"] == "OAUTHBEARER"
    assert c["acks"] == "all" and c["request.timeout.ms"] == 5000 and c["message.timeout.ms"] == 10000
    assert c["metadata.max.age.ms"] == 180000 and c["socket.keepalive.enable"] is True  # Microsoft's settings
    assert c["partitioner"] == "murmur2_random"  # the Spark sink's partitioning: same key, same partition
    assert c["oauth_cb"](None) == ("t:https://ns.servicebus.windows.net/.default", 123.0)


def test_an_unknown_mode_is_refused(lane):
    with pytest.raises(ValueError):
        delivery.Delivery(lambda app: None, FakeStore(), None, "drop", lane)
