"""Publishing a hub batch's notifications to each app's own topic, and what happens when an app's destination fails.

Two modes, per hub pipeline (hl7.subscriptions.on_failure):
- park (the default): each app has a circuit. A CLOSED app's notifications are produced and waited on, for at most its
  own park_after (below); an app with any failure or unacked message opens, and its undelivered notifications are
  parked in the Store, in order, before the batch returns. An OPEN or DRAINING app's notifications go straight to the
  Store. A drainer per app probes every PROBE_S, then replays what's parked in order and closes the circuit when
  nothing is left. One app's broken destination never holds up another app's notifications.
- block: retry until every notification is acked, however long the destination is down. The batch doesn't return, so
  its offsets aren't committed: the pipeline's checkpoint holds what's undelivered. It never raises (store errors
  included): SDP restarts a failed flow at most twice before it fails the update.

How long a batch waits for an app's acks follows that app: 3x the p99 of its last ACK_SAMPLES ack times, within
PARK_MIN_S..PARK_MAX_S, and PARK_COLD_S before it has any (a new app's first acks include its token, metadata and
connection: with a fixed 2 s, S7 parked every new app once). A slow but healthy destination isn't parked for being slow.

Each app has its own producer (producer_for), so a destination that fails at the connection level (a deleted hub) can't
stall another app's acks. Delivery reports are waited on per batch (poll), never with flush(), which would also wait
for another app's stuck messages. A message parked while still in flight and delivered after all removes its parked
copy (late acks, flushed by the drainer: a delivery callback runs inside any poll, so it never does I/O).

The Store is the hub's (hl7rt.registry, over Lakebase); this module runs no SQL.
"""

import collections
import math
import threading
import time
from typing import Protocol

from hl7rt import eventhubs

PARK_MIN_S, PARK_MAX_S, PARK_COLD_S = 2.0, 10.0, 10.0  # how long a CLOSED app's batch may wait for its acks
PARK_FACTOR, ACK_SAMPLES = 3.0, 200
PROBE_S = 5.0  # an OPEN app's drainer tries one parked notification this often
DRAIN_CHUNK, DRAIN_MIN_S = 500, 0.5  # ~1,000 notifications/s into a recovering destination
BACKOFF_MAX_S = 5.0  # block mode's retry backoff
SEND_WAIT_S = 12.0  # replays and block mode: longer than message.timeout.ms, so every message has reported by then
SETTLE_S = 15.0  # before a replay: let the app's in-flight messages report (their late acks remove parked copies)


def producer_config(namespace: str, credential) -> dict:
    """confluent-kafka settings for the hub's writes: Event Hubs' (eventhubs.producer_config, Microsoft's client
    guidance), plus acks from every replica, the Spark Kafka sink's partitioning (Java's murmur2: a patient's
    notifications land on the same partition as before), and the 5 s hung-request guard (eventhubs.HUNG_REQUEST_GUARD)
    with a 10 s cap on a message's whole delivery. `credential`: an azure-identity TokenCredential, e.g.
    dbutils.credentials.getServiceCredentialsProvider(<the hub's service credential>)."""
    return {
        **eventhubs.producer_config(namespace, credential),
        "acks": "all",
        "partitioner": "murmur2_random",
        "request.timeout.ms": int(eventhubs.HUNG_REQUEST_GUARD["kafka.request.timeout.ms"]),
        "message.timeout.ms": 10000,
        "queue.buffering.max.messages": 1_000_000,
    }


def park_after_s(ack_times) -> float:
    """How long a batch waits for one app's acks, from its recent ack times (seconds)."""
    if not ack_times:
        return PARK_COLD_S
    s = sorted(ack_times)
    p99 = s[max(0, math.ceil(0.99 * len(s)) - 1)]
    return min(PARK_MAX_S, max(PARK_MIN_S, PARK_FACTOR * p99))


class Msg:
    """One notification for one app: its topic, Kafka key and value, headers, notification ID, and the binding (the
    batch decision) it came from."""

    __slots__ = ("app", "binding", "headers", "key", "nid", "topic", "value")

    def __init__(self, app, topic, key, value, headers, nid, binding):
        self.app, self.topic, self.key, self.value = app, topic, key, value
        self.headers, self.nid, self.binding = headers, nid, binding


class Store(Protocol):
    """Where a hub parks notifications and records its circuits, for one (hub, lane). One Store is one database
    session (its locks are that session's)."""

    def park(self, msgs: list[Msg]) -> None:
        """Append, in order."""

    def head(self, app: str, n: int) -> list[tuple]:
        """The app's first n parked notifications, in order: (seq, topic, key, value, notification_id, binding_id)."""

    def delete_seqs(self, seqs: list[int]) -> None: ...

    def delete_nids(self, app: str, nids: list[str]) -> None: ...

    def apps_with_pending(self) -> list[str]: ...

    def set_state(self, app: str, state: str, pending: int) -> None:
        """Record a circuit change (CLOSED | OPEN | DRAINING | BLOCKED)."""

    def try_lock(self, key: str) -> bool:
        """A session lock (pg_try_advisory_lock): true if this session holds it now."""

    def unlock(self, key: str) -> None: ...


# Drainers claimed in this process. A Postgres advisory lock is re-entrant within its session, so two Delivery objects
# sharing one drain connection would both get it: the claim keeps it to one drainer per (lane, app) per process, and the
# Store's lock to one across processes.
_CLAIMS: set[str] = set()
_CLAIMS_LOCK = threading.Lock()


class _Acks:
    """One send's delivery reports. Once the sender stops waiting (close), a report that still arrives goes to the
    handler set with then(): a message parked while in flight that was delivered after all."""

    def __init__(self, apps: list[str], on_ack=None):
        n = len(apps)
        self.apps, self.ok, self.err, self.sent = apps, [False] * n, [None] * n, [0.0] * n
        self.left = collections.Counter(apps)  # app -> messages not reported yet
        self.lock, self.closed, self.late, self.on_late, self.on_ack = threading.Lock(), False, [], None, on_ack

    def cb(self, i):
        def done(err, _msg):
            if err is None and self.on_ack is not None:  # in time or late: how long this destination takes
                self.on_ack(self.apps[i], time.monotonic() - self.sent[i])
            with self.lock:
                if not self.closed:
                    self.ok[i], self.err[i] = err is None, (str(err) if err else None)
                    self.left[self.apps[i]] -= 1
                    return
                if self.on_late is None:
                    self.late.append((i, err is None))
                    return
                handler = self.on_late
            handler([(i, err is None)])

        return done

    def waiting(self) -> list[str]:
        with self.lock:
            return [a for a, n in self.left.items() if n > 0]

    def close(self) -> "_Acks":
        with self.lock:
            self.closed = True
        return self

    def then(self, handler):
        with self.lock:
            self.on_late, buffered, self.late = handler, self.late, []
        if buffered:
            handler(buffered)


class Delivery:
    def __init__(self, producer_for, store: Store, drain_store: Store | None, mode: str, lane_key: str,
                 replay_headers=None):  # fmt: skip
        """producer_for(app) -> that app's producer. store: the batch threads' Store; drain_store: the one every
        drainer thread shares (one connection per process), or store. lane_key: '<hub>:<lane>', which the drainer
        claims are keyed by. replay_headers(binding_id) -> the headers of a replayed notification (none by default)."""
        if mode not in ("park", "block"):
            raise ValueError(f"on_failure must be park or block, not {mode!r}")
        self.producer_for, self.store, self.drain_store = producer_for, store, drain_store or store
        self.mode, self.lane_key = mode, lane_key
        self.replay_headers = replay_headers or (lambda binding: [])
        self.state: dict[str, str] = {}  # app -> CLOSED | OPEN | DRAINING (park) / CLOSED | BLOCKED (block)
        self.locks: dict[str, threading.Lock] = {}
        self.errors = 0  # circuit changes the Store failed to record (the circuit in memory is the one that counts)
        self._drainers: dict[str, threading.Thread] = {}
        self._late: dict[str, list[tuple[str, object]]] = {}  # app -> parked copies delivered late: ("nid"|"seq", id)
        self._acked: dict[str, collections.deque] = {}  # app -> its recent ack times (s)
        self._meta, self._timing = threading.Lock(), threading.Lock()

    def _st(self) -> Store:
        """Drainer threads share one Store (one connection); batch threads use their own."""
        return self.drain_store if threading.current_thread().name.startswith("drain-") else self.store

    def _lock(self, app):
        with self._meta:
            return self.locks.setdefault(app, threading.Lock())

    def _set(self, app, state, pending=0):
        if self.state.get(app) != state:
            self.state[app] = state
            try:
                self._st().set_state(app, state, pending)
            except Exception:  # noqa: BLE001 - a record of the change, not the circuit: never fail a batch for it
                self.errors += 1

    def _acked_in(self, app, seconds):
        with self._timing:
            self._acked.setdefault(app, collections.deque(maxlen=ACK_SAMPLES)).append(seconds)

    def park_after(self, app: str) -> float:
        """How long the next batch waits for this app's acks before parking them."""
        with self._timing:
            return park_after_s(list(self._acked.get(app, ())))

    def _send(self, msgs: list[Msg], wait) -> _Acks:
        """Produce msgs and wait for their reports: app -> seconds (or the same for every app), from now. A producer
        that can't be built or a produce that's refused (a deleted topic: _UNKNOWN_TOPIC) is a failed report."""
        started = time.monotonic()
        acks, producers = _Acks([m.app for m in msgs], self._acked_in), {}
        for i, m in enumerate(msgs):
            if m.app not in producers:
                try:
                    producers[m.app] = self.producer_for(m.app)
                except Exception as e:  # noqa: BLE001 - not sent: this app's messages fail, the others go on
                    producers[m.app] = e
            p = producers[m.app]
            if isinstance(p, Exception):
                acks.cb(i)(p, None)
                continue
            acks.sent[i] = time.monotonic()
            while True:
                try:
                    p.produce(m.topic, key=m.key, value=m.value, headers=m.headers, on_delivery=acks.cb(i))
                    break
                except BufferError:
                    p.poll(0.05)
                except Exception as e:  # noqa: BLE001 - refused at produce: not sent
                    acks.cb(i)(e, None)
                    break
        deadline = {a: started + (wait[a] if isinstance(wait, dict) else wait) for a in producers}
        while True:
            now = time.monotonic()
            # each app's producer until that app has reported or its wait is over (one producer may serve several)
            polling = {id(producers[a]): producers[a] for a in acks.waiting() if now < deadline[a]}
            if not polling:
                break
            for p in polling.values():
                p.poll(0.02 / len(polling))
        return acks.close()

    def resume(self):
        """At startup: an app with notifications still parked (a previous process stopped before draining them) starts
        DRAINING, so they go out first and new ones queue behind them."""
        if self.mode != "park":
            return
        for app in self.store.apps_with_pending():
            with self._lock(app):
                self._set(app, "DRAINING")
            self._drain(app)

    def publish(self, msgs: list[Msg]) -> dict:
        """Deliver (or park) a batch's notifications. park: {delivered, parked}; block: {delivered, attempts}."""
        if not msgs:
            return {}
        return self._block(msgs) if self.mode == "block" else self._park(msgs)

    # -- block ----------------------------------------------------------------------------------------------------
    def _block(self, msgs):
        backoff, todo, attempts = 0.5, msgs, 0
        while True:
            attempts += 1
            try:
                acks = self._send(todo, SEND_WAIT_S)
                failed = [m for m, ok in zip(todo, acks.ok, strict=True) if not ok]
            except Exception:  # noqa: BLE001 - never raise: retry everything this attempt sent
                failed = todo
            if not failed:
                for app in {m.app for m in msgs}:
                    self._set(app, "CLOSED")
                return {"delivered": len(msgs), "attempts": attempts}
            for app in {m.app for m in failed}:
                self._set(app, "BLOCKED", sum(m.app == app for m in failed))
            time.sleep(backoff)
            backoff, todo = min(BACKOFF_MAX_S, backoff * 2), failed

    # -- park -----------------------------------------------------------------------------------------------------
    def _park(self, msgs):
        apps = sorted({m.app for m in msgs})
        live = [m for m in msgs if self.state.get(m.app, "CLOSED") == "CLOSED"]
        sent = {id(m) for m in live}
        acks = self._send(live, {a: self.park_after(a) for a in {m.app for m in live}}) if live else _Acks([])
        undelivered = {m.nid for m, ok in zip(live, acks.ok, strict=True) if not ok}
        failing = {m.app for m in live if m.nid in undelivered}
        stats = {"delivered": len(live) - len(undelivered), "parked": 0}
        for app in apps:
            with self._lock(app):  # a drainer closing the circuit checks pending under the same lock
                if app in failing:
                    self._set(app, "OPEN")
                # Whatever this batch didn't deliver is parked, whatever the state is now: the drainer may have
                # closed the circuit since `live` was taken, and those notifications were never sent.
                park = [m for m in msgs if m.app == app and (m.nid in undelivered or id(m) not in sent)]
                if not park:
                    continue
                self._st().park(park)
                stats["parked"] += len(park)
                if self.state.get(app, "CLOSED") == "CLOSED":
                    self._set(app, "DRAINING")  # the drainer closed meanwhile: reopen, so they go out (in order)
            self._drain(app)
        if undelivered:  # still in flight when parked: a late success removes its parked copy (the drainer does)
            acks.then(lambda reports: self._note_late([(live[i].app, "nid", live[i].nid) for i, ok in reports if ok]))
        return stats

    def _note_late(self, items):
        """Called from delivery callbacks, which run inside any poll (a batch's too): record only, no I/O."""
        with self._meta:
            for app, kind, ident in items:
                self._late.setdefault(app, []).append((kind, ident))

    def _flush_late(self, app):
        with self._meta:
            items = self._late.pop(app, [])
        nids, seqs = [i for k, i in items if k == "nid"], [i for k, i in items if k == "seq"]
        if nids:
            self._st().delete_nids(app, nids)
        if seqs:
            self._st().delete_seqs(seqs)

    def _settled(self, app, limit=None):
        """Wait until the app's producer has nothing in flight, so its late reports are in before a replay; then remove
        the parked copies those reports delivered."""
        try:
            p, end = self.producer_for(app), time.monotonic() + (SETTLE_S if limit is None else limit)
            while len(p) and time.monotonic() < end:
                p.poll(0.05)
        except Exception:  # noqa: BLE001 - no producer: nothing in flight to wait for
            pass
        self._flush_late(app)

    def _drain(self, app):
        with self._meta:
            t = self._drainers.get(app)
            if t is not None and t.is_alive():
                return
            t = threading.Thread(target=self._drainer, args=(app,), daemon=True, name=f"drain-{app}")
            self._drainers[app] = t
        t.start()

    def _claim(self, key) -> bool:
        with _CLAIMS_LOCK:
            if key in _CLAIMS:
                return False
            _CLAIMS.add(key)
        try:
            if self._st().try_lock(key):
                return True
        except Exception:  # noqa: BLE001 - no lock, no drain: the next batch that parks tries again
            pass
        with _CLAIMS_LOCK:
            _CLAIMS.discard(key)
        return False

    def _release(self, key):
        try:
            self._st().unlock(key)
        finally:
            with _CLAIMS_LOCK:
                _CLAIMS.discard(key)

    def _drainer(self, app):
        """One drainer per (lane, app) anywhere, so two Delivery objects (or processes) never replay the same parked
        notifications."""
        key = f"drain:{self.lane_key}:{app}"
        try:
            if not self._claim(key):
                return
            try:
                self._drain_loop(app)
            finally:
                self._release(key)
        except Exception:  # noqa: BLE001 - the next batch that parks starts another drainer; the circuit stays as it was
            return

    def _drain_loop(self, app):
        while True:
            state = self.state.get(app)
            if state == "OPEN":
                time.sleep(PROBE_S)
                self._settled(app)
                head = self._st().head(app, 1)
                if not head:
                    with self._lock(app):
                        self._set(app, "CLOSED")
                    return
                if self._replay(app, head):
                    self._set(app, "DRAINING")
                continue
            if state == "DRAINING":
                t0 = time.monotonic()
                self._settled(app)
                head = self._st().head(app, DRAIN_CHUNK)
                if not head:
                    with self._lock(app):
                        if not self._st().head(app, 1):  # nothing parked since: back in line
                            self._set(app, "CLOSED")
                            return
                    continue
                if not self._replay(app, head):
                    self._set(app, "OPEN")
                time.sleep(max(0.0, DRAIN_MIN_S - (time.monotonic() - t0)))
                continue
            return

    def _replay(self, app, rows) -> bool:
        msgs = [Msg(app, t, k, v, self.replay_headers(b), n, b) for _, t, k, v, n, b in rows]
        acks = self._send(msgs, SEND_WAIT_S)
        done = [seq for (seq, *_), ok in zip(rows, acks.ok, strict=True) if ok]
        if done:
            self._st().delete_seqs(done)
        if len(done) < len(rows):  # a replay delivered after the wait: don't replay it again
            acks.then(lambda reports: self._note_late([(app, "seq", rows[i][0]) for i, ok in reports if ok]))
        return len(done) == len(rows)
