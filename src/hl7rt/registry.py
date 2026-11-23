"""The hub's side of the subscription registry (Lakebase Postgres; schema in src/registry/registry.sql).

One Registry is one hub pipeline's view: its hub (edp_shared or a team's own), its tier, and its lane (the tier, or
'<tier>:<sources>' when a pipeline runs only some sources). It reads the rules that run there: its hub's apps' rules
of its tier (for deferred, every app's: they run on the hub with runs_deferred) on its sources. Read once per refresh:
one query for their (max, sum, count) of generations, and when that moved, only the rules changed since (an indexed
range on generation). Every write takes a new generation; a write that committed after a later one was read, an app
moved to another hub, or a deleted row shows in the sum or count, and the client then reloads everything.

It writes only its own hub's rows: each revision's verdict (status: PLACED in the matcher, ACTIVE once a batch decided
with it, CANCELLED, FAILED), a heartbeat and progress per source, every revision as it ran it (revisions), each
batch's decisions (bindings), and its delivery state (pending, app_delivery: PendingStore). edp_shared connects as the
registry owner, which row-level security doesn't hold, so every statement here filters by hub itself; a team's hub
connects as its service principal, which row-level security holds to its own apps and rows.

Connections: the hub's flows run batches on concurrent threads, and a pg8000 connection serves one statement at a
time, so batch threads share a Pool of at most hl7.registry.connections connections; drainers share one more (a pool of
1, whose session holds their advisory locks); the session lock (one engine per hub and lane) holds its own. S7 opened
one per drainer and exhausted Lakebase. Each statement is idempotent (a read, an upsert, an insert that can't conflict
or a delete), so a connection Lakebase dropped is reopened and the statement run again once.

Authenticates as the pipeline's identity with a short-lived Lakebase database credential (no password). pg8000 is pure
Python; psycopg[binary] aborted the serverless pipeline process (native library clash).
"""

import contextlib
import json
import threading
import time
from dataclasses import dataclass

from pg8000.exceptions import InterfaceError

from hl7rt.subscriptions import SOURCES, Rule
from subscription_api.db import TOKEN_SECONDS, connect

LOOKBACK = 100  # generations re-read on each change: writes in flight together commit within a few numbers
LOCK_POLL_S = 2.0


def lane_of(tier: str, sources: list[str]) -> str:
    """A pipeline's lane: its tier when it runs every source, else '<tier>:<its sources>' (the per-source option)."""
    return tier if set(sources) == set(SOURCES) else f"{tier}:{','.join(sorted(sources))}"


class HubAlreadyRunning(RuntimeError):
    """Another engine holds this hub and lane (a second copy of the pipeline, or an old process not gone yet)."""


def _close(conn):
    with contextlib.suppress(Exception):  # already gone
        conn.close()


class Pool:
    """At most `size` connections, each running one statement at a time. opener() opens one."""

    def __init__(self, opener, size: int = 4):
        self._opener, self.size = opener, size
        self._idle: list[tuple[object, float]] = []
        self._slots = threading.BoundedSemaphore(size)
        self._lock = threading.Lock()
        self._all: list = []
        self.closed = False

    @classmethod
    def lakebase(cls, endpoint: str, database: str, size: int = 4) -> "Pool":
        return cls(lambda: connect(endpoint, database), size)

    def open(self):
        """A new connection of the pool's kind (for a session of its own: the session lock)."""
        if self.closed:  # the engine stopped: its drainers must not reconnect
            raise InterfaceError("pool closed")
        conn = self._opener()
        with self._lock:
            self._all.append(conn)
        return conn

    def _take(self):
        with self._lock:
            while self._idle:
                conn, opened = self._idle.pop()
                if time.time() - opened < TOKEN_SECONDS:  # database credentials last an hour
                    return conn, opened
                _close(conn)
        return self.open(), time.time()

    def run(self, sql: str, **params):
        with self._slots:
            conn, opened = self._take()
            try:
                try:
                    return conn.run(sql, **params)
                except InterfaceError:  # Lakebase dropped it (idle close, compute restart): reopen, run again once
                    _close(conn)
                    conn, opened = self.open(), time.time()
                    return conn.run(sql, **params)
            finally:
                with self._lock:
                    self._idle.append((conn, opened))

    def close(self):
        with self._lock:
            self.closed = True
            conns, self._all, self._idle = self._all, [], []
        for conn in conns:
            _close(conn)


@dataclass(frozen=True)
class Row:
    """One subscription as this hub runs it: its current revision as a Rule (topic: its app's own; granted: its
    template's columns, or the app's grant), whose rule it is, and whether it's enabled."""

    rule: Rule
    owner: str
    enabled: bool
    generation: int


# A subscription that names a data template is held to the template's columns (none, if the template was removed).
_COLS = (
    "s.subscription_id::text, s.name, s.revision, s.enabled, s.source, s.where_sql, s.fields, a.destination_topic, "
    "CASE WHEN s.template IS NULL THEN a.allowed_fields ELSE coalesce(t.fields, '{}') END, s.generation, s.owner"
)
_FROM = (
    "FROM subscription_registry.subscriptions s JOIN subscription_registry.apps a ON a.app_role = s.owner "
    "LEFT JOIN subscription_registry.templates t ON t.app_role = s.owner AND t.name = s.template "
    "WHERE s.tier = :tier AND subscription_registry.runs_on(a.hub, s.tier) = :hub "
    "AND s.source = ANY(CAST(:sources AS text[]))"
)
_SIGNATURE = "SELECT coalesce(max(s.generation), 0), coalesce(sum(s.generation), 0)::text, count(*) " + _FROM


def _row(r) -> Row:
    sid, name, revision, enabled, source, where, fields, topic, granted, generation, owner = r
    rule = Rule(sid, name, revision, source, where, list(fields), topic, frozenset(granted))
    return Row(rule, owner, enabled, generation)


class Registry:
    """One hub pipeline's registry: the rules of its (hub, tier) on its sources, and the rows it writes."""

    def __init__(self, pool: Pool, hub: str, tier: str = "realtime", sources: list[str] | None = None,
                 drain_pool: Pool | None = None):  # fmt: skip
        self.pool, self.hub, self.tier = pool, hub, tier
        self.sources = list(sources or SOURCES)
        self.lane = lane_of(tier, self.sources)
        self.drain_pool = drain_pool or Pool(pool._opener, 1)
        self._args = {"tier": tier, "hub": hub, "sources": self.sources}
        self._signature, self._generation, self._rows = None, 0, {}
        self._reported: set[tuple[str, int, str]] = set()
        self._captured: set[tuple[str, int]] = set()
        self._lock_conn, self._lock_key = None, f"hub:{hub}:{self.lane}"
        self._refresh = threading.Lock()

    def close(self):
        self.unlock()
        self.pool.close()
        self.drain_pool.close()

    # -- startup -----------------------------------------------------------------------------------------------------
    def check(self):
        """This hub is onboarded (and visible to this identity), and runs this tier."""
        rows = self.pool.run("SELECT runs_deferred FROM subscription_registry.hubs WHERE name = :h", h=self.hub)
        if not rows:
            raise RuntimeError(f"HUB_NOT_ONBOARDED: hub {self.hub} isn't in subscription_registry.hubs, or this "
                               "identity isn't its (make registry onboards terraform's subscriber_hubs)")  # fmt: skip
        if self.tier == "deferred" and not rows[0][0]:
            raise RuntimeError(f"TIER_NOT_RUN_HERE: every app's deferred rules run on the hub with runs_deferred "
                               f"(edp_shared), not on {self.hub}")  # fmt: skip

    def lock(self, wait_s: float = 120.0):
        """Take this hub and lane's session lock, on a connection of its own, waiting up to wait_s for a previous
        process's session to end; then HubAlreadyRunning. Two engines would decide, bind, and deliver every batch
        twice."""
        conn, deadline = self.pool.open(), time.monotonic() + wait_s
        while not conn.run("SELECT pg_try_advisory_lock(hashtextextended(:k, 0))", k=self._lock_key)[0][0]:
            if time.monotonic() >= deadline:
                _close(conn)
                raise HubAlreadyRunning(f"HUB_ALREADY_RUNNING: another engine holds {self._lock_key}")
            time.sleep(min(LOCK_POLL_S, max(0.05, wait_s / 10)))
        self._lock_conn = conn

    def keep_lock(self):
        """Called now and then: a lock whose connection Lakebase dropped went with it, so take it again (or stop:
        another engine has it now)."""
        if self._lock_conn is None:
            return
        try:
            self._lock_conn.run("SELECT 1")
        except InterfaceError:
            self._lock_conn = None
            self.lock(wait_s=0)

    def unlock(self):
        conn, self._lock_conn = self._lock_conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.run("SELECT pg_advisory_unlock(hashtextextended(:k, 0))", k=self._lock_key)
            _close(conn)

    # -- rules ---------------------------------------------------------------------------------------------------------
    def refresh(self) -> tuple[list[Row], list[str]]:
        """The rules changed since the last call (all of them the first time), and the IDs of those that no longer run
        here (their app moved to another hub, or they moved to a source or tier this pipeline doesn't run)."""
        with self._refresh:
            top, total, count = self.pool.run(_SIGNATURE, **self._args)[0]
            signature = (int(top), int(total), int(count))
            if signature == self._signature:
                return [], []
            rows = None
            if self._signature is not None:
                rows = dict(self._rows)
                delta = self.pool.run(f"SELECT {_COLS} {_FROM} AND s.generation > :g", **self._args,
                                      g=self._generation - LOOKBACK)  # fmt: skip
                rows.update({r[0]: r for r in delta})
                if sum(r[9] for r in rows.values()) != signature[1] or len(rows) != signature[2]:
                    rows = None  # committed out of order, moved, or deleted: read everything
            if rows is None:
                rows = {r[0]: r for r in self.pool.run(f"SELECT {_COLS} {_FROM}", **self._args)}
            changed = [_row(r) for sid, r in rows.items() if self._rows.get(sid) != r]
            removed = [sid for sid in self._rows if sid not in rows]
            self._rows, self._signature, self._generation = rows, signature, signature[0]
            return changed, removed

    def report(self, revisions: list[tuple[str, int]], state: str, errors: dict[str, str] | None = None):
        """This hub's verdict for each revision, once per (revision, state). errors: subscription id -> why (FAILED)."""
        new = [(s, r) for s, r in revisions if (s, r, state) not in self._reported]
        if not new:
            return
        self.pool.run(
            "INSERT INTO subscription_registry.status (subscription_id, hub, revision, state, error, observed_at) "
            "SELECT CAST(s AS uuid), :h, r, :st, e, now() FROM unnest(CAST(:s AS text[]), CAST(:r AS int[]), "
            "CAST(:e AS text[])) AS x(s, r, e) ON CONFLICT (subscription_id, hub) DO UPDATE SET "
            "revision = excluded.revision, state = excluded.state, error = excluded.error, "
            "observed_at = excluded.observed_at",
            h=self.hub, st=state, s=[s for s, _ in new], r=[r for _, r in new],
            e=[(errors or {}).get(s) for s, _ in new],
        )  # fmt: skip
        self._reported |= {(s, r, state) for s, r in new}

    def capture(self, rules: list[tuple[Rule, str]]):
        """Each revision as this hub runs it, once (the registry rewrites a rule in place): what a retried batch
        rebuilds its decisions from."""
        new = [(r, o) for r, o in rules if (r.subscription_id, r.revision) not in self._captured]
        if not new:
            return
        self.pool.run(
            "INSERT INTO subscription_registry.revisions (hub, subscription_id, revision, owner, name, source, "
            "where_sql, fields, topic, granted) SELECT :h, CAST(s AS uuid), r, o, n, src, w, string_to_array(f, ','), "
            "t, string_to_array(g, ',') FROM unnest(CAST(:s AS text[]), CAST(:r AS int[]), CAST(:o AS text[]), "
            "CAST(:n AS text[]), CAST(:src AS text[]), CAST(:w AS text[]), CAST(:f AS text[]), CAST(:t AS text[]), "
            "CAST(:g AS text[])) AS x(s, r, o, n, src, w, f, t, g) ON CONFLICT DO NOTHING",
            h=self.hub, s=[r.subscription_id for r, _ in new], r=[r.revision for r, _ in new],
            o=[o for _, o in new], n=[r.name for r, _ in new], src=[r.source for r, _ in new],
            w=[r.where for r, _ in new], f=[",".join(r.fields) for r, _ in new], t=[r.topic for r, _ in new],
            g=[",".join(sorted(r.granted)) for r, _ in new],
        )  # fmt: skip
        self._captured |= {(r.subscription_id, r.revision) for r, _ in new}

    def captured(self, revisions: dict[str, int]) -> list[tuple[Rule, str]]:
        """The captured revisions named (subscription id -> revision), as (rule, owner)."""
        if not revisions:
            return []
        rows = self.pool.run(
            "SELECT subscription_id::text, revision, owner, name, source, where_sql, fields, topic, granted "
            "FROM subscription_registry.revisions WHERE hub = :h AND (subscription_id::text, revision) IN "
            "(SELECT * FROM unnest(CAST(:s AS text[]), CAST(:r AS int[])))",
            h=self.hub, s=list(revisions), r=list(revisions.values()),
        )  # fmt: skip
        return [(Rule(sid, name, rev, source, where, list(fields), topic, frozenset(granted)), owner)
                for sid, rev, owner, name, source, where, fields, topic, granted in rows]  # fmt: skip

    # -- batch bindings ------------------------------------------------------------------------------------------------
    def bound(self, source: str, query_id: str, batch_id: int) -> dict[str, dict]:
        """This batch's bindings, if it was bound before (a retried batch): app -> its binding."""
        rows = self.pool.run(
            "SELECT app, binding_id, revisions, n, digest FROM subscription_registry.bindings WHERE hub = :h AND "
            "lane = :l AND source = :s AND query_id = :q AND batch_id = :b",
            h=self.hub, l=self.lane, s=source, q=query_id, b=batch_id,
        )  # fmt: skip
        return {app: {"binding_id": bid, "revisions": revs, "n": n, "digest": digest}
                for app, bid, revs, n, digest in rows}  # fmt: skip

    def bind(self, bindings: list[dict]):
        """Record a batch's decisions per app before anything is published (a binding is never overwritten)."""
        if not bindings:
            return
        cols = {c: [b[c] for b in bindings] for c in ("binding_id", "app", "source", "query_id", "batch_id", "n",
                                                         "digest")}  # fmt: skip
        self.pool.run(
            "INSERT INTO subscription_registry.bindings (binding_id, hub, lane, app, source, query_id, batch_id, "
            "offsets, revisions, n, digest) SELECT i, :h, :l, a, s, q, b, CAST(o AS jsonb), CAST(r AS jsonb), n, d "
            "FROM unnest(CAST(:i AS text[]), CAST(:a AS text[]), CAST(:s AS text[]), CAST(:q AS text[]), "
            "CAST(:b AS bigint[]), CAST(:o AS text[]), CAST(:r AS text[]), CAST(:n AS int[]), CAST(:d AS text[])) "
            "AS x(i, a, s, q, b, o, r, n, d) ON CONFLICT (binding_id) DO NOTHING",
            h=self.hub, l=self.lane, i=cols["binding_id"], a=cols["app"], s=cols["source"], q=cols["query_id"],
            b=cols["batch_id"], o=[json.dumps(b["offsets"]) for b in bindings],
            r=[json.dumps(b["revisions"]) for b in bindings], n=cols["n"], d=cols["digest"],
        )  # fmt: skip

    def prune(self, source: str, query_id: str, batch_id: int):
        """Keep a flow's last two batches' bindings: only the batch in progress can be retried."""
        self.pool.run(
            "DELETE FROM subscription_registry.bindings WHERE hub = :h AND lane = :l AND source = :s AND "
            "(query_id <> :q OR batch_id < :b - 1)",
            h=self.hub, l=self.lane, s=source, q=query_id, b=batch_id,
        )  # fmt: skip

    def replayed(self, binding_id: str, live_same: bool, digest_match: bool, n: int):
        self.pool.run(
            "INSERT INTO subscription_registry.replays (binding_id, hub, live_same, digest_match, n) "
            "VALUES (:b, :h, :l, :d, :n)", b=binding_id, h=self.hub, l=live_same, d=digest_match, n=n,
        )  # fmt: skip

    def prune_revisions(self, older_than_s: int = 3600):
        """Drop captured revisions that a newer one replaced long ago and no binding names."""
        self.pool.run(
            "DELETE FROM subscription_registry.revisions r WHERE hub = :h "
            "AND captured_at < clock_timestamp() - make_interval(secs => :age) "
            "AND EXISTS (SELECT 1 FROM subscription_registry.revisions n WHERE n.hub = r.hub "
            "AND n.subscription_id = r.subscription_id AND n.revision > r.revision) "
            "AND NOT EXISTS (SELECT 1 FROM subscription_registry.bindings b WHERE b.hub = r.hub "
            "AND b.revisions ->> r.subscription_id::text = r.revision::text)",
            h=self.hub, age=older_than_s,
        )  # fmt: skip

    # -- telemetry -----------------------------------------------------------------------------------------------------
    def heartbeat(self, source: str, batch_id: int, rules: int):
        self.pool.run(
            "INSERT INTO subscription_registry.hub_heartbeat (hub, tier, source, last_batch_at, batch_id, rules) "
            "VALUES (:h, :t, :s, now(), :b, :n) ON CONFLICT (hub, tier, source) DO UPDATE SET last_batch_at = now(), "
            "batch_id = excluded.batch_id, rules = excluded.rules",
            h=self.hub, t=self.tier, s=source, b=batch_id, n=rules,
        )  # fmt: skip

    def progress(self, source: str, batch_id: int, rules: int, ms: dict[str, int], rows: int = 0, delivered: int = 0,
                 parked: int = 0, age_ms: int | None = None, spark_ms: dict | None = None):  # fmt: skip
        """Where this batch's time went (subscription_registry.hub_progress), for docs/SCALABILITY.md. A new checkpoint
        numbers batches from 0 again, so a restarted hub's batches replace older rows."""
        phases = ("refresh", "collect", "match", "bind", "deliver", "status")
        self.pool.run(
            "INSERT INTO subscription_registry.hub_progress (hub, tier, source, batch_id, at, rules, refresh_ms, "
            "compile_ms, collect_ms, match_ms, bind_ms, deliver_ms, status_ms, rows, delivered, parked, age_ms, "
            "spark_ms) VALUES (:h, :t, :s, :b, now(), :n, :refresh, :compile, :collect, :match, :bind, :deliver, "
            ":status, :rows, :delivered, :parked, :age, CAST(:spark AS jsonb)) ON CONFLICT (hub, tier, source, "
            "batch_id) DO UPDATE SET at = now(), rules = excluded.rules, refresh_ms = excluded.refresh_ms, "
            "compile_ms = excluded.compile_ms, collect_ms = excluded.collect_ms, match_ms = excluded.match_ms, "
            "bind_ms = excluded.bind_ms, deliver_ms = excluded.deliver_ms, status_ms = excluded.status_ms, "
            "rows = excluded.rows, delivered = excluded.delivered, parked = excluded.parked, "
            "age_ms = excluded.age_ms, spark_ms = excluded.spark_ms",
            h=self.hub, t=self.tier, s=source, b=batch_id, n=rules, compile=ms.get("compile", 0),
            rows=rows, delivered=delivered, parked=parked, age=age_ms,
            spark=json.dumps(spark_ms) if spark_ms else None, **{p: ms.get(p, 0) for p in phases},
        )  # fmt: skip

    # -- delivery state ------------------------------------------------------------------------------------------------
    def store(self) -> "PendingStore":
        """The batch threads' delivery Store (hl7rt.delivery.Store), on the shared pool."""
        return PendingStore(self.pool, self.hub, self.lane)

    def drain_store(self) -> "PendingStore":
        """The drainers' Store: one connection, whose session holds their locks."""
        return PendingStore(self.drain_pool, self.hub, self.lane)


class PendingStore:
    """hl7rt.delivery's Store for one hub and lane: parked notifications (pending) and each app's circuit
    (app_delivery, app_delivery_log). `app` is the app's registry role."""

    def __init__(self, pool: Pool, hub: str, lane: str):
        self.pool, self.hub, self.lane = pool, hub, lane

    def park(self, msgs):
        self.pool.run(
            "INSERT INTO subscription_registry.pending (hub, lane, app, topic, key, value, binding_id, "
            "notification_id) SELECT :h, :l, a, t, k, v, b, n FROM unnest(CAST(:a AS text[]), CAST(:t AS text[]), "
            "CAST(:k AS text[]), CAST(:v AS text[]), CAST(:b AS text[]), CAST(:n AS text[])) WITH ORDINALITY "
            "AS x(a, t, k, v, b, n, o) ORDER BY o",
            h=self.hub, l=self.lane, a=[m.app for m in msgs], t=[m.topic for m in msgs], k=[m.key for m in msgs],
            v=[m.value for m in msgs], b=[m.binding for m in msgs], n=[m.nid for m in msgs],
        )  # fmt: skip

    def head(self, app: str, n: int) -> list[tuple]:
        return [tuple(r) for r in self.pool.run(
            "SELECT seq, topic, key, value, notification_id, binding_id FROM subscription_registry.pending "
            "WHERE hub = :h AND lane = :l AND app = :a ORDER BY seq LIMIT :n", h=self.hub, l=self.lane, a=app, n=n,
        )]  # fmt: skip

    def delete_seqs(self, seqs: list[int]):
        if seqs:
            self.pool.run("DELETE FROM subscription_registry.pending WHERE hub = :h AND "
                          "seq = ANY(CAST(:s AS bigint[]))", h=self.hub, s=list(seqs))  # fmt: skip

    def delete_nids(self, app: str, nids: list[str]):
        if nids:
            self.pool.run(
                "DELETE FROM subscription_registry.pending WHERE hub = :h AND lane = :l AND app = :a AND "
                "notification_id = ANY(CAST(:n AS text[]))", h=self.hub, l=self.lane, a=app, n=list(nids),
            )  # fmt: skip

    def apps_with_pending(self) -> list[str]:
        return [a for (a,) in self.pool.run(
            "SELECT DISTINCT app FROM subscription_registry.pending WHERE hub = :h AND lane = :l ORDER BY app",
            h=self.hub, l=self.lane,
        )]  # fmt: skip

    def set_state(self, app: str, state: str, pending: int):
        self.pool.run(
            "WITH changed AS (INSERT INTO subscription_registry.app_delivery (hub, lane, app, state, pending, since) "
            "VALUES (:h, :l, :a, :st, :p, clock_timestamp()) ON CONFLICT (hub, lane, app) DO UPDATE SET "
            "state = excluded.state, pending = excluded.pending, since = excluded.since RETURNING 1) "
            "INSERT INTO subscription_registry.app_delivery_log (hub, lane, app, state, pending, at) "
            "VALUES (:h, :l, :a, :st, :p, clock_timestamp())",
            h=self.hub, l=self.lane, a=app, st=state, p=pending,
        )  # fmt: skip

    def try_lock(self, key: str) -> bool:
        return bool(self.pool.run("SELECT pg_try_advisory_lock(hashtextextended(:k, 0))", k=key)[0][0])

    def unlock(self, key: str):
        self.pool.run("SELECT pg_advisory_unlock(hashtextextended(:k, 0))", k=key)
