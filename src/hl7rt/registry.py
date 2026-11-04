"""The hub's side of the subscription registry (Lakebase Postgres; schema in src/registry/registry.sql).

Read once per micro-batch: one indexed max(generation) query, and a full reload only when it moved. Writes the hub's
verdict per revision (status) and a heartbeat per source, so apps can tell a quiet stream from a stopped hub.

The hub's flows (one per gold source) run batches on concurrent threads and share this one connection, which pg8000
doesn't allow, so every call holds a lock (each is a single short query).

Authenticates as the pipeline's identity with a short-lived Lakebase database credential (no password). pg8000 is pure
Python; psycopg[binary] aborted the serverless pipeline process (native library clash).
"""

import ssl
import threading
import time
from functools import wraps

from hl7rt.subscriptions import Rule

_RULES = (
    "SELECT s.subscription_id::text, s.name, s.revision, s.enabled, s.source, s.predicate, s.fields, "
    "a.destination_topic "
    "FROM subscription_registry.subscriptions s JOIN subscription_registry.apps a ON a.app_role = s.owner"
)
_TOKEN_SECONDS = 2700  # database credentials last an hour; reconnect well before


def _locked(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return call


class Registry:
    def __init__(self, endpoint: str, database: str):
        self._endpoint, self._database = endpoint, database
        self._conn, self._opened, self._generation = None, 0.0, None
        self._enabled: list[tuple] = []
        self._disabled: list[tuple] = []
        self._reported: dict[str, tuple[int, str]] = {}
        self._lock = threading.RLock()

    def _db(self):
        if self._conn is None or time.time() - self._opened > _TOKEN_SECONDS:
            import pg8000.native
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            host = w.postgres.get_endpoint(name=self._endpoint).status.hosts.host
            token = w.postgres.generate_database_credential(endpoint=self._endpoint).token
            if self._conn is not None:
                self._conn.close()
            self._conn = pg8000.native.Connection(
                w.current_user.me().user_name, host=host, database=self._database, password=token,
                ssl_context=ssl.create_default_context(),
            )  # fmt: skip
            self._opened = time.time()
        return self._conn

    @_locked
    def refresh(self) -> bool:
        """Reload subscriptions if the registry changed since the last call. True when it did."""
        db = self._db()
        generation = db.run("SELECT coalesce(max(generation), 0) FROM subscription_registry.subscriptions")[0][0]
        if generation == self._generation:
            return False
        rows = db.run(_RULES)
        self._enabled = [r for r in rows if r[3]]
        self._disabled = [r for r in rows if not r[3]]
        self._generation = generation
        return True

    @_locked
    def rules(self, source: str) -> list[Rule]:
        return [
            Rule(subscription_id=r[0], name=r[1], revision=r[2], source=r[4], predicate=r[5], fields=list(r[6]),
                 topic=r[7])
            for r in self._enabled if r[4] == source
        ]  # fmt: skip

    @_locked
    def cancelled(self, source: str) -> list[tuple[str, int]]:
        return [(r[0], r[2]) for r in self._disabled if r[4] == source]

    @_locked
    def report(self, subscription_id: str, revision: int, state: str, error: str | None = None):
        """Record the hub's verdict for a revision (once per change)."""
        if self._reported.get(subscription_id) == (revision, state):
            return
        self._db().run(
            "INSERT INTO subscription_registry.status (subscription_id, revision, state, error) "
            "VALUES (CAST(:s AS uuid), :r, :st, :e) ON CONFLICT (subscription_id) DO UPDATE "
            "SET revision = excluded.revision, state = excluded.state, error = excluded.error, observed_at = now()",
            s=subscription_id, r=revision, st=state, e=error,
        )  # fmt: skip
        self._reported[subscription_id] = (revision, state)

    @_locked
    def heartbeat(self, source: str, batch_id: int, rules: int):
        self._db().run(
            "INSERT INTO subscription_registry.hub_heartbeat VALUES (:s, now(), :b, :n) ON CONFLICT (source) "
            "DO UPDATE SET last_batch_at = now(), batch_id = excluded.batch_id, rules = excluded.rules",
            s=source, b=batch_id, n=rules,
        )  # fmt: skip
