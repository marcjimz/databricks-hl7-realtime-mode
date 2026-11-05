"""Subscription API: the one way apps register subscriptions. A thin facade; Databricks does the compiling.

  PUT    /api/v1/subscriptions          the app's full set [{name, sql}]: atomic (any refusal applies nothing), cancels
                                        what's no longer declared
  PUT    /api/v1/subscriptions/{name}   one subscription {sql}
  DELETE /api/v1/subscriptions/{name}   cancel one
  GET    /api/v1/subscriptions?wait=N   the app's subscriptions and the hub's state; with wait, until the hub runs
                                        exactly these revisions (READY), can't (FAILED), or isn't processing (HUB_STALE)
  POST   /api/v1/validate               the same checks as PUT, applying nothing
  GET    /api/v1/health

Per rule: the shape check (rules.py), then Databricks compiles it with DESCRIBE QUERY against the caller's own views
(<catalog>.sub_<app>), so its errors (UNRESOLVED_COLUMN, PARSE_SYNTAX_ERROR, TABLE_OR_VIEW_NOT_FOUND, ...) pass through
unchanged. The caller is the identity the Databricks Apps proxy verified (X-Forwarded-Email = the service principal's
application ID; a client can't forge it). Writes run as that app's own registry role (SET LOCAL ROLE), so the registry's
trigger and row-level security apply as if the app wrote them itself.

Stateless: the registry is the only state, so any instance can serve any call (horizontal scaling).
"""

import os
import re
import threading
import time

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

try:
    from subscription_api import db, rules  # tests and the hub (src/ on the path)
except ImportError:  # the deployed app: its folder is the root
    import db
    import rules

CATALOG = os.environ.get("HL7_CATALOG", "hl7_rtm_demo")
ENDPOINT = os.environ.get("HL7_REGISTRY_ENDPOINT", "")
DATABASE = os.environ.get("HL7_REGISTRY_DATABASE", "databricks_postgres")
WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")  # = the registry's CHECK on subscriptions.name
HUB_STALE_AFTER = 120  # seconds without a hub batch for a source
MAX_WAIT = 60  # per call; clients poll (a proxy in between may cut long requests)

app = FastAPI(title="HL7 subscriptions", version="1")


class Item(BaseModel):
    name: str
    sql: str


class Declared(BaseModel):
    subscriptions: list[Item]


class One(BaseModel):
    sql: str


class Refused(Exception):
    def __init__(self, status: int, body: dict):
        self.status, self.body = status, body


@app.exception_handler(Refused)
def _refused(_: Request, e: Refused):
    return JSONResponse(status_code=e.status, content=e.body)


# ---------------------------------------------------------------------------------------------------------------------
# Registry connections: a small pool per instance, renewed before the database credential expires.
class Pool:
    def __init__(self, connect):
        self._connect, self._idle, self._lock = connect, [], threading.Lock()

    def get(self):
        with self._lock:
            while self._idle:
                conn, opened = self._idle.pop()
                if time.time() - opened < db.TOKEN_SECONDS:
                    return conn, opened
                conn.close()
        return self._connect(), time.time()

    def put(self, conn, opened):
        with self._lock:
            self._idle.append((conn, opened))


pool = Pool(lambda: db.connect(ENDPOINT, DATABASE))


class Session:
    """One registry transaction as the calling app's own role."""

    def __init__(self, role: str):
        self.role = role

    def __enter__(self):
        self.conn, self.opened = pool.get()
        self.conn.run("BEGIN")
        self.conn.run(f'SET LOCAL ROLE "{self.role}"')
        return self.conn

    def __exit__(self, exc_type, *_):
        try:
            self.conn.run("ROLLBACK" if exc_type else "COMMIT")
            pool.put(self.conn, self.opened)
        except Exception:  # noqa: BLE001 - a broken connection isn't returned to the pool
            self.conn.close()


def _caller(email: str | None) -> dict:
    if not email:
        raise Refused(401, {"code": "UNAUTHENTICATED", "message": "no verified caller"})
    conn, opened = pool.get()
    try:
        rows = conn.run("SELECT app_role, app_name FROM subscription_registry.apps WHERE service_principal = :sp",
                        sp=email)  # fmt: skip
    finally:
        pool.put(conn, opened)
    if not rows:
        raise Refused(403, {"code": "APP_NOT_ONBOARDED", "message": f"{email} isn't an onboarded subscriber app"})
    role, name = rows[0]
    return {"role": role, "name": name, "schema": f"{CATALOG}.sub_{name.replace('-', '_')}"}


# ---------------------------------------------------------------------------------------------------------------------
# Databricks compiles each rule against the caller's own views; its error passes through unchanged.
_ERROR_CLASS = re.compile(r"\[([A-Z_][A-Z0-9_.]*)\]")


def compile_with_databricks(sql: str, schema: str) -> dict | None:
    """None if Databricks compiles `sql` with `schema` as the default, else {code, message} from Databricks."""
    catalog, sch = schema.split(".", 1)
    w = WorkspaceClient()
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=f"DESCRIBE QUERY {sql}",
                                                catalog=catalog, schema=sch, wait_timeout="50s")  # fmt: skip
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value == "SUCCEEDED":
        return None
    message = (r.status.error.message if r.status.error else r.status.state.value).strip()
    m = _ERROR_CLASS.search(message)
    return {"code": m.group(1) if m else "DATABRICKS_ERROR", "message": message}


def _check(item: Item, who: dict, registered: dict) -> dict:
    """{name, ok: True, rule} or {name, ok: False, code, message} (shape check, then Databricks)."""
    try:
        rule = rules.parse(item.sql, rules.SOURCES, who["schema"])
    except rules.RuleError as e:
        if e.code != "PARSE":
            return {"name": item.name, "ok": False, "code": e.code, "message": e.message}
        error = compile_with_databricks(item.sql, who["schema"])  # Databricks' own words for why it doesn't parse
        return {"name": item.name, "ok": False, **(error or {"code": "SQL_NOT_ALLOWED", "message": e.message})}
    same = registered.get(item.name)
    if not (same and same["enabled"] and same["query"] == rule.query):  # unchanged rules were compiled before
        error = compile_with_databricks(rule.query, who["schema"])
        if error:
            return {"name": item.name, "ok": False, **error}
    return {"name": item.name, "ok": True, "rule": rule}


def _registered(conn) -> dict:
    listing = conn.run("SELECT subscription_api.list_subscriptions()")[0][0]
    return {s["name"]: s for s in listing["subscriptions"]}


def _apply(conn, checked: list[dict], registered: dict) -> list[dict]:
    out = []
    for c in checked:
        r = c["rule"]
        try:
            receipt = conn.run("SELECT subscription_api.subscribe(:n, :s, :q, :w, CAST(:f AS text[]))", n=c["name"],
                               s=r.source, q=r.query, w=r.where, f=list(r.fields))[0][0]  # fmt: skip
        except Exception as e:  # noqa: BLE001 - a registry domain error (RULE_LIMIT_REACHED, ...): refuse it all
            message = _registry_message(e)
            raise Refused(
                422,
                {"applied": False, "refused": [{"name": c["name"], "code": message.split(":")[0], "message": message}]},
            ) from None
        before = registered.get(c["name"])
        out.append(receipt | {"unchanged": bool(before and before["enabled"] and
                                                before["revision"] == receipt["revision"])})  # fmt: skip
    return out


def _registry_message(e: Exception) -> str:
    return e.args[0].get("M", str(e)) if e.args and isinstance(e.args[0], dict) else str(e)


def _validate(items: list[Item], who: dict, registered: dict) -> list[dict]:
    names = [i.name for i in items]
    if len(names) != len(set(names)):
        raise Refused(422, {"code": "DUPLICATE_NAME", "message": "each subscription name once"})
    bad = [n for n in names if not NAME.fullmatch(n)]
    if bad:
        raise Refused(
            422,
            {
                "applied": False,
                "refused": [
                    {"name": n, "code": "INVALID_NAME", "message": "a name is 1-63 lowercase letters, digits, or -"}
                    for n in bad
                ],
            },
        )
    return [_check(i, who, registered) for i in items]


def _refusals(checked):
    return [{k: v for k, v in c.items() if k != "ok"} for c in checked if not c["ok"]]


# ---------------------------------------------------------------------------------------------------------------------
@app.get("/api/v1/health")
def health():
    return {"ok": True}


@app.post("/api/v1/validate")
def validate(body: Declared, x_forwarded_email: str | None = Header(default=None)):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        checked = _validate(body.subscriptions, who, _registered(conn))
    return {"valid": all(c["ok"] for c in checked), "refused": _refusals(checked),
            "accepted": [{"name": c["name"], "query": c["rule"].query} for c in checked if c["ok"]]}  # fmt: skip


@app.put("/api/v1/subscriptions")
def declare(body: Declared, x_forwarded_email: str | None = Header(default=None)):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        registered = _registered(conn)
        checked = _validate(body.subscriptions, who, registered)
        if not all(c["ok"] for c in checked):
            raise Refused(422, {"applied": False, "refused": _refusals(checked)})
        accepted = _apply(conn, checked, registered)
        keep = {c["name"] for c in checked}
        cancelled = []
        for name, s in registered.items():
            if s["enabled"] and name not in keep:
                cancelled.append(conn.run("SELECT subscription_api.unsubscribe(:n)", n=name)[0][0])
    return {"applied": True, "accepted": accepted, "cancelled": cancelled}


@app.put("/api/v1/subscriptions/{name}")
def put_one(name: str, body: One, x_forwarded_email: str | None = Header(default=None)):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        registered = _registered(conn)
        checked = _validate([Item(name=name, sql=body.sql)], who, registered)
        if not checked[0]["ok"]:
            raise Refused(422, {"applied": False, "refused": _refusals(checked)})
        return _apply(conn, checked, registered)[0]


@app.delete("/api/v1/subscriptions/{name}")
def cancel(name: str, x_forwarded_email: str | None = Header(default=None)):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        try:
            return conn.run("SELECT subscription_api.unsubscribe(:n)", n=name)[0][0]
        except Exception as e:  # noqa: BLE001 - the registry's domain error
            message = _registry_message(e)
            raise Refused(404, {"code": message.split(":")[0], "message": message}) from None


def _state(listing: dict) -> dict:
    subs = [s for s in listing["subscriptions"] if s["enabled"]]
    failed = [f"{s['name']}: {s['error']}" for s in subs if s["state"] == "FAILED"]
    pending = [s["name"] for s in subs if s["state"] != "ACTIVE"]
    now = time.time()
    stale = sorted({s["source"] for s in subs} - {src for src, at in listing["hub"].items()
                                                   if now - _epoch(at) < HUB_STALE_AFTER})  # fmt: skip
    if failed:
        return {"state": "FAILED", "problems": failed}
    if pending:
        return {"state": "PENDING", "problems": pending}
    if stale:
        return {
            "state": "HUB_STALE",
            "problems": [f"the hub hasn't processed {s} for {HUB_STALE_AFTER} s" for s in stale],
        }
    return {"state": "READY", "problems": []}


def _epoch(ts: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(ts).timestamp()


@app.get("/api/v1/subscriptions")
def status(wait: int = 0, x_forwarded_email: str | None = Header(default=None)):
    who = _caller(x_forwarded_email)
    deadline = time.time() + min(max(wait, 0), MAX_WAIT)
    while True:
        with Session(who["role"]) as conn:
            listing = conn.run("SELECT subscription_api.list_subscriptions()")[0][0]
        state = _state(listing)
        if state["state"] in ("READY", "FAILED") or time.time() >= deadline:
            return {"app": who["name"], **state, **listing}
        time.sleep(1)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", "8000"))))
