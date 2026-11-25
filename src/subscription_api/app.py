"""Subscription API: the one way apps register subscriptions. A thin facade; Databricks does the compiling.

  PUT    /api/v1/subscriptions          the app's full set [{name, sql}]: atomic (any refusal applies nothing), cancels
                                        what's no longer declared
  PUT    /api/v1/subscriptions/{name}   one subscription {sql}
  DELETE /api/v1/subscriptions/{name}   cancel one
  GET    /api/v1/subscriptions?wait=N   the app's subscriptions and the hub's state; with wait, until the hub runs
                                        exactly these revisions (READY), can't (FAILED), or isn't processing (HUB_STALE)
  POST   /api/v1/validate               the same checks as PUT, applying nothing
  GET    /api/v1/connection             the app's topic, hub, grant, and how to read the topic
  GET    /api/v1/health

The same calls whichever hub runs an app's rules (the platform assigns it: edp_shared, or a team's own hub): the
registry says which hub runs each rule, and only that hub's verdict and heartbeat count.

Per rule: the shape check (rules.py), then Databricks compiles it with DESCRIBE QUERY against the caller's own views
(<catalog>.sub_<app>), so its errors (UNRESOLVED_COLUMN, PARSE_SYNTAX_ERROR, TABLE_OR_VIEW_NOT_FOUND, ...) pass through
unchanged. The caller is the identity the Databricks Apps proxy verified (X-Forwarded-Email = the service principal's
application ID; a client can't forge it). Writes run as that app's own registry role (SET LOCAL ROLE), so the registry's
trigger and row-level security apply as if the app wrote them itself.

Stateless: the registry is the only state, so any instance can serve any call (horizontal scaling).
"""

import contextlib
import os
import re
import threading
import time
from typing import Any, Literal

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Header, Query, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pg8000.exceptions import InterfaceError
from pydantic import BaseModel, ConfigDict, Field

try:
    from subscription_api import db, rules  # tests and the hub (src/ on the path)
except ImportError:  # the deployed app: its folder is the root
    import db
    import rules

CATALOG = os.environ.get("HL7_CATALOG", "hl7_rtm_demo")
ENDPOINT = os.environ.get("HL7_REGISTRY_ENDPOINT", "")
DATABASE = os.environ.get("HL7_REGISTRY_DATABASE", "databricks_postgres")
WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
EVENTHUBS_NAMESPACE = os.environ.get("HL7_EVENTHUBS_NAMESPACE", "")  # where every app's topic lives
# The sources rules may name: the Delta tables the hub reads instead of the silver topics (HL7_SOURCE_TABLES = its
# hl7.subscriptions.tables), else the silver sources. Each app may still use only the ones it's granted.
SOURCES = rules.sources(os.environ.get("HL7_SOURCE_TABLES", ""))
NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")  # = the registry's CHECK on subscriptions.name
HUB_STALE_AFTER = 120  # seconds without a hub batch for a source
DEFERRED_INTERVAL = int(os.environ.get("HL7_HUB_DEFERRED_INTERVAL_SECONDS", "60"))  # the deferred hub's trigger
RUNNING = ("PLACED", "ACTIVE")  # the hub's verdicts that mean it runs the revision
MAX_WAIT = 60  # per call; clients poll (a proxy in between may cut long requests)
WAIT_POLL = 0.25  # seconds between registry reads while a call waits (each is part of edit -> ACTIVE)

DESCRIPTION = """
The contract any application uses to receive HL7 records it chooses with Databricks SQL. The full guide, with
examples: docs/integration.md in the repo.

**Authentication.** Call with `Authorization: Bearer <token>`. The token is your app's Databricks service principal's
OAuth token (machine-to-machine, from `https://<workspace>/oidc/v1/token`). The Databricks Apps proxy verifies it and
tells the API who is calling. The service principal must be an onboarded subscriber app.

**The handshake.** An app needs only its service principal and this API's Databricks App name (its URL is
`GET https://<workspace>/api/2.0/apps/<name>` → `url`, which your CAN_USE grant allows).
0. `GET /api/v1/connection`: your topic and how to read it (Kafka bootstrap servers, your Unity Catalog credential, and
   the token scope to request), the hub that runs your rules, and the sources and columns you're granted.
1. `PUT /api/v1/subscriptions` with the app's whole set of `{name, sql}`. Databricks compiles every rule in that call.
   Either all are accepted (each with a `revision` and your `destination` topic), or nothing is applied and each refusal
   carries Databricks' own error.
2. `GET /api/v1/subscriptions?wait=25`, repeated until `state` is `READY`: the hub is running exactly those revisions.
   `FAILED` and `HUB_STALE` are final answers too. Keep each call short (≤ 25 s): a proxy in between may cut long
   requests.
3. Read your topic (Kafka protocol on Azure Event Hubs, OAUTHBEARER with a token Unity Catalog issues for your
   credential: `POST /api/2.1/unity-catalog/temporary-service-credentials` on the workspace).
   Deduplicate on `notificationId`, and act only on the `(subscriptionId, revision)` pairs you registered.

A rule is `SELECT <columns> FROM <silver source> WHERE <filter>`, with any Databricks SQL built-in in the filter. The
same SQL against your views in the SQL editor returns the records you'll receive.

**Hubs.** A hub is the pipeline that runs rules and delivers matches. The platform assigns each app one: `edp_shared`
(the default), or a team's own hub, which serves only the apps assigned to it. Every app's `deferred` rules run on
`edp_shared`'s deferred pipeline. Either way your calls, rules, and topic are the same; `hub` fields say which hub runs
what, and only that hub's verdict and heartbeat count.

**Compatibility.** Version 1 only adds fields: clients must ignore fields they don't know.
"""

app = FastAPI(
    title="HL7 Subscription API",
    version="1",
    description=DESCRIPTION,
    openapi_tags=[
        {"name": "subscriptions", "description": "Declare, change, cancel, and watch your subscriptions"},
        {"name": "service", "description": "Health"},
    ],
)
_VERIFIED_CALLER = Header(default=None, include_in_schema=False)  # set by the Apps proxy, never by the client


_TEMPLATE = Field(default=None, description="A data template granted to your app: the rule may read, filter on, and "
                  "receive only the template's columns (TEMPLATE_DENIED otherwise). Omitted: your app's whole grant.",
                  examples=["ed-respiratory-monitoring"])  # fmt: skip
_TIER = Field(default="realtime", description="realtime: delivered within seconds (clinical), by your app's hub. "
              "deferred: delivered by edp_shared's deferred pipeline on a longer trigger, for work that can wait (e.g. "
              "billing); it never shares a batch with realtime rules.")  # fmt: skip
_HUB = Field(default=None, description="The hub that runs it: your app's hub for realtime rules, edp_shared for "
             "deferred ones", examples=["edp_shared"])  # fmt: skip


class Item(BaseModel):
    name: str = Field(description="1-63 lowercase letters, digits, or '-'; unique within your app",
                      examples=["critical-potassium"])  # fmt: skip
    sql: str = Field(
        description="SELECT <columns> FROM <silver source> WHERE <filter>, in Databricks SQL",
        examples=["SELECT patient_id, value_num, units FROM silver_observations "
                  "WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5"],
    )  # fmt: skip
    template: str | None = _TEMPLATE
    tier: Literal["realtime", "deferred"] = _TIER


class Declared(BaseModel):
    subscriptions: list[Item] = Field(description="Your app's whole set: anything not listed is cancelled")


class One(BaseModel):
    sql: str = Field(description="SELECT <columns> FROM <silver source> WHERE <filter>, in Databricks SQL")
    template: str | None = _TEMPLATE
    tier: Literal["realtime", "deferred"] = _TIER


# Responses. Extra fields are kept, so the contract can grow without breaking clients.
class _Open(BaseModel):
    model_config = ConfigDict(extra="allow")


class Accepted(_Open):
    subscriptionId: str = Field(description="Stable for the subscription's life; notifications carry it")
    name: str
    revision: int = Field(description="Counts every change; an identical rule keeps its revision")
    state: Literal["ACCEPTED"]
    query: str = Field(description="The canonical SQL that's stored and run")
    destination: str = Field(description="Your topic: where notifications arrive")
    template: str | None = Field(default=None, description="The data template the rule is held to, if any")
    tier: str = Field(default="realtime", description="realtime or deferred")
    hub: str | None = _HUB
    unchanged: bool | None = Field(default=None, description="True when the rule was already active as is")


class Cancelled(_Open):
    subscriptionId: str
    name: str
    revision: int
    state: Literal["CANCELLING"] = Field(description="The hub stops it at its next batch (then CANCELLED)")


class DeclareResult(_Open):
    applied: Literal[True]
    accepted: list[Accepted]
    cancelled: list[Cancelled] = Field(description="Subscriptions you no longer declare")


class Refusal(_Open):
    name: str | None = None
    code: str = Field(description="Databricks' error class (UNRESOLVED_COLUMN, PARSE_SYNTAX_ERROR, ...) or ours "
                      "(SQL_NOT_ALLOWED, INVALID_NAME, RULE_LIMIT_REACHED, TEMPLATE_DENIED)")  # fmt: skip
    message: str


class RefusedResult(_Open):
    applied: Literal[False]
    refused: list[Refusal]


class Error(_Open):
    code: str = Field(description="UNAUTHENTICATED, APP_NOT_ONBOARDED, DUPLICATE_NAME, NOT_FOUND_OR_INACCESSIBLE")
    message: str


class Checked(_Open):
    name: str
    query: str


class ValidateResult(_Open):
    valid: bool
    refused: list[Refusal]
    accepted: list[Checked]


class Subscription(_Open):
    subscriptionId: str
    name: str
    revision: int
    enabled: bool
    source: str
    query: str
    fields: list[str]
    template: str | None = None
    tier: str = "realtime"
    hub: str | None = _HUB
    state: str = Field(description="The verdict of the hub that runs it, for this revision: PENDING (not yet), PLACED "
                       "(in the hub's matcher: it decides every record from now on), ACTIVE (it has decided a batch "
                       "with it), FAILED, or CANCELLED")  # fmt: skip
    error: str | None = Field(default=None, description="Spark's error when FAILED")


class Heartbeat(_Open):
    hub: str = Field(description="The hub", examples=["edp_shared"])
    tier: str = Field(description="realtime or deferred: which of its pipelines")
    source: str = Field(description="The silver source it read")
    lastBatchAt: str = Field(description="When it last processed a batch of the source (ISO 8601)")
    batchId: int = Field(json_schema_extra={"format": "int64"})
    rules: int = Field(description="Rules it ran on the source in that batch")


class Status(_Open):
    app: str
    state: Literal["READY", "PENDING", "FAILED", "HUB_STALE"] = Field(
        description="READY: the hub runs exactly your current revisions (each PLACED or ACTIVE). PENDING: not yet. "
        "FAILED: it can't run one (see problems). HUB_STALE: the hub that runs one of your rules hasn't processed its "
        "source recently."
    )  # fmt: skip
    problems: list[str]
    subscriptions: list[Subscription]
    hub: dict[str, str] = Field(description="Source -> the last batch time (ISO 8601) of the hubs that run your rules; "
                                "the deferred tier's sources are prefixed 'deferred:'")  # fmt: skip
    hubName: str = Field(description="Your app's hub: the one that runs your realtime rules", examples=["edp_shared"])
    heartbeats: list[Heartbeat] = Field(description="The heartbeats of the hubs that run your rules (your hub's "
                                        "realtime pipeline and the deferred pipeline), per source")  # fmt: skip


class Health(_Open):
    ok: bool


class Connection(_Open):
    """Everything an app needs to read its notifications, so it configures only its service principal and the API."""

    app: str
    topic: str = Field(description="Your topic: the Kafka topic your notifications arrive on (same as `destination`)")
    bootstrapServers: str = Field(description="Kafka bootstrap servers (SASL_SSL, OAUTHBEARER)",
                                  examples=["<namespace>.servicebus.windows.net:9093"])  # fmt: skip
    credential: str = Field(
        description="Your Unity Catalog service credential. Mint the Kafka token with it, as your service principal: "
        "POST <workspace>/api/2.1/unity-catalog/temporary-service-credentials "
        '{"credential_name": credential, "azure_options": {"resources": [tokenScope]}} -> azure_aad.aad_token',
        examples=["sub_my_app_eventhubs"],
    )
    tokenScope: str = Field(description="The resource to request the Kafka token for",
                            examples=["https://<namespace>.servicebus.windows.net/.default"])  # fmt: skip
    hub: str = Field(description="The hub that runs your realtime rules (the platform assigns it): edp_shared, or "
                     "your team's own hub. Your deferred rules run on edp_shared.",
                     examples=["edp_shared"])  # fmt: skip
    sources: list[str] = Field(description="The silver sources you may subscribe to")
    fields: list[str] = Field(description="The columns you may read and filter on (a data template may narrow them)")


class Notification(_Open):
    """The JSON value of each message on your destination topic. Its Kafka key is the patient ID. Not returned by any
    call: published here so clients can generate it with the API's own models."""

    notificationId: str = Field(description="Stable across redeliveries (at-least-once delivery): deduplicate on it")
    subscriptionId: str
    subscription: str = Field(description="The subscription's name")
    revision: int = Field(description="Act only on the (subscriptionId, revision) pairs your last declare returned")
    source: str = Field(description="The silver source the record came from")
    sourcePartition: int
    sourceOffset: int = Field(json_schema_extra={"format": "int64"})
    source_ts: str = Field(description="When the source record was produced (ISO 8601, broker time)")
    payload: dict[str, Any] = Field(description="Exactly the columns the rule selects, nulls included")


_ERRORS = {
    401: {"model": Error, "description": "No verified caller"},
    403: {"model": Error, "description": "Not an onboarded subscriber app (APP_NOT_ONBOARDED)"},
}
_REFUSED = {422: {"model": RefusedResult, "description": "Refused: nothing applied; Databricks' error per rule"}}


class Refused(Exception):
    def __init__(self, status: int, body: dict):
        self.status, self.body = status, body


@app.exception_handler(Refused)
def _refused(_: Request, e: Refused):
    return JSONResponse(status_code=e.status, content=e.body)


# ---------------------------------------------------------------------------------------------------------------------
# Registry connections: a small pool per instance, renewed before the database credential expires. Lakebase closes
# idle connections (and suspends when idle), so a pooled one may be dead: if its first statement hits a network error
# (nothing has happened on it yet), it's closed and the statement runs on a new connection. A connection that failed
# is never pooled, or every later request would fail on it.
class Pool:
    def __init__(self, connect):
        self._connect, self._idle, self._lock = connect, [], threading.Lock()

    def get(self):
        with self._lock:
            while self._idle:
                conn, opened = self._idle.pop()
                if time.time() - opened < db.TOKEN_SECONDS:
                    return conn, opened
                _close(conn)
        return self._connect(), time.time()

    def open(self, first: str, **params):
        """A connection, its open time, and its first statement's result."""
        conn, opened = self.get()
        try:
            return conn, opened, conn.run(first, **params)
        except InterfaceError:
            _close(conn)
        conn, opened = self._connect(), time.time()
        try:
            return conn, opened, conn.run(first, **params)
        except Exception:
            _close(conn)
            raise

    def put(self, conn, opened):
        with self._lock:
            self._idle.append((conn, opened))


def _close(conn):
    with contextlib.suppress(Exception):  # already gone
        conn.close()


pool = Pool(lambda: db.connect(ENDPOINT, DATABASE))


class Session:
    """One registry transaction as the calling app's own role."""

    def __init__(self, role: str):
        self.role = role

    def __enter__(self):
        self.conn, self.opened, _ = pool.open("BEGIN")
        self.conn.run(f'SET LOCAL ROLE "{self.role}"')
        return self.conn

    def __exit__(self, exc_type, *_):
        try:
            self.conn.run("ROLLBACK" if exc_type else "COMMIT")
            pool.put(self.conn, self.opened)
        except Exception:  # noqa: BLE001 - a broken connection isn't returned to the pool
            _close(self.conn)


def _caller(email: str | None) -> dict:
    if not email:
        raise Refused(401, {"code": "UNAUTHENTICATED", "message": "no verified caller"})
    conn, opened, rows = pool.open(  # onboarding stores principals lowercase; the proxy may send an email in any case
        "SELECT app_role, app_name, destination_topic, eventhubs_credential, hub, allowed_sources, allowed_fields "
        "FROM subscription_registry.apps WHERE lower(service_principal) = lower(:sp)",
        sp=email,
    )
    pool.put(conn, opened)
    if not rows:
        raise Refused(403, {"code": "APP_NOT_ONBOARDED", "message": f"{email} isn't an onboarded subscriber app"})
    role, name, topic, credential, hub, sources, fields = rows[0]
    return {"role": role, "name": name, "schema": f"{CATALOG}.sub_{name.replace('-', '_')}", "topic": topic,
            "credential": credential, "hub": hub, "sources": sources, "fields": fields}  # fmt: skip


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


# Shapes Databricks compiled for an app's views lately: (views schema, rules.shape(query)) -> when. A rule edit that
# only changes literals (a patient list) reuses it and skips the warehouse. Kept SHAPE_TTL, so a revoked grant shows.
_COMPILED: dict[tuple[str, str], float] = {}
SHAPE_TTL = 300  # seconds


def _check(item: Item, who: dict, registered: dict, templates: dict) -> dict:
    """{name, ok: True, rule, template, tier} or {name, ok: False, code, message} (shape check, the template, then
    Databricks)."""
    try:
        rule = rules.parse(item.sql, SOURCES, who["schema"])
    except rules.RuleError as e:
        if e.code != "PARSE":
            return {"name": item.name, "ok": False, "code": e.code, "message": e.message}
        error = compile_with_databricks(item.sql, who["schema"])  # Databricks' own words for why it doesn't parse
        return {"name": item.name, "ok": False, **(error or {"code": "SQL_NOT_ALLOWED", "message": e.message})}
    if item.template is not None:
        t = templates.get(item.template)
        outside = None if t is None else (set(rule.fields) | rules.columns(rule.where)) - set(t["fields"])
        if t is None or rule.source not in t["sources"] or outside:
            why = (f"isn't granted to {who['name']}" if t is None else f"doesn't include {rule.source}"
                   if rule.source not in t["sources"] else f"doesn't include {sorted(outside)}")  # fmt: skip
            return {"name": item.name, "ok": False, "code": "TEMPLATE_DENIED",
                    "message": f"TEMPLATE_DENIED: template {item.template} {why}"}  # fmt: skip
    same = registered.get(item.name)
    if not (same and same["enabled"] and same["query"] == rule.query):  # unchanged rules were compiled before
        key = (who["schema"], rules.shape(rule.query))
        if time.monotonic() - _COMPILED.get(key, -SHAPE_TTL) >= SHAPE_TTL:  # a new shape, or not recently
            error = compile_with_databricks(rule.query, who["schema"])
            if error:
                return {"name": item.name, "ok": False, **error}
            if len(_COMPILED) > 20_000:
                _COMPILED.clear()
            _COMPILED[key] = time.monotonic()
    return {"name": item.name, "ok": True, "rule": rule, "template": item.template, "tier": item.tier}


def _registered(conn) -> dict:
    listing = conn.run("SELECT subscription_api.list_subscriptions()")[0][0]
    return {s["name"]: s for s in listing["subscriptions"]}


def _templates(conn) -> dict:
    """The caller's data templates (row-level security: only its own): name -> {sources, fields}."""
    rows = conn.run("SELECT name, sources, fields FROM subscription_registry.templates")
    return {name: {"sources": sources, "fields": fields} for name, sources, fields in rows}


def _apply(conn, checked: list[dict], registered: dict) -> list[dict]:
    out = []
    for c in checked:
        r = c["rule"]
        try:
            receipt = conn.run("SELECT subscription_api.subscribe(:n, :s, :q, :w, CAST(:f AS text[]), :t, :tier)",
                               n=c["name"], s=r.source, q=r.query, w=r.where, f=list(r.fields), t=c["template"],
                               tier=c["tier"])[0][0]  # fmt: skip
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


def _validate(items: list[Item], who: dict, registered: dict, templates: dict) -> list[dict]:
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
    return [_check(i, who, registered, templates) for i in items]


def _refusals(checked):
    return [{k: v for k, v in c.items() if k != "ok"} for c in checked if not c["ok"]]


# ---------------------------------------------------------------------------------------------------------------------
@app.get("/api/v1/health", operation_id="health", tags=["service"], response_model=Health, summary="Liveness")
def health():
    return {"ok": True}


@app.get(
    "/api/v1/connection",
    operation_id="getConnection",
    tags=["subscriptions"],
    response_model=Connection,
    responses=_ERRORS | {503: {"model": Error, "description": "The platform hasn't finished onboarding this app"}},
    summary="Your topic and how to read it (Kafka bootstrap, Unity Catalog credential, token scope), hub and grant",
)
def connection(x_forwarded_email: str | None = _VERIFIED_CALLER):
    who = _caller(x_forwarded_email)
    if not (EVENTHUBS_NAMESPACE and who["credential"] and who["topic"]):
        raise Refused(503, {"code": "NOT_CONFIGURED", "message": f"{who['name']}'s connection isn't set up yet "
                            "(the platform runs make registry)"})  # fmt: skip
    with Session(who["role"]) as conn:  # its templates sit within its grant; listed too, in case one adds to it
        templates = _templates(conn)
    sources = set(who["sources"]).union(*(t["sources"] for t in templates.values()))
    fields = set(who["fields"]).union(*(t["fields"] for t in templates.values()))
    return {"app": who["name"], "topic": who["topic"],
            "bootstrapServers": f"{EVENTHUBS_NAMESPACE}.servicebus.windows.net:9093", "credential": who["credential"],
            "tokenScope": f"https://{EVENTHUBS_NAMESPACE}.servicebus.windows.net/.default", "hub": who["hub"],
            "sources": sorted(sources), "fields": sorted(fields)}  # fmt: skip


@app.post(
    "/api/v1/validate",
    operation_id="validate",
    tags=["subscriptions"],
    response_model=ValidateResult,
    responses=_ERRORS,
    summary="Check rules without applying anything",
)
def validate(body: Declared, x_forwarded_email: str | None = _VERIFIED_CALLER):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        checked = _validate(body.subscriptions, who, _registered(conn), _templates(conn))
    return {"valid": all(c["ok"] for c in checked), "refused": _refusals(checked),
            "accepted": [{"name": c["name"], "query": c["rule"].query} for c in checked if c["ok"]]}  # fmt: skip


@app.put(
    "/api/v1/subscriptions",
    operation_id="declare",
    tags=["subscriptions"],
    response_model=DeclareResult,
    responses=_ERRORS | _REFUSED,
    summary="Declare your whole set (atomic; cancels what you no longer declare)",
)
def declare(body: Declared, x_forwarded_email: str | None = _VERIFIED_CALLER):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        registered = _registered(conn)
        checked = _validate(body.subscriptions, who, registered, _templates(conn))
        if not all(c["ok"] for c in checked):
            raise Refused(422, {"applied": False, "refused": _refusals(checked)})
        accepted = _apply(conn, checked, registered)
        keep = {c["name"] for c in checked}
        cancelled = []
        for name, s in registered.items():
            if s["enabled"] and name not in keep:
                cancelled.append(conn.run("SELECT subscription_api.unsubscribe(:n)", n=name)[0][0])
    return {"applied": True, "accepted": accepted, "cancelled": cancelled}


@app.put(
    "/api/v1/subscriptions/{name}",
    operation_id="putSubscription",
    tags=["subscriptions"],
    response_model=Accepted,
    responses=_ERRORS | _REFUSED,
    summary="Create or change one subscription",
)
def put_one(name: str, body: One, x_forwarded_email: str | None = _VERIFIED_CALLER):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        registered = _registered(conn)
        checked = _validate(
            [Item(name=name, sql=body.sql, template=body.template, tier=body.tier)], who, registered, _templates(conn)
        )
        if not checked[0]["ok"]:
            raise Refused(422, {"applied": False, "refused": _refusals(checked)})
        return _apply(conn, checked, registered)[0]


@app.delete(
    "/api/v1/subscriptions/{name}",
    operation_id="cancel",
    tags=["subscriptions"],
    response_model=Cancelled,
    responses=_ERRORS | {404: {"model": Error, "description": "No active subscription with that name"}},
    summary="Cancel one subscription",
)
def cancel(name: str, x_forwarded_email: str | None = _VERIFIED_CALLER):
    who = _caller(x_forwarded_email)
    with Session(who["role"]) as conn:
        try:
            return conn.run("SELECT subscription_api.unsubscribe(:n)", n=name)[0][0]
        except Exception as e:  # noqa: BLE001 - the registry's domain error
            message = _registry_message(e)
            raise Refused(404, {"code": message.split(":")[0], "message": message}) from None


def _state(listing: dict) -> dict:
    """READY, PENDING, FAILED, or HUB_STALE from the registry's listing, whose `hub` holds only the heartbeats of the
    hubs that run the caller's rules (another hub beating says nothing about this app's)."""
    subs = [s for s in listing["subscriptions"] if s["enabled"]]
    failed = [f"{s['name']}: {s['error']}" for s in subs if s["state"] == "FAILED"]
    pending = [s["name"] for s in subs if s["state"] not in RUNNING]
    now = time.time()

    def fresh(key):  # the deferred pipeline runs a batch per trigger interval
        at = listing["hub"].get(key)
        return at is not None and now - _epoch(at) < HUB_STALE_AFTER + (DEFERRED_INTERVAL if ":" in key else 0)

    keys = {s["source"] if s.get("tier", "realtime") == "realtime" else f"{s['tier']}:{s['source']}" for s in subs}
    stale = sorted(k for k in keys if not fresh(k))
    if failed:
        return {"state": "FAILED", "problems": failed}
    if pending:
        return {"state": "PENDING", "problems": pending}
    if stale:
        hub = f"hub {listing.get('hubName')}"
        return {
            "state": "HUB_STALE",
            "problems": [f"the {'deferred hub' if ':' in s else hub} hasn't processed {s} for {HUB_STALE_AFTER} s"
                         for s in stale],
        }  # fmt: skip
    return {"state": "READY", "problems": []}


def _epoch(ts: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(ts).timestamp()


@app.get(
    "/api/v1/subscriptions",
    operation_id="getStatus",
    tags=["subscriptions"],
    response_model=Status,
    responses=_ERRORS,
    summary="Your subscriptions and whether the hub runs them (optionally wait)",
)
def status(
    wait: int = Query(0, description=f"Seconds to wait for READY or FAILED (capped at {MAX_WAIT}; use <= 25)"),
    x_forwarded_email: str | None = _VERIFIED_CALLER,
):
    who = _caller(x_forwarded_email)
    deadline = time.time() + min(max(wait, 0), MAX_WAIT)
    while True:
        with Session(who["role"]) as conn:
            listing = conn.run("SELECT subscription_api.list_subscriptions()")[0][0]
        state = _state(listing)
        if state["state"] in ("READY", "FAILED") or time.time() >= deadline:
            return {"app": who["name"], **state, **listing}
        time.sleep(WAIT_POLL)


def _openapi() -> dict:
    """The served contract, plus the Notification message clients receive on their topic."""
    if app.openapi_schema is None:
        schema = get_openapi(title=app.title, version=app.version, description=app.description, routes=app.routes,
                             tags=app.openapi_tags)  # fmt: skip
        schema["components"]["schemas"]["Notification"] = Notification.model_json_schema(
            ref_template="#/components/schemas/{model}"
        )
        for model in schema["components"]["schemas"].values():  # the contract: clients ignore fields they don't know
            model.pop("additionalProperties", None)
        app.openapi_schema = schema
    return app.openapi_schema


app.openapi = _openapi


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", "8000"))))
