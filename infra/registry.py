"""Idempotent subscription registry bootstrap (`make registry`, run by `make up` and `make demo`). Safe to re-run.

Two inputs, the same steps (a Config):
  Terraform         (default) the platform's environment: infra/azure's outputs
  --target <env>    the subscription layer alone, in a workspace that already has its data: subscriptions/targets/
                    <env>.yml read as its bundle reads it (`make subscriptions TARGET=<env>`, or the notebook
                    subscriptions/setup.py, which imports these steps from a Git folder)

  Lakebase project  <registry_project> (created if missing; scales to zero)
  registry schema   src/registry/registry.sql (tables, owner trigger, RLS, API functions, monitor views, grants)
  UC catalog        <catalog>_registry (a target: registry_catalog, empty skips it): the registry database in Unity
                    Catalog, read by the subscriptions dashboard (schema subscription_monitor; nothing granted to apps)
  subscriber apps   terraform var subscriber_apps (or the target's), per app:
                      - a NOLOGIN Postgres role app_<name> in `subscriber`, usable only by the Subscription API, and its
                        binding (principal, topic, credential, hub, sources, fields) and its data templates
                        (subscriber_templates). Apps removed from the config lose it. A principal is a service
                        principal's application ID, or (a target only) a user's email, stored lowercase.
                      - UC views <catalog>.sub_<name>.<source> exposing only its granted fields: Databricks compiles
                        the app's SQL against them, and developers test rules there in the SQL editor. Over silver
                        (Terraform), or over the target's subscription_tables, with their columns from Unity Catalog.
                      - CAN_USE on the Subscription API app
  subscription hubs edp_shared (the platform's; the main bundle, or the subscriptions bundle, deploys it) and each team
                    hub of terraform var subscriber_hubs (bring your own hub: the team deploys hub/ as its service
                    principal), and which one serves each app (subscriber_apps.<app>.subscription_hub). A team hub's SP
                    gets a login role in hub_<name> (in `hub`: it reads only its apps' rules and writes only its own
                    rows), use of the Lakebase project, and a schema <catalog>.hub_<name> for its pipeline. Hubs gone
                    from the config are deleted once their apps are back on edp_shared. A target's hub pipelines get
                    their schema <catalog>.<hub_schema>.
  Subscription API  its service principal joins `subscription_service` (so it can act as each app) and may read the
                    apps' views; the Lakebase Data API is turned off, so the API is the only way in
  app settings      (Terraform only) examples/subscribers/apps/<app>.properties (gitignored) for the example apps,
                    rewritten every run (URLs change when the workspace is rebuilt). The profile name defaults to the
                    app name.

Two phases: before the bundle deploys (--phase before-deploy: everything above but the Subscription API's grants), and
after it deployed the API (--phase after-deploy: its grants). Without --phase, both: until the API exists, its grants
are skipped, so `make demo dev` runs this before and after its deploy.

Runs as you (the project owner): Terraform's path authenticated like the rest of the Makefile (DATABRICKS_HOST + Azure
CLI), a target's as its bundle (its workspace host, profile, or auth_type), and in a notebook as the notebook's user.
"""

import argparse
import json
import os
import re
import ssl
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pg8000.native
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.postgres import Catalog, CatalogCatalogSpec

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
SUBSCRIPTIONS = ROOT / "subscriptions"  # the subscription layer's bundle; --target reads its targets/<env>.yml


def tf_output(name: str):
    out = subprocess.run(["terraform", "-chdir=infra/azure", "output", "-json", name], cwd=ROOT,
                         capture_output=True, text=True, check=True)  # fmt: skip
    return json.loads(out.stdout)


def role(app: str) -> str:
    return "app_" + app.replace("-", "_")


def credential(app: str) -> str:
    """The app's UC service credential for its hub (infra/uc.sh creates it with this name)."""
    return f"sub_{app.replace('-', '_')}_eventhubs"


SHARED_HUB = "edp_shared"  # the platform's subscription hub (registry.sql seeds it; the main bundle deploys it)


def hub_role(hub: str) -> str:
    """A team hub's group role (and the name of its pipeline's schema, <catalog>.hub_<name>)."""
    return f"hub_{hub}"


def hub_credential(hub: str) -> str:
    """A team hub's UC service credential (infra/uc.sh creates it with this name)."""
    return f"hub_{hub}_eventhubs"


class ConfigError(ValueError):
    """A configuration the registry can't be built from; the message says what to fix."""


# ---------------------------------------------------------------------------------------------------------------------
# Principals: a service principal is its application ID (what the Apps proxy reports for it); a user is an email.
_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def is_service_principal(principal: str) -> bool:
    return bool(_UUID.fullmatch(principal))


def stored_principal(name: str) -> str:
    """An app's principal as the registry stores it: an application ID as it is, an email in lowercase (the API
    matches the caller case-insensitively)."""
    return name if is_service_principal(name) else name.lower()


def can_use(name: str) -> AppAccessControlRequest:
    """CAN_USE on the Subscription API for an app's principal, a service principal or a user."""
    if is_service_principal(name):
        return AppAccessControlRequest(service_principal_name=name, permission_level=AppPermissionLevel.CAN_USE)
    return AppAccessControlRequest(user_name=name, permission_level=AppPermissionLevel.CAN_USE)


# ---------------------------------------------------------------------------------------------------------------------
# A target of the subscriptions bundle, read as the Databricks CLI reads it.
_REF = re.compile(r"\$\{([A-Za-z0-9_.\-]+)\}")


def _text(value) -> str:
    return ("true" if value else "false") if isinstance(value, bool) else str(value)


def resolve_variables(values: dict, target: str, unresolved: frozenset = frozenset()) -> dict:
    """`values` with ${var.<name>} (and ${var.<name>.<key>...}) and ${bundle.target} interpolated, as the CLI does.
    Other references (${workspace...}, ${resources...}) stay as written, as do references to `unresolved` variables
    (lookups: the CLI resolves them in the workspace). A reference that is the whole string keeps its value's type."""
    done = {}

    def var(path: str, stack: list, text):
        name, *keys = path.split(".")
        if name in unresolved:
            return text
        if name not in values:
            raise ConfigError(f"${{var.{name}}} names no variable with a value (in {stack[-1] if stack else name})")
        if name in stack:
            raise ConfigError(f"variables reference each other in a cycle: {' -> '.join([*stack, name])}")
        if name not in done:
            done[name] = sub(values[name], [*stack, name])
        value = done[name]
        for key in keys:
            try:
                value = value[key]
            except (KeyError, IndexError, TypeError):
                raise ConfigError(f"${{var.{path}}}: {name} has no {key}") from None
        return value

    def ref(path: str, stack: list, text: str):
        if path == "bundle.target":
            return target
        return var(path[4:], stack, text) if path.startswith("var.") else text

    def sub(value, stack: list):
        if isinstance(value, dict):
            return {k: sub(v, stack) for k, v in value.items()}
        if isinstance(value, list):
            return [sub(v, stack) for v in value]
        if not isinstance(value, str):
            return value
        whole = _REF.fullmatch(value)
        if whole:
            return ref(whole.group(1), stack, value)
        return _REF.sub(lambda m: _text(ref(m.group(1), stack, m.group(0))), value)

    return {name: var(name, [], None) for name in values}


_LONG_FORM = {"default", "lookup", "description", "type"}  # a target may set a variable as {default: ...}


def read_target(env: str, bundle: Path = SUBSCRIPTIONS) -> tuple[dict, dict]:
    """Target `env` of a bundle as the CLI reads it: targets/<env>.yml's variables over the bundle's defaults,
    resolved. Returns (variables, the target's workspace block). A lookup variable (warehouse_id) is left out unless the
    target sets its value: the workspace resolves it."""
    declared = yaml.safe_load((bundle / "databricks.yml").read_text())["variables"]
    path = bundle / "targets" / f"{env}.yml"
    if not path.is_file():
        raise ConfigError(f"no {path}: copy {bundle / 'target.example.yml'} there and name its target {env}")
    target = ((yaml.safe_load(path.read_text()) or {}).get("targets") or {}).get(env)
    if target is None:
        raise ConfigError(f"{path} has no target {env} (name the target after its file)")
    values = {name: spec["default"] for name, spec in declared.items() if "default" in spec}
    lookups = {name for name, spec in declared.items() if "lookup" in spec}
    for name, value in (target.get("variables") or {}).items():
        if name not in declared:
            raise ConfigError(f"{path}: variable {name} is undeclared in {bundle / 'databricks.yml'}")
        if isinstance(value, dict) and set(value) & {"default", "lookup"} and set(value) <= _LONG_FORM:
            if "default" not in value:
                continue  # a lookup of its own
            value = value["default"]
        values[name] = value
    missing = sorted(name for name in declared if name not in values and name not in lookups)
    if missing:
        raise ConfigError(f"{path}: set {', '.join(missing)} (no default)")
    return resolve_variables(values, env, frozenset(lookups - set(values))), target.get("workspace") or {}


_APP = re.compile(r"[a-z0-9][a-z0-9-]{0,40}")  # = terraform's validation of subscriber_apps names
_SOURCE = re.compile(r"[a-z_][a-z0-9_]*")


def source_tables(tables: str) -> dict[str, str]:
    """subscription_tables (a comma list of <catalog>.<schema>.<table>) as source -> table: a source is named by its
    table, in lowercase, as rules name it (identifiers are case-insensitive) and Unity Catalog stores it."""
    out = {}
    for table in (t.strip() for t in tables.split(",")):
        if not table:
            continue
        parts = table.split(".")
        source = parts[-1].strip("`")
        if len(parts) != 3 or not all(parts) or not _SOURCE.fullmatch(source) or source in out:
            raise ConfigError(f"subscription_tables: {table} isn't <catalog>.<schema>.<table> with a lowercase table "
                              "name of its own")  # fmt: skip
        out[source] = table
    return out


def _distinct(names) -> list[str]:
    return list(dict.fromkeys(n.lower() for n in names))


def _target_app(name: str, a: dict, templates: dict, hubs: dict) -> dict:
    """A target's app in the shape both inputs give: its grant is its own sources and fields plus its templates'."""
    if not _APP.fullmatch(name):
        raise ConfigError(f"app {name}: names are lowercase letters, digits, and hyphens")
    for key in ("principal", "topic", "eventhubs_credential"):
        if not a.get(key):
            raise ConfigError(f"app {name}: set its {key}")
    unknown = [t for t in a.get("templates") or [] if t not in templates]
    if unknown:
        raise ConfigError(f"app {name}: templates {unknown} aren't in subscriber_templates")
    own = {t: {"sources": _distinct(templates[t]["sources"]), "fields": _distinct(templates[t]["fields"])}
           for t in a.get("templates") or []}  # fmt: skip
    hub = a.get("subscription_hub") or SHARED_HUB
    if hub != SHARED_HUB and hub not in hubs:
        raise ConfigError(f"app {name}: subscription_hub {hub} is neither {SHARED_HUB} nor one of subscriber_hubs")
    sources = _distinct([*(a.get("sources") or []), *(s for t in own.values() for s in t["sources"])])
    if not sources:
        raise ConfigError(f"app {name}: grant it sources, or templates")
    return {
        "principal": stored_principal(a["principal"]), "topic": a["topic"],
        "eventhubs_credential": a["eventhubs_credential"], "subscription_hub": hub, "sources": sources,
        "fields": _distinct([*(a.get("fields") or []), *(f for t in own.values() for f in t["fields"])]),
        "templates": own,
    }  # fmt: skip


@dataclass
class Config:
    """What the bootstrap builds, from Terraform (the platform's environment) or a target of the subscriptions bundle.
    apps: name -> {principal, topic, eventhubs_credential, sources, fields (["*"]: every column), templates: {name:
    {sources, fields}}, subscription_hub}."""

    target: str | None  # the subscriptions bundle's target; None: Terraform's
    project: str  # Lakebase project
    endpoint: str
    database: str
    api_app: str  # the Subscription API's app name
    warehouse: str  # SQL warehouse, by name (warehouse_id, when set, wins)
    catalog: str  # the apps' view schemas <catalog>.sub_<app>
    schema: str | None  # the platform's silver schema
    hub_schema: str | None  # the hub pipelines' schema, created here (None: the main bundle creates its own)
    hub_credential: str  # edp_shared's UC service credential
    registry_catalog: str  # the registry in Unity Catalog ("" skips it)
    tables: dict  # source -> its table
    apps: dict
    hubs: dict = field(default_factory=dict)  # team hubs (subscriber_hubs)
    workspace: dict = field(default_factory=dict)  # a target's workspace block (host, profile, auth_type)
    workspace_id: str | None = None
    warehouse_id: str | None = None

    @property
    def branch(self) -> str:
        return self.endpoint.rsplit("/endpoints/", 1)[0]

    @classmethod
    def from_terraform(cls, project: str = "hl7-subscriptions") -> "Config":
        catalog, schema = tf_output("catalog"), tf_output("schema")
        apps = {
            name: {"principal": a["application_id"], "topic": a["hub"], "eventhubs_credential": credential(name),
                   "sources": a["sources"], "fields": a["fields"], "templates": a.get("templates") or {},
                   "subscription_hub": a.get("subscription_hub") or SHARED_HUB}
            for name, a in tf_output("subscriber_apps").items()
        }  # fmt: skip
        return cls(
            target=None, project=project, endpoint=f"projects/{project}/branches/production/endpoints/primary",
            database="databricks_postgres", api_app=os.environ.get("HL7_API_APP", "hl7-subscriptions-dev"),
            warehouse=os.environ.get("HL7_WAREHOUSE", "Serverless Starter Warehouse"), catalog=catalog, schema=schema,
            hub_schema=None, hub_credential=f"{catalog}_eventhubs", registry_catalog=f"{catalog}_registry",
            tables={s: f"{catalog}.{schema}.{s}" for s in silver_columns()}, apps=apps,
            hubs=tf_output("subscriber_hubs"), workspace_id=str(tf_output("workspace_id")),
        )  # fmt: skip

    @classmethod
    def from_target(cls, env: str, bundle: Path = SUBSCRIPTIONS) -> "Config":
        v, workspace = read_target(env, bundle)
        if v.get("subscription_hub", SHARED_HUB) != SHARED_HUB:
            raise ConfigError(f"subscription_hub: this bundle deploys {SHARED_HUB}; a team's own hub is hub/'s bundle")
        hubs, templates = v.get("subscriber_hubs") or {}, v.get("subscriber_templates") or {}
        apps = {n: _target_app(n, a or {}, templates, hubs) for n, a in (v.get("subscriber_apps") or {}).items()}
        tables = source_tables(v.get("subscription_tables") or "")
        if not tables:  # none of your own: the hub reads the platform's silver, <catalog>.<schema>.<source>
            tables = {s: f"{v['catalog']}.{v['schema']}.{s}" for a in apps.values() for s in a["sources"]}
        for name, a in apps.items():
            unknown = [s for s in a["sources"] if s not in tables]
            if unknown:
                raise ConfigError(f"app {name}: sources {unknown} aren't in subscription_tables ({', '.join(tables)})")
        return cls(
            target=env, project=v["registry_project"], endpoint=v["registry_endpoint"], database=v["registry_database"],
            api_app=v["subscription_api_app"], warehouse=v["warehouse"], warehouse_id=v.get("warehouse_id"),
            catalog=v["catalog"], schema=v.get("schema"), hub_schema=v["hub_schema"],
            hub_credential=v["hub_service_credential"], registry_catalog=v.get("registry_catalog") or "",
            tables=tables, apps=apps, hubs=hubs, workspace=workspace,
        )  # fmt: skip


# ---------------------------------------------------------------------------------------------------------------------
# Grants and views: what each app may read.


def silver_columns() -> dict[str, list[str]]:
    """The platform's silver sources and their columns (hl7rt's schemas), for Terraform's path."""
    from hl7rt.subscriptions import SOURCES  # pyspark: only this path needs it

    return {source: [c.name for c in schema] for source, schema in SOURCES.items()}


def table_columns(w, tables: dict[str, str]) -> dict[str, list[str]]:
    """Each source table's columns, from Unity Catalog, for a target's path."""
    out = {}
    for source, table in tables.items():
        try:
            out[source] = [c.name for c in w.tables.get(table.replace("`", "")).columns or []]
        except Exception as e:  # noqa: BLE001 - not found, or not yours to read
            raise ConfigError(f"source {source}: can't read table {table}: {e}") from None
    return out


def granted(fields: list[str], sources: list[str], columns: dict[str, list[str]]) -> list[str]:
    """A grant's columns as the registry stores them (lowercase, as rules name them): the ones it names, or for "*"
    every column of its sources, in their tables' order."""
    return _distinct([c for s in sources for c in columns[s]] if "*" in fields else fields)


def view_sql(views: str, source: str, table: str, columns: list[str], fields: list[str]) -> str:
    """An app's view of one source: the table's columns it's granted (every one for "*"), in the table's order."""
    grant = {f.lower() for f in fields}
    cols = [c for c in columns if "*" in grant or c.lower() in grant]
    if not cols:
        raise ConfigError(f"{views}.{source}: none of {sorted(grant)} is a column of {table}")
    quoted = ", ".join("`" + c.replace("`", "``") + "`" for c in cols)
    return f"CREATE OR REPLACE VIEW {views}.{source} AS SELECT {quoted} FROM {table}"


def _with_grants(a: dict, columns: dict[str, list[str]]) -> dict:
    """An app with its grant's and its templates' columns named ("*" expanded)."""
    templates = {t: {"sources": s["sources"], "fields": granted(s["fields"], s["sources"], columns)}
                 for t, s in a["templates"].items()}  # fmt: skip
    return {**a, "fields": granted(a["fields"], a["sources"], columns), "templates": templates}


def _views(app: str, catalog: str) -> str:
    return f"{catalog}.sub_{app.replace('-', '_')}"  # = the Subscription API's schema for the app


# ---------------------------------------------------------------------------------------------------------------------
# The registry: hubs and apps.


def _login(db, principal: str):
    """The principal's own login role: a Databricks identity on Lakebase, a plain login role on a local PostgreSQL."""
    if db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=principal):
        return
    if db.run("SELECT 1 FROM pg_proc WHERE proname = 'databricks_create_role'"):
        db.run("SELECT databricks_create_role(:r, 'SERVICE_PRINCIPAL')", r=principal)
    else:
        db.run(f'CREATE ROLE "{principal}" LOGIN')


def _hub_grants(db, hub: str, principal: str, me: str):
    """A team hub's Postgres identity: hub_<name> in `hub` (whose grants and RLS let it read only its apps' rules and
    write only its own rows), and the hub's service principal, the one member, so its pipeline connects as itself."""
    r = hub_role(hub)  # created, in `hub`, by subscription_registry.onboard_hub
    _login(db, principal)
    members = (
        "SELECT u.rolname FROM pg_auth_members m JOIN pg_roles u ON u.oid = m.member "
        "JOIN pg_roles g ON g.oid = m.roleid WHERE g.rolname = :g AND u.rolname <> ALL (CAST(:keep AS text[]))"
    )
    for (old,) in db.run(members, g=r, keep=[principal, me]):  # the deployer keeps managing the role it created
        db.run(f'REVOKE {r} FROM "{old}"')
    db.run(f'GRANT {r} TO "{principal}"')


def onboard_hubs(db, hubs: dict, apps: dict, me: str, catalog: str, shared_credential: str | None = None) -> list[str]:
    """Register the subscription hubs and which one serves each app's realtime rules: edp_shared (the platform's, run
    as the deployer `me`, which also runs every app's deferred rules; it writes with `shared_credential`, by default
    <catalog>_eventhubs) and each team hub of terraform var subscriber_hubs. Apps move before a hub that's gone from the
    config is deleted. Returns what it did, a line each."""
    done = []
    db.run(
        "INSERT INTO subscription_registry.hubs (name, principal, eventhubs_credential, runs_deferred) "
        "VALUES (:n, :p, :c, true) ON CONFLICT (name) DO UPDATE SET principal = excluded.principal, "
        "eventhubs_credential = excluded.eventhubs_credential",
        n=SHARED_HUB, p=me, c=shared_credential or f"{catalog}_eventhubs",
    )  # fmt: skip
    for name, h in hubs.items():
        db.run("SELECT subscription_registry.onboard_hub(:n, :p, :c, :m)", n=name, p=h["application_id"],
               c=hub_credential(name), m=h.get("max_enabled_rules", 2000))  # fmt: skip
        _hub_grants(db, name, h["application_id"], me)
        done.append(f"ok       hub {name} runs as {h['application_id']} ({hub_role(name)}, {hub_credential(name)})")
    for app, a in apps.items():
        hub = a.get("subscription_hub") or SHARED_HUB
        for (moved,) in db.run(
            "UPDATE subscription_registry.apps SET hub = :h WHERE app_role = :r AND hub <> :h RETURNING app_name",
            h=hub, r=role(app),
        ):  # fmt: skip
            done.append(f"moved    app {moved} to hub {hub}")
    for (gone,) in db.run(
        "DELETE FROM subscription_registry.hubs WHERE name <> ALL (CAST(:k AS text[])) RETURNING name",
        k=[SHARED_HUB, *hubs],
    ):
        db.run(f"DROP ROLE IF EXISTS {hub_role(gone)}")
        done.append(f"removed  hub {gone} (no longer in subscriber_hubs)")
    return done


def onboard_apps(db, apps: dict) -> list[str]:
    """Each app's NOLOGIN role (in `subscriber`, usable only by the Subscription API), its binding (principal, topic,
    credential, grant), and its data templates; `apps` name their columns (no "*"). Apps no longer configured lose
    their binding. Returns what it removed, a line each."""
    done = [
        f"removed  app {gone} (no longer in subscriber_apps, or its old binding)"
        for (gone,) in db.run(
            "DELETE FROM subscription_registry.apps WHERE app_role <> ALL (CAST(:k AS text[])) RETURNING app_name",
            k=[role(n) for n in apps],
        )
    ]
    for name, a in apps.items():
        r = role(name)
        db.run(f"DO $$ BEGIN CREATE ROLE {r} NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$")
        db.run(f"GRANT subscriber TO {r}")
        db.run(f"GRANT {r} TO subscription_service")
        db.run(
            "INSERT INTO subscription_registry.apps (app_role, app_name, destination_topic, allowed_sources, "
            "allowed_fields, service_principal, eventhubs_credential) "
            "VALUES (:r, :n, :t, CAST(:s AS text[]), CAST(:f AS text[]), :sp, :c) "
            "ON CONFLICT (app_role) DO UPDATE SET app_name = excluded.app_name, "
            "destination_topic = excluded.destination_topic, allowed_sources = excluded.allowed_sources, "
            "allowed_fields = excluded.allowed_fields, service_principal = excluded.service_principal, "
            "eventhubs_credential = excluded.eventhubs_credential",
            r=r, n=name, t=a["topic"], s=a["sources"], f=a["fields"], sp=stored_principal(a["principal"]),
            c=a["eventhubs_credential"],
        )  # fmt: skip
        templates = a.get("templates") or {}
        db.run("DELETE FROM subscription_registry.templates WHERE app_role = :r AND name <> ALL (CAST(:k AS text[]))",
               r=r, k=list(templates))  # fmt: skip
        for t, spec in templates.items():
            db.run(
                "INSERT INTO subscription_registry.templates (app_role, name, sources, fields) "
                "VALUES (:r, :n, CAST(:s AS text[]), CAST(:f AS text[])) ON CONFLICT (app_role, name) "
                "DO UPDATE SET sources = excluded.sources, fields = excluded.fields",
                r=r, n=t, s=spec["sources"], f=spec["fields"],
            )  # fmt: skip
    return done


# ---------------------------------------------------------------------------------------------------------------------
# The workspace: steps before and after the bundle deploys.


def grant_hub_workspace(w, warehouse_id: str, catalog: str, hubs: dict, project: str):
    """What a team hub's pipeline needs in the workspace, as its service principal: a schema of its own to target
    (<catalog>.hub_<name>; it writes no tables there) and use of the registry's Lakebase project. Its Event Hubs
    credential is infra/uc.sh's."""
    for name, h in hubs.items():
        sp, schema = h["application_id"], f"{catalog}.{hub_role(name)}"
        sql(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT 'Subscription hub {name}: its pipeline'")
        sql(w, warehouse_id, f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp}`")
        sql(w, warehouse_id, f"GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA {schema} TO `{sp}`")
        w.permissions.update("database-projects", project, access_control_list=[AccessControlRequest(
            service_principal_name=sp, permission_level=PermissionLevel.CAN_USE)])  # fmt: skip
        print(f"ok       hub {name}: schema {schema} and Lakebase project {project} for {sp}")


def sql(w, warehouse_id: str, statement: str):
    r = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        sys.exit(f"error: {statement[:80]}...: {r.status.error.message if r.status.error else r.status.state}")


def client(config: Config) -> WorkspaceClient:
    """The workspace, authenticated as you: in a Databricks notebook, the notebook's; for a target, as its bundle does
    (the target's host, and its profile or auth_type when set); for Terraform's path, the Makefile's environment."""
    if config.target is None or os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return WorkspaceClient()
    ws = config.workspace
    return WorkspaceClient(host=ws.get("host"), profile=ws.get("profile"), auth_type=ws.get("auth_type"))


def warehouse_id(w, config: Config) -> str:
    if config.warehouse_id:
        return config.warehouse_id
    found = next((x.id for x in w.warehouses.list() if x.name == config.warehouse), None)
    if not found:
        sys.exit(f"error: no SQL warehouse named {config.warehouse!r} (variable warehouse)")
    return found


def connect(w, config: Config):
    """(a registry connection as you, the project owner; the endpoint's host; your user name), once the endpoint has a
    host (a new project's takes a few minutes)."""
    host = None
    for _ in range(90):
        try:
            host = w.postgres.get_endpoint(name=config.endpoint).status.hosts.host
            if host:
                break
        except Exception:  # noqa: BLE001 - still provisioning
            pass
        time.sleep(10)
    if not host:
        sys.exit(f"error: endpoint {config.endpoint} has no host yet; run this again")
    me = w.current_user.me().user_name
    token = w.postgres.generate_database_credential(endpoint=config.endpoint).token
    db = pg8000.native.Connection(me, host=host, database=config.database, password=token,
                                  ssl_context=ssl.create_default_context())  # fmt: skip
    return db, host, me


def _registry_catalog(w, config: Config):
    """The registry database in Unity Catalog, for monitoring (the subscriptions dashboard)."""
    name = config.registry_catalog
    # After a sandbox wipe the shared metastore keeps the old registration, made for the deleted workspace's database.
    # It only feeds the subscriptions dashboard, so say how to replace it and carry on.
    try:
        old = w.catalogs.get(name)
    except Exception:  # noqa: BLE001 - not registered
        old = None
    built_for = (old.properties or {}).get("workspace_id") if old else None
    if built_for and built_for != (config.workspace_id or str(w.get_workspace_id())):
        print(f"STALE    catalog {name} is registered for workspace {built_for} (gone), so the "
              "subscriptions dashboard has no data. Remove it, then run this again:\n"
              f"         databricks api delete /api/2.0/postgres/catalogs/{name}")  # fmt: skip
        return
    try:
        w.postgres.get_catalog(name=f"catalogs/{name}")
        print(f"ok       catalog {name} (the registry database, for the subscriptions dashboard)")
    except Exception:  # noqa: BLE001 - not registered yet
        w.postgres.create_catalog(
            catalog=Catalog(spec=CatalogCatalogSpec(postgres_database=config.database, branch=config.branch)),
            catalog_id=name,
        ).wait()
        print(f"created  catalog {name} (the registry database, for the subscriptions dashboard)")


def before_deploy(w, config: Config):
    """What the bundle's resources need before they deploy: the Lakebase project and registry, the apps (roles,
    bindings, templates, views), the hubs, and the hub pipelines' schema."""
    api = w.api_client
    try:
        w.postgres.get_project(name=f"projects/{config.project}")
        print(f"ok       Lakebase project {config.project}")
    except Exception:  # noqa: BLE001 - not found
        api.do("POST", "/api/2.0/postgres/projects", query={"project_id": config.project}, body={"spec": {
            "display_name": config.project, "pg_version": 17,
            "default_endpoint_settings": {"autoscaling_limit_min_cu": 0.5, "autoscaling_limit_max_cu": 1,
                                          "suspend_timeout_duration": "300s"}}})  # fmt: skip
        print(f"created  Lakebase project {config.project} (0.5-1 CU, suspends after 5 idle minutes)")

    db, host, me = connect(w, config)
    try:
        db.run("CREATE EXTENSION IF NOT EXISTS databricks_auth")
        db.run((ROOT / "src/registry/registry.sql").read_text())
        print("ok       registry schema (src/registry/registry.sql)")

        # The Lakebase Data API was the apps' way in before the Subscription API; turn it off so it can't be a bypass.
        data_api = f"/api/2.0/postgres/{config.branch}/databases/{config.database.replace('_', '-')}/data-api"
        try:
            api.do("GET", data_api)
            api.do("DELETE", data_api)
            print("removed  Lakebase Data API (the Subscription API is the only way in)")
        except Exception:  # noqa: BLE001 - not enabled
            print("ok       Lakebase Data API off")
        # Roles from the Data API design: SP-named roles in `subscriber`, usable by the Data API's `authenticator`.
        members = (
            "SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member "
            "JOIN pg_roles g ON g.oid = m.roleid WHERE g.rolname = :g AND r.rolname !~ '^app_' AND r.rolname <> :me"
        )
        for (old,) in db.run(members, g="subscriber", me=me):
            db.run(f'REVOKE subscriber FROM "{old}"')
            print(f"revoked  {old} (the old direct-access role)")
        for (old,) in db.run(
            "SELECT g.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member "
            "JOIN pg_roles g ON g.oid = m.roleid WHERE r.rolname = 'authenticator'"
        ):
            db.run(f'REVOKE "{old}" FROM authenticator')
            print(f"revoked  {old} from the Data API's authenticator")
        hub_principals = {h["application_id"] for h in config.hubs.values()}  # a team hub's SP logs in as itself
        for a in config.apps.values():  # apps never log in to the registry: drop an old login role (an SP's only)
            p = a["principal"]
            if p in hub_principals or not is_service_principal(p):
                continue
            if db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=p):
                db.run(f'DROP ROLE "{p}"')
                print(f"dropped  {p} (an app's old login role; apps reach the registry only via the API)")

        if config.registry_catalog:
            _registry_catalog(w, config)

        wid = warehouse_id(w, config)
        columns = silver_columns() if config.target is None else table_columns(w, config.tables)
        apps = {name: _with_grants(a, columns) for name, a in config.apps.items()}
        for line in onboard_apps(db, apps):
            print(line)
        for name, a in apps.items():
            views = _views(name, config.catalog)
            sql(w, wid, f"CREATE SCHEMA IF NOT EXISTS {views} COMMENT 'Views {name} may subscribe to'")
            for source in a["sources"]:
                sql(w, wid, view_sql(views, source, config.tables[source], columns[source], a["fields"]))
            print(f"ok       app {name} ({a['principal']}) -> {a['topic']}; views {views}; "
                  f"templates {sorted(a['templates']) or 'none'}")  # fmt: skip

        # Subscription hubs: edp_shared and the team hubs (bring your own hub), and which one serves each app.
        for line in onboard_hubs(db, config.hubs, config.apps, me, config.catalog, config.hub_credential):
            print(line)
        grant_hub_workspace(w, wid, config.catalog, config.hubs, config.project)
        if config.hub_schema:
            schema = f"{config.catalog}.{config.hub_schema}"
            sql(w, wid, f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT 'The subscription hub pipelines (no tables)'")
            print(f"ok       schema {schema} (the hub pipelines' target)")
    finally:
        db.close()
    if config.target is None:
        write_example_settings(config, host)


def after_deploy(w, config: Config):
    """What needs the deployed Subscription API: its service principal acts for the onboarded apps and reads their
    views, and each app's principal may call it. Skipped, saying so, until the app exists."""
    try:
        api_app = w.apps.get(config.api_app)
    except Exception:  # noqa: BLE001 - not deployed yet
        api_app = None
    api_sp = api_app.service_principal_client_id if api_app else None
    if not api_sp:
        how = "make demo dev deploys it" if config.target is None else "deploy the bundle, then run this step again"
        print(f"skipped  Subscription API grants: app {config.api_app} isn't deployed yet ({how})")
        return
    db, _, me = connect(w, config)
    try:
        if not db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=api_sp):
            db.run("SELECT databricks_create_role(:r, 'SERVICE_PRINCIPAL')", r=api_sp)
        db.run(f'GRANT subscription_service TO "{api_sp}"')
    finally:
        db.close()
    print(f"ok       Subscription API {config.api_app} ({api_sp}) acts for onboarded apps; {api_app.url}")
    wid = warehouse_id(w, config)
    for name, a in config.apps.items():
        views = _views(name, config.catalog)
        sql(w, wid, f"GRANT USE CATALOG ON CATALOG {config.catalog} TO `{api_sp}`")
        sql(w, wid, f"GRANT USE SCHEMA, SELECT ON SCHEMA {views} TO `{api_sp}`")
        if a["principal"] != me.lower():  # you manage the app already: CAN_USE would only add noise to your grant
            w.apps.update_permissions(config.api_app, access_control_list=[can_use(a["principal"])])
        print(f"ok       app {name}: {a['principal']} may call {config.api_app}, which reads {views}")


def bootstrap(config: Config, before: bool = True, after: bool = True, w=None):
    """Both phases by default (the second skips itself until the Subscription API is deployed). Idempotent."""
    w = w or client(config)
    if before:
        before_deploy(w, config)
    if after:
        after_deploy(w, config)


def write_example_settings(config: Config, host: str):
    """examples/subscribers' settings for the platform's apps (Terraform's path only)."""
    catalog, schema, apps = config.catalog, config.schema, config.apps
    data_api_url = f"https://{host}/api/2.0/workspace/{config.workspace_id}/rest/{config.database}"  # the old bypass
    out = ROOT / "examples/subscribers/apps"
    out.mkdir(exist_ok=True)
    for name in apps:
        (out / f"{name}.properties").write_text(
            "# Written by `make registry` (rewritten every run). No secrets: the profile holds the SP's credentials.\n"
            "# The app needs only these two: it looks up the API's URL by app name, then gets its topic and\n"
            "# Event Hubs connection from the API.\n"
            f"# Its realtime rules run on subscription hub {apps[name].get('subscription_hub') or SHARED_HUB}.\n"
            f"databricks.profile   = {name}\n"
            f"api.app              = {config.api_app}\n"
            "# E2E checks (--e2e): all must be refused to this app.\n"
            f"e2e.forbidden_credential = {catalog}_eventhubs\n"
            f"e2e.forbidden_topic      = {catalog}.{schema}.silver_observations\n"
            f"e2e.own_topic            = {apps[name]['topic']}\n"
            f"e2e.base_schema          = {catalog}.{schema}\n"
            f"e2e.data_api_url         = {data_api_url}\n"
        )
        print(f"wrote    examples/subscribers/apps/{name}.properties")
    # The subscriber bundle (examples/subscribers/java/databricks.yml, the workspace E2E job) for the first app.
    if apps:
        name, a = sorted(apps.items())[0]
        overrides = ROOT / "examples/subscribers/java/.databricks/bundle/dev/variable-overrides.json"
        overrides.parent.mkdir(parents=True, exist_ok=True)
        overrides.write_text(json.dumps({
            "app": name, "app_id": a["principal"], "api_app": config.api_app,
            "forbidden_credential": f"{catalog}_eventhubs",
            "forbidden_topic": f"{catalog}.{schema}.silver_observations",
            "own_topic": a["topic"],
            "base_schema": f"{catalog}.{schema}", "data_api_url": data_api_url,
        }, indent=2))  # fmt: skip
        print(f"wrote    examples/subscribers/java/.databricks/bundle/dev/variable-overrides.json ({name})")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Create or repair the subscription registry (idempotent).")
    parser.add_argument("project", nargs="?", default="hl7-subscriptions", help="Lakebase project (Terraform's path)")
    parser.add_argument("--target", help="a target of the subscriptions bundle (subscriptions/targets/<env>.yml), "
                                         "instead of Terraform's outputs")  # fmt: skip
    parser.add_argument("--phase", choices=["before-deploy", "after-deploy"], help="one phase only (default: both)")
    args = parser.parse_args(argv)
    try:
        config = Config.from_target(args.target) if args.target else Config.from_terraform(args.project)
        bootstrap(config, before=args.phase != "after-deploy", after=args.phase != "before-deploy")
    except ConfigError as e:
        sys.exit(f"error: {e}")


if __name__ == "__main__":
    main()
