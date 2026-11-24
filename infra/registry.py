"""Idempotent subscription registry bootstrap (`make registry`, run by `make up` and `make demo`). Safe to re-run.

  Lakebase project  <registry_project> (created if missing; scales to zero)
  registry schema   src/registry/registry.sql (tables, owner trigger, RLS, API functions, monitor views, grants)
  UC catalog        <catalog>_registry: the registry database in Unity Catalog, read by the subscriptions dashboard
                    (schema subscription_monitor; platform only, nothing granted to apps)
  subscriber apps   terraform var subscriber_apps, per app:
                      - a NOLOGIN Postgres role app_<name> in `subscriber`, usable only by the Subscription API, and its
                        binding (service principal, hub, sources, fields) and its data templates
                        (subscriber_templates). Apps removed from Terraform lose it.
                      - UC views <catalog>.sub_<name>.<source> exposing only its granted fields: Databricks compiles
                        the app's SQL against them, and developers test rules there in the SQL editor
                      - CAN_USE on the Subscription API app
  subscription hubs edp_shared (the platform's; the main bundle deploys it) and each team hub of terraform var
                    subscriber_hubs (bring your own hub: the team deploys hub/ as its service principal), and which one
                    serves each app (subscriber_apps.<app>.subscription_hub). A team hub's SP gets a login role in
                    hub_<name> (in `hub`: it reads only its apps' rules and writes only its own rows), use of the
                    Lakebase project, and a schema <catalog>.hub_<name> for its pipeline. Hubs gone from Terraform are
                    deleted once their apps are back on edp_shared.
  Subscription API  its service principal joins `subscription_service` (so it can act as each app) and may read the
                    apps' views; the Lakebase Data API is turned off, so the API is the only way in
  app settings      examples/subscribers/apps/<app>.properties (gitignored) for the example apps, rewritten
                    every run (URLs change when the workspace is rebuilt). The profile name defaults to the app name.

Runs as you (the project owner), authenticated like the rest of the Makefile (DATABRICKS_HOST + Azure CLI). The
Subscription API app must exist (`make demo dev` deploys it, then runs this); until it does, its grants are skipped.
"""

import json
import os
import ssl
import subprocess
import sys
import time
from pathlib import Path

import pg8000.native
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.postgres import Catalog, CatalogCatalogSpec

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from hl7rt.subscriptions import SOURCES  # noqa: E402

PROJECT = sys.argv[1] if len(sys.argv) > 1 else "hl7-subscriptions"
BRANCH = f"projects/{PROJECT}/branches/production"
ENDPOINT = f"{BRANCH}/endpoints/primary"
DATABASE = "databricks_postgres"
API_APP = os.environ.get("HL7_API_APP", "hl7-subscriptions-dev")
WAREHOUSE = os.environ.get("HL7_WAREHOUSE", "Serverless Starter Warehouse")


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


def onboard_hubs(db, hubs: dict, apps: dict, me: str, catalog: str) -> list[str]:
    """Register the subscription hubs and which one serves each app's realtime rules: edp_shared (the platform's, run
    as the deployer `me`, which also runs every app's deferred rules) and each team hub of terraform var
    subscriber_hubs. Apps move before a hub that's gone from Terraform is deleted. Returns what it did, a line each."""
    done = []
    db.run(
        "INSERT INTO subscription_registry.hubs (name, principal, eventhubs_credential, runs_deferred) "
        "VALUES (:n, :p, :c, true) ON CONFLICT (name) DO UPDATE SET principal = excluded.principal, "
        "eventhubs_credential = excluded.eventhubs_credential",
        n=SHARED_HUB, p=me, c=f"{catalog}_eventhubs",
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


def grant_hub_workspace(w, warehouse_id: str, catalog: str, hubs: dict):
    """What a team hub's pipeline needs in the workspace, as its service principal: a schema of its own to target
    (<catalog>.hub_<name>; it writes no tables there) and use of the registry's Lakebase project. Its Event Hubs
    credential is infra/uc.sh's."""
    for name, h in hubs.items():
        sp, schema = h["application_id"], f"{catalog}.{hub_role(name)}"
        sql(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT 'Subscription hub {name}: its pipeline'")
        sql(w, warehouse_id, f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp}`")
        sql(w, warehouse_id, f"GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA {schema} TO `{sp}`")
        w.permissions.update("database-projects", PROJECT, access_control_list=[AccessControlRequest(
            service_principal_name=sp, permission_level=PermissionLevel.CAN_USE)])  # fmt: skip
        print(f"ok       hub {name}: schema {schema} and Lakebase project {PROJECT} for {sp}")


def sql(w, warehouse_id: str, statement: str):
    r = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        sys.exit(f"error: {statement[:80]}...: {r.status.error.message if r.status.error else r.status.state}")


def main():
    w = WorkspaceClient()
    api = w.api_client

    try:
        w.postgres.get_project(name=f"projects/{PROJECT}")
        print(f"ok       Lakebase project {PROJECT}")
    except Exception:  # noqa: BLE001 - not found
        api.do("POST", "/api/2.0/postgres/projects", query={"project_id": PROJECT}, body={"spec": {
            "display_name": PROJECT, "pg_version": 17,
            "default_endpoint_settings": {"autoscaling_limit_min_cu": 0.5, "autoscaling_limit_max_cu": 1,
                                          "suspend_timeout_duration": "300s"}}})  # fmt: skip
        print(f"created  Lakebase project {PROJECT} (0.5-1 CU, suspends after 5 idle minutes)")

    host = None
    for _ in range(90):
        try:
            host = w.postgres.get_endpoint(name=ENDPOINT).status.hosts.host
            if host:
                break
        except Exception:  # noqa: BLE001 - still provisioning
            pass
        time.sleep(10)
    if not host:
        sys.exit(f"error: endpoint {ENDPOINT} has no host yet; rerun make registry")

    me = w.current_user.me().user_name
    token = w.postgres.generate_database_credential(endpoint=ENDPOINT).token
    db = pg8000.native.Connection(me, host=host, database=DATABASE, password=token,
                                  ssl_context=ssl.create_default_context())  # fmt: skip
    db.run("CREATE EXTENSION IF NOT EXISTS databricks_auth")
    db.run((ROOT / "src/registry/registry.sql").read_text())
    print("ok       registry schema (src/registry/registry.sql)")

    # The Lakebase Data API was the apps' way in before the Subscription API; turn it off so it can't be a bypass.
    data_api = f"/api/2.0/postgres/{BRANCH}/databases/{DATABASE.replace('_', '-')}/data-api"
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
    hubs = tf_output("subscriber_hubs")
    hub_principals = {h["application_id"] for h in hubs.values()}  # a team hub's SP logs in as itself (onboard_hubs)
    for app in tf_output("subscriber_apps").values():  # apps never log in to the registry: drop an old login role
        if app["application_id"] in hub_principals:
            continue
        if db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=app["application_id"]):
            db.run(f'DROP ROLE "{app["application_id"]}"')
            print(
                f"dropped  {app['application_id']} (an app's old login role; apps reach the registry only via the API)"
            )

    catalog = tf_output("catalog")
    registry_catalog = f"{catalog}_registry"
    # After a sandbox wipe the shared metastore keeps the old registration, made for the deleted workspace's database.
    # It only feeds the subscriptions dashboard, so say how to replace it and carry on.
    try:
        old = w.catalogs.get(registry_catalog)
    except Exception:  # noqa: BLE001 - not registered
        old = None
    built_for = (old.properties or {}).get("workspace_id") if old else None
    if built_for and built_for != str(tf_output("workspace_id")):
        print(f"STALE    catalog {registry_catalog} is registered for workspace {built_for} (gone), so the "
              "subscriptions dashboard has no data. Remove it, then rerun make registry:\n"
              f"         databricks api delete /api/2.0/postgres/catalogs/{registry_catalog}")  # fmt: skip
    else:
        try:
            w.postgres.get_catalog(name=f"catalogs/{registry_catalog}")
            print(f"ok       catalog {registry_catalog} (the registry database, for the subscriptions dashboard)")
        except Exception:  # noqa: BLE001 - not registered yet
            w.postgres.create_catalog(
                catalog=Catalog(spec=CatalogCatalogSpec(postgres_database=DATABASE, branch=BRANCH)),
                catalog_id=registry_catalog,
            ).wait()
            print(f"created  catalog {registry_catalog} (the registry database, for the subscriptions dashboard)")

    try:
        api_app = w.apps.get(API_APP)
    except Exception:  # noqa: BLE001 - not deployed yet
        api_app = None
        print(f"skipped  Subscription API grants: app {API_APP} isn't deployed yet (make demo dev deploys it)")
    api_sp = api_app.service_principal_client_id if api_app else None
    if api_sp:
        if not db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=api_sp):
            db.run("SELECT databricks_create_role(:r, 'SERVICE_PRINCIPAL')", r=api_sp)
        db.run(f'GRANT subscription_service TO "{api_sp}"')
        print(f"ok       Subscription API {API_APP} ({api_sp}) acts for onboarded apps; {api_app.url}")

    warehouse_id = next(x.id for x in w.warehouses.list() if x.name == WAREHOUSE)
    schema = tf_output("schema")
    apps = tf_output("subscriber_apps")
    removed = db.run(
        "DELETE FROM subscription_registry.apps WHERE app_role <> ALL (CAST(:k AS text[])) RETURNING app_name",
        k=[role(n) for n in apps],
    )
    for (gone,) in removed:
        print(f"removed  app {gone} (no longer in subscriber_apps, or its old binding)")
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
            r=r, n=name, t=a["hub"], s=a["sources"], f=a["fields"], sp=a["application_id"], c=credential(name),
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

        views = f"{catalog}.sub_{name.replace('-', '_')}"
        sql(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {views} COMMENT 'Views {name} may subscribe to'")
        for source in a["sources"]:
            cols = [c.name for c in SOURCES[source] if c.name in a["fields"]]
            sql(w, warehouse_id, f"CREATE OR REPLACE VIEW {views}.{source} AS SELECT {', '.join(cols)} "
                                 f"FROM {catalog}.{schema}.{source}")  # fmt: skip
        if api_sp:
            sql(w, warehouse_id, f"GRANT USE CATALOG ON CATALOG {catalog} TO `{api_sp}`")
            sql(w, warehouse_id, f"GRANT USE SCHEMA, SELECT ON SCHEMA {views} TO `{api_sp}`")
            w.apps.update_permissions(API_APP, access_control_list=[AppAccessControlRequest(
                service_principal_name=a["application_id"], permission_level=AppPermissionLevel.CAN_USE)])  # fmt: skip
        print(
            f"ok       app {name} ({a['application_id']}) -> {a['hub']}; views {views}; "
            f"templates {sorted(templates) or 'none'}"
        )

    # Subscription hubs: edp_shared and the team hubs (bring your own hub), and which one serves each app.
    for line in onboard_hubs(db, hubs, apps, me, catalog):
        print(line)
    grant_hub_workspace(w, warehouse_id, catalog, hubs)

    data_api_url = f"https://{host}/api/2.0/workspace/{tf_output('workspace_id')}/rest/{DATABASE}"  # the old bypass
    out = ROOT / "examples/subscribers/apps"
    out.mkdir(exist_ok=True)
    for name in apps:
        (out / f"{name}.properties").write_text(
            "# Written by `make registry` (rewritten every run). No secrets: the profile holds the SP's credentials.\n"
            "# The app needs only these two: it looks up the API's URL by app name, then gets its topic and\n"
            "# Event Hubs connection from the API.\n"
            f"# Its realtime rules run on subscription hub {apps[name].get('subscription_hub') or SHARED_HUB}.\n"
            f"databricks.profile   = {name}\n"
            f"api.app              = {API_APP}\n"
            "# E2E checks (--e2e): all must be refused to this app.\n"
            f"e2e.forbidden_credential = {catalog}_eventhubs\n"
            f"e2e.forbidden_topic      = {catalog}.{schema}.silver_observations\n"
            f"e2e.own_topic            = {apps[name]['hub']}\n"
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
            "app": name, "app_id": a["application_id"], "api_app": API_APP,
            "forbidden_credential": f"{catalog}_eventhubs",
            "forbidden_topic": f"{catalog}.{schema}.silver_observations",
            "own_topic": a["hub"],
            "base_schema": f"{catalog}.{schema}", "data_api_url": data_api_url,
        }, indent=2))  # fmt: skip
        print(f"wrote    examples/subscribers/java/.databricks/bundle/dev/variable-overrides.json ({name})")


if __name__ == "__main__":
    main()
