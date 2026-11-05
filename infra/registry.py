"""Idempotent subscription registry bootstrap (`make registry`, run by `make up` and `make demo`). Safe to re-run.

  Lakebase project  <registry_project> (created if missing; scales to zero)
  registry schema   src/registry/registry.sql (tables, owner trigger, RLS, API functions, monitor views, grants)
  UC catalog        <catalog>_registry: the registry database in Unity Catalog, read by the subscriptions dashboard
                    (schema subscription_monitor; platform only, nothing granted to apps)
  subscriber apps   terraform var subscriber_apps, per app:
                      - a NOLOGIN Postgres role app_<name> in `subscriber`, usable only by the Subscription API, and its
                        binding (service principal, hub, sources, fields). Apps removed from Terraform lose it.
                      - UC views <catalog>.sub_<name>.<source> exposing only its granted fields: Databricks compiles
                        the app's SQL against them, and developers test rules there in the SQL editor
                      - CAN_USE on the Subscription API app
  Subscription API  its service principal joins `subscription_service` (so it can act as each app) and may read the
                    apps' views; the Lakebase Data API is turned off, so the API is the only way in
  app settings      src/subscriber/apps/<app>.properties (gitignored) for the Java app, rewritten every run (URLs change
                    when the workspace is rebuilt). The profile name defaults to the app name.

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
    for app in tf_output("subscriber_apps").values():  # apps never log in to the registry: drop an old login role
        if db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=app["application_id"]):
            db.run(f'DROP ROLE "{app["application_id"]}"')
            print(
                f"dropped  {app['application_id']} (an app's old login role; apps reach the registry only via the API)"
            )

    catalog = tf_output("catalog")
    registry_catalog = f"{catalog}_registry"
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
            "allowed_fields, service_principal) VALUES (:r, :n, :t, CAST(:s AS text[]), CAST(:f AS text[]), :sp) "
            "ON CONFLICT (app_role) DO UPDATE SET app_name = excluded.app_name, "
            "destination_topic = excluded.destination_topic, allowed_sources = excluded.allowed_sources, "
            "allowed_fields = excluded.allowed_fields, service_principal = excluded.service_principal",
            r=r, n=name, t=a["hub"], s=a["sources"], f=a["fields"], sp=a["application_id"],
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
        print(f"ok       app {name} ({a['application_id']}) -> {a['hub']}; views {views}")

    ns = tf_output("eh_namespace")
    data_api_url = f"https://{host}/api/2.0/workspace/{tf_output('workspace_id')}/rest/{DATABASE}"  # the old bypass
    api_url = api_app.url if api_app else ""
    out = ROOT / "src/subscriber/apps"
    out.mkdir(exist_ok=True)
    for name in apps:
        (out / f"{name}.properties").write_text(
            "# Written by `make registry` (rewritten every run). No secrets: the profile holds the SP's credentials.\n"
            f"databricks.profile   = {name}\n"
            f"api.url              = {api_url}\n"
            f"eventhubs.namespace  = {ns}\n"
            f"eventhubs.credential = sub_{name.replace('-', '_')}_eventhubs\n"
            "# E2E checks (--e2e): all must be refused to this app.\n"
            f"e2e.forbidden_credential = {catalog}_eventhubs\n"
            f"e2e.forbidden_topic      = {catalog}.{schema}.gold_observations\n"
            f"e2e.own_topic            = {apps[name]['hub']}\n"
            f"e2e.base_schema          = {catalog}.{schema}\n"
            f"e2e.data_api_url         = {data_api_url}\n"
        )
        print(f"wrote    src/subscriber/apps/{name}.properties")
    # The subscriber bundle (src/subscriber/databricks.yml, the workspace E2E job) for the first app.
    if apps:
        name, a = sorted(apps.items())[0]
        overrides = ROOT / "src/subscriber/.databricks/bundle/dev/variable-overrides.json"
        overrides.parent.mkdir(parents=True, exist_ok=True)
        overrides.write_text(json.dumps({
            "app": name, "app_id": a["application_id"], "credential": f"sub_{name.replace('-', '_')}_eventhubs",
            "api_url": api_url, "eh_namespace": ns, "forbidden_credential": f"{catalog}_eventhubs",
            "forbidden_topic": f"{catalog}.{schema}.gold_observations", "own_topic": a["hub"],
            "base_schema": f"{catalog}.{schema}", "data_api_url": data_api_url,
        }, indent=2))  # fmt: skip
        print(f"wrote    src/subscriber/.databricks/bundle/dev/variable-overrides.json ({name})")


if __name__ == "__main__":
    main()
