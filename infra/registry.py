"""Idempotent subscription registry bootstrap (`make registry`, run by `make up`). Safe to re-run.

  Lakebase project <registry_project> (created if missing; scales to zero)
  registry schema   src/registry/registry.sql (tables, validation, API functions, grants)
  source columns    every gold source's columns and types, from hl7rt.gold
  Data API          exposes schema subscription_api only
  UC catalog        <catalog>_registry: the registry database in Unity Catalog, read by the subscriptions dashboard
                    (schema subscription_monitor; platform only, nothing granted to apps)
  subscriber apps   terraform var subscriber_apps: a Postgres role per app SP, member of `subscriber`, usable by the
                    Data API, plus its binding (hub, sources, fields). Apps removed from Terraform lose their binding,
                    so they can't subscribe and the hub stops serving their rules.
  app settings      src/subscriber/apps/<app>.properties (gitignored) for the Java app: the registry URL changes when
                    the workspace is rebuilt, so this is rewritten every run. The profile name defaults to the app name.

Runs as you (the project owner), authenticated like the rest of the Makefile (DATABRICKS_HOST + Azure CLI).
"""

import json
import ssl
import subprocess
import sys
import time
from pathlib import Path

import pg8000.native
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Catalog, CatalogCatalogSpec

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from hl7rt.subscriptions import registry_columns  # noqa: E402

PROJECT = sys.argv[1] if len(sys.argv) > 1 else "hl7-subscriptions"
BRANCH = f"projects/{PROJECT}/branches/production"
ENDPOINT = f"{BRANCH}/endpoints/primary"
DATABASE = "databricks_postgres"
SCHEMA = "subscription_api"


def tf_output(name: str):
    out = subprocess.run(["terraform", "-chdir=infra/azure", "output", "-json", name], cwd=ROOT,
                         capture_output=True, text=True, check=True)  # fmt: skip
    return json.loads(out.stdout)


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

    cols = registry_columns()
    db.run("DELETE FROM subscription_registry.source_columns")
    for source, column, kind in cols:
        db.run("INSERT INTO subscription_registry.source_columns VALUES (:s, :c, :t)", s=source, c=column, t=kind)
    print(f"ok       {len(cols)} columns across {len({c[0] for c in cols})} gold sources")

    data_api = f"/api/2.0/postgres/{BRANCH}/databases/{DATABASE.replace('_', '-')}/data-api"
    try:
        status = api.do("GET", data_api)["status"]
        print("ok       Data API")
    except Exception:  # noqa: BLE001 - not enabled yet
        status = api.do("POST", data_api, body={"spec": {"db_schemas": [SCHEMA]}})["response"]["status"]
        print(f"created  Data API (exposes {SCHEMA} only)")
    db.run("NOTIFY pgrst, 'reload schema'")  # pick up changed function signatures

    # The registry as a Unity Catalog catalog, so the subscriptions dashboard can read subscription_monitor from SQL.
    registry_catalog = f"{tf_output('catalog')}_registry"
    try:
        w.postgres.get_catalog(name=f"catalogs/{registry_catalog}")
        print(f"ok       catalog {registry_catalog} (the registry database, for the subscriptions dashboard)")
    except Exception:  # noqa: BLE001 - not registered yet
        w.postgres.create_catalog(
            catalog=Catalog(spec=CatalogCatalogSpec(postgres_database=DATABASE, branch=BRANCH)),
            catalog_id=registry_catalog,
        ).wait()
        print(f"created  catalog {registry_catalog} (the registry database, for the subscriptions dashboard)")

    apps = tf_output("subscriber_apps")
    for name, a in apps.items():
        role = a["application_id"]
        if not db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=role):
            db.run("SELECT databricks_create_role(:r, 'SERVICE_PRINCIPAL')", r=role)
        db.run(f'GRANT subscriber TO "{role}"')
        db.run(f'GRANT "{role}" TO authenticator')  # lets the Data API act as this SP
        db.run(
            "INSERT INTO subscription_registry.apps VALUES (:r, :n, :t, CAST(:s AS text[]), CAST(:f AS text[])) "
            "ON CONFLICT (app_role) DO UPDATE SET app_name = excluded.app_name, "
            "destination_topic = excluded.destination_topic, "
            "allowed_sources = excluded.allowed_sources, allowed_fields = excluded.allowed_fields",
            r=role, n=name, t=a["hub"], s=a["sources"], f=a["fields"],
        )  # fmt: skip
        print(f"ok       app {name} ({role}) -> {a['hub']}")
    removed = db.run(
        "DELETE FROM subscription_registry.apps WHERE app_role <> ALL (CAST(:k AS text[])) RETURNING app_name",
                     k=[a["application_id"] for a in apps.values()])  # fmt: skip
    for (gone,) in removed:
        print(f"removed  app {gone} (no longer in subscriber_apps)")

    url = status.get("url", "")
    ns, catalog, schema = tf_output("eh_namespace"), tf_output("catalog"), tf_output("schema")
    out = ROOT / "src/subscriber/apps"
    out.mkdir(exist_ok=True)
    for name in apps:
        (out / f"{name}.properties").write_text(
            "# Written by `make registry` (rewritten every run). No secrets: the profile holds the SP's credentials.\n"
            f"databricks.profile   = {name}\n"
            f"registry.url         = {url}\n"
            f"registry.endpoint    = {ENDPOINT}\n"
            f"eventhubs.namespace  = {ns}\n"
            f"eventhubs.credential = sub_{name.replace('-', '_')}_eventhubs\n"
            "# E2E checks (--e2e): both must be refused to this app.\n"
            f"e2e.forbidden_credential = {catalog}_eventhubs\n"
            f"e2e.forbidden_topic      = {catalog}.{schema}.gold_observations\n"
            f"e2e.own_topic            = {apps[name]['hub']}\n"
        )
        print(f"wrote    src/subscriber/apps/{name}.properties")
    # The subscriber bundle (src/subscriber/databricks.yml, the workspace E2E job) for the first app.
    if apps:
        name, a = sorted(apps.items())[0]
        overrides = ROOT / "src/subscriber/.databricks/bundle/dev/variable-overrides.json"
        overrides.parent.mkdir(parents=True, exist_ok=True)
        overrides.write_text(
            json.dumps(
                {
                    "app": name,
                    "app_id": a["application_id"],
                    "credential": f"sub_{name.replace('-', '_')}_eventhubs",
                    "registry_url": url,
                    "registry_endpoint": ENDPOINT,
                    "eh_namespace": ns,
                    "forbidden_credential": f"{catalog}_eventhubs",
                    "forbidden_topic": f"{catalog}.{schema}.gold_observations",
                    "own_topic": a["hub"],
                },
                indent=2,
            )
        )
        print(f"wrote    src/subscriber/.databricks/bundle/dev/variable-overrides.json ({name})")


if __name__ == "__main__":
    main()
