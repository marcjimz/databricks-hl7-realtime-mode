"""Idempotent live Power BI dashboard deploy (`make powerbi`). Safe to re-run.

  Databricks (this workspace)
    views           src/dashboards/powerbi/views.sql -> <catalog>.powerbi (checked against definition.TABLES)
    read-only SP    HL7_POWERBI_SP (default sp-hl7-powerbi, a Databricks-managed service principal you create once):
                    USE CATALOG on <catalog>, USE SCHEMA + SELECT on <catalog>.powerbi only, CAN_USE on the warehouse,
                    and the databricks-sql-access entitlement (SQL only)
  Fabric (the tenant and capacity in infra/fabric's Terraform outputs; `make fabric-up` creates them)
    workspace       HL7_POWERBI_WORKSPACE (default hl7-realtime), on that capacity
    semantic model  DirectQuery over the views (definition.model_parts), created or updated in place
    report          three pages, automatic page refresh (definition.report_parts), created or updated in place

Runs as you: the Databricks side like the rest of the Makefile (DATABRICKS_HOST + Azure CLI), the Fabric side with an
Azure CLI token for the Fabric tenant (`az login --tenant <tenant_id>` once). The model reaches Databricks across
tenants as the read-only SP: enter its client ID and OAuth secret once in the model's settings (Data source
credentials -> Databricks Client Credentials). No secret passes through this script.
"""

import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from string import Template

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import Patch, PatchOp, PatchSchema
from databricks.sdk.service.sql import WarehouseAccessControlRequest, WarehousePermissionLevel

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "src/dashboards/powerbi"
_spec = importlib.util.spec_from_file_location("powerbi_definition", DIR / "definition.py")
definition = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(definition)

SP = os.environ.get("HL7_POWERBI_SP", "sp-hl7-powerbi")
WORKSPACE = os.environ.get("HL7_POWERBI_WORKSPACE", "hl7-realtime")
ITEM = os.environ.get("HL7_POWERBI_ITEM", "hl7-realtime")  # the model's and the report's display name
WAREHOUSE = os.environ.get("HL7_WAREHOUSE", "Serverless Starter Warehouse")
FABRIC = "https://api.fabric.microsoft.com/v1"


def tf_output(stack: str, name: str):
    out = subprocess.run(["terraform", f"-chdir=infra/{stack}", "output", "-json", name], cwd=ROOT,
                         capture_output=True, text=True, check=True)  # fmt: skip
    return json.loads(out.stdout)


def sql(w, warehouse_id: str, statement: str):
    r = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        sys.exit(f"error: {statement[:80]}...: {r.status.error.message if r.status.error else r.status.state}")


def databricks_side(w) -> tuple[str, str, str]:
    """Views, their check against the model, and the read-only SP's grants. Returns (host, http_path, catalog)."""
    catalog, schema = tf_output("azure", "catalog"), tf_output("azure", "schema")
    warehouse = next(x for x in w.warehouses.list() if x.name == WAREHOUSE)
    rendered = Template((DIR / "views.sql").read_text()).substitute(
        catalog=catalog, schema=schema, registry_catalog=f"{catalog}_registry"
    )
    for statement in rendered.split(";\n\n"):
        body = "\n".join(line for line in statement.splitlines() if not line.startswith("--")).strip().rstrip(";")
        if body:
            sql(w, warehouse.id, body)
    print(f"ok       views {catalog}.powerbi ({len(definition.TABLES)}, src/dashboards/powerbi/views.sql)")

    for view, columns in definition.TABLES.items():
        actual = {c.name: c.type_text.split("(")[0] for c in w.tables.get(f"{catalog}.powerbi.{view}").columns}
        if actual != columns:
            sys.exit(f"error: {view} has columns {actual}; definition.TABLES says {columns}: update one of them")

    sps = list(w.service_principals.list(filter=f'displayName eq "{SP}"'))
    if not sps:
        sys.exit(f"error: no service principal {SP}: create it first (README: Live Power BI dashboard)")
    app_id = sps[0].application_id
    if "databricks-sql-access" not in {e.value for e in sps[0].entitlements or []}:
        w.service_principals.patch(sps[0].id, schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                                   operations=[Patch(op=PatchOp.ADD, path="entitlements",
                                                     value=[{"value": "databricks-sql-access"}])])  # fmt: skip
        print(f"granted  {SP} the databricks-sql-access entitlement (SQL warehouses only; no workspace UI)")
    sql(w, warehouse.id, f"GRANT USE CATALOG ON CATALOG {catalog} TO `{app_id}`")
    sql(w, warehouse.id, f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.powerbi TO `{app_id}`")
    w.warehouses.update_permissions(warehouse.id, access_control_list=[WarehouseAccessControlRequest(
        service_principal_name=app_id, permission_level=WarehousePermissionLevel.CAN_USE)])  # fmt: skip
    print(f"ok       {SP} ({app_id}) reads {catalog}.powerbi only; CAN_USE on warehouse {WAREHOUSE}")
    return w.config.host.removeprefix("https://").rstrip("/"), warehouse.odbc_params.path, catalog


class Fabric:
    def __init__(self, tenant: str):
        self.token = subprocess.run(
            ["az", "account", "get-access-token", "--tenant", tenant, "--resource", "https://api.fabric.microsoft.com",
             "--query", "accessToken", "-o", "tsv"], capture_output=True, text=True)  # fmt: skip
        if self.token.returncode != 0:
            sys.exit(f"error: no Azure CLI token for the Fabric tenant: az login --tenant {tenant}")  # fmt: skip
        self.token = self.token.stdout.strip()

    def call(self, method: str, path: str, body=None):
        url = path if path.startswith("http") else FABRIC + path
        req = urllib.request.Request(url, method=method, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Authorization": f"Bearer {self.token}",
                                              "Content-Type": "application/json"})  # fmt: skip
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                text = r.read()
                status, headers = r.status, r.headers
        except urllib.error.HTTPError as e:
            sys.exit(f"error: Fabric {method} {path}: {e.code} {e.read().decode(errors='replace')[:1000]}")
        if status == 202 and headers.get("Location"):  # a long-running operation: wait for it
            for _ in range(100):
                time.sleep(int(headers.get("Retry-After", 3)))
                op = self.call("GET", headers["Location"])
                if op.get("status") == "Succeeded":
                    return op
                if op.get("status") == "Failed":
                    sys.exit(f"error: Fabric {method} {path}: {json.dumps(op.get('error', op))[:1500]}")
            sys.exit(f"error: Fabric {method} {path}: still running")
        return json.loads(text) if text else {}

    def find(self, path: str, name: str):
        return next((x for x in self.call("GET", path).get("value", []) if x["displayName"] == name), None)

    def upsert(self, workspace: str, kind: str, name: str, fmt: str, parts: list[dict]) -> str:
        """Create the item, or replace the definition of the existing one (its ID, URL, and credentials stay)."""
        body = {"definition": {"format": fmt, "parts": parts}}
        item = self.find(f"/workspaces/{workspace}/{kind}", name)
        if item:
            self.call("POST", f"/workspaces/{workspace}/{kind}/{item['id']}/updateDefinition", body)
            print(f"updated  {kind[:-1]} {name}")
            return item["id"]
        self.call("POST", f"/workspaces/{workspace}/{kind}", {"displayName": name, **body})
        print(f"created  {kind[:-1]} {name}")
        return self.find(f"/workspaces/{workspace}/{kind}", name)["id"]


def main():
    host, http_path, catalog = databricks_side(WorkspaceClient())

    fabric = Fabric(tf_output("fabric", "tenant_id"))
    capacity_name = tf_output("fabric", "capacity_name")
    capacity = fabric.find("/capacities", capacity_name)
    if not capacity:
        sys.exit(f"error: Fabric capacity {capacity_name} not found (make fabric-up), or you aren't its admin")
    if capacity["state"] != "Active":
        sys.exit(f"error: Fabric capacity {capacity_name} is {capacity['state']}: make fabric-resume")
    ws = fabric.find("/workspaces", WORKSPACE)
    if not ws:
        ws = fabric.call("POST", "/workspaces", {"displayName": WORKSPACE, "capacityId": capacity["id"]})
        print(f"created  Fabric workspace {WORKSPACE} on capacity {capacity_name}")
    elif ws.get("capacityId") != capacity["id"]:
        fabric.call("POST", f"/workspaces/{ws['id']}/assignToCapacity", {"capacityId": capacity["id"]})
        print(f"moved    Fabric workspace {WORKSPACE} to capacity {capacity_name}")
    else:
        print(f"ok       Fabric workspace {WORKSPACE} on capacity {capacity_name}")

    model = fabric.upsert(ws["id"], "semanticModels", ITEM, "TMDL", definition.model_parts(host, http_path, catalog))
    report = fabric.upsert(ws["id"], "reports", ITEM, "PBIR", definition.report_parts(model))
    print(f"report:  https://app.powerbi.com/groups/{ws['id']}/reports/{report}  (re-queries every "
          f"{definition.REFRESH_SECONDS} s if the capacity allows it: Admin portal -> Capacity settings -> "
          f"{capacity_name} -> Power BI workloads -> Automatic page refresh minimum; a new capacity starts at "
          "5 min)")  # fmt: skip
    print(f"model:   https://app.powerbi.com/groups/{ws['id']}/datasets/{model}/details  (first time: Settings -> "
          f"Data source credentials -> Databricks Client Credentials, as {SP})")  # fmt: skip


if __name__ == "__main__":
    main()
