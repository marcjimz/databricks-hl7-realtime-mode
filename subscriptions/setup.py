# Databricks notebook source
# MAGIC %md
# MAGIC # The subscription layer: setup
# MAGIC
# MAGIC Sets up the subscription layer in this workspace: the Lakebase registry, the Subscription API (a Databricks
# MAGIC App), and `edp_shared`, the hub that matches your Delta tables' records to the apps' rules. Everything comes
# MAGIC from your target file, `subscriptions/targets/<target>.yml` in this Git folder. Start from `target.example.yml`;
# MAGIC the `targets/` folder is gitignored, so your values stay out of git. The bundle deploys with the same file.
# MAGIC
# MAGIC Set the **target** widget, then run the cells in order:
# MAGIC
# MAGIC 1. **Your target**: loads and prints the settings the steps below use.
# MAGIC 2. **Before you deploy the bundle**: the registry, your apps (roles, bindings, views), and the hub's schema.
# MAGIC 3. Deploy the bundle in `subscriptions/` (target `<target>`), and start the App.
# MAGIC 4. **After you deploy the bundle and start the App**: the App's grants, and access to it for each app.
# MAGIC 5. **Status** and **Stop everything** whenever you need them.
# MAGIC
# MAGIC Every step can run again safely. Use **environment version 4** (Python 3.12) in the notebook's Environment
# MAGIC panel. Older environments (Python 3.10) work too, with the packages the next cells install.

# COMMAND ----------

# ruff: noqa: E402, F821, UP017, UP036
# dbutils is the notebook's; imports follow the cells that install them; the code runs on Python 3.10 too.
import sys

print(sys.version)
if sys.version_info < (3, 10):
    raise RuntimeError("This notebook needs Python 3.10 or later: pick environment version 4 in the Environment panel")

# COMMAND ----------

# MAGIC %pip install "pg8000>=1.31" "databricks-sdk>=0.159" "pyyaml>=6.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("target", "", "Target (subscriptions/targets/<target>.yml)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Your target
# MAGIC
# MAGIC Reads `subscriptions/targets/<target>.yml` over the bundle's defaults, as the bundle does, and prints the
# MAGIC result. It holds no secrets.

# COMMAND ----------

import json
import os
from dataclasses import asdict
from pathlib import Path


def repo_root() -> Path:
    """This Git folder's root. A notebook's working directory is its own folder; else its path says where it is."""
    starts = [Path(os.getcwd())]
    try:
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        starts.append(Path("/Workspace" + path).parent)
    except Exception:  # noqa: BLE001 - not in a notebook context
        pass
    for start in starts:
        for d in [start, *start.parents]:
            if (d / "subscriptions/databricks.yml").is_file() and (d / "infra/registry.py").is_file():
                return d
    raise RuntimeError("Can't find the repo (subscriptions/databricks.yml): run this notebook from its Git folder")


ROOT = repo_root()
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "infra")]
from databricks.sdk import WorkspaceClient

import registry  # infra/registry.py: the same steps as `make subscriptions`

TARGET = dbutils.widgets.get("target").strip()
if not TARGET:
    raise ValueError("Set the target widget: the name of your subscriptions/targets/<target>.yml")
config = registry.Config.from_target(TARGET)
w = WorkspaceClient()  # you, in this workspace
host = config.workspace.get("host", "").rstrip("/").lower()
if host and host != w.config.host.rstrip("/").lower():
    raise RuntimeError(f"Target {TARGET} is for {host}, but this notebook runs in {w.config.host}")
print(json.dumps(asdict(config), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Before you deploy the bundle
# MAGIC
# MAGIC Creates (or repairs) the Lakebase project and the registry in it, onboards the apps in your target (each one's
# MAGIC role, binding, and templates), creates their views `<catalog>.sub_<app>.<source>` over your tables, and the hub
# MAGIC pipelines' schema. A new Lakebase project takes a few minutes to get its endpoint; this waits for it.

# COMMAND ----------

registry.before_deploy(w, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deploy the bundle
# MAGIC
# MAGIC Deploy the bundle in this Git folder's `subscriptions/` folder with target `<target>` (open
# MAGIC `subscriptions/databricks.yml`, then deploy from the bundle's deployments panel). It creates the Subscription
# MAGIC API app and the two hub pipelines, `hl7rt-<target>-hub-edp_shared` and `hl7rt-<target>-hub-edp_shared-deferred`.
# MAGIC Then start the app (its page under Compute, Apps), and continue below once it is running.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. After you deploy the bundle and start the App
# MAGIC
# MAGIC The App's service principal may act for every onboarded app in the registry and read the apps' views, and each
# MAGIC app's principal may call the App. Run it again whenever you change the apps in your target (after step 2).

# COMMAND ----------

registry.after_deploy(w, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Status
# MAGIC
# MAGIC The App, both hub pipelines, and the hubs' heartbeats in the registry: a hub writes one per source every
# MAGIC micro-batch, so an old one means a stopped hub, not a quiet table.

# COMMAND ----------

from datetime import datetime, timezone

PIPELINES = [f"hl7rt-{TARGET}-hub-{registry.SHARED_HUB}", f"hl7rt-{TARGET}-hub-{registry.SHARED_HUB}-deferred"]


def pipelines() -> dict:
    """The hub pipelines the bundle deployed, by name."""
    found = {}
    for name in PIPELINES:
        for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
            if p.name == name:
                found[name] = p
    return found


try:
    app = w.apps.get(config.api_app)
    state = app.compute_status.state.value if app.compute_status and app.compute_status.state else "?"
    print(f"app       {app.name}: compute {state}, {app.url}")
except Exception as e:  # noqa: BLE001 - not deployed yet
    print(f"app       {config.api_app}: not deployed ({e})")
found = pipelines()
for name in PIPELINES:
    p = found.get(name)
    print(f"pipeline  {name}: {p.state.value if p and p.state else 'not deployed'}")

db, _, _ = registry.connect(w, config)
try:
    beats = db.run("SELECT hub, tier, source, last_batch_at, batch_id, rules FROM subscription_registry.hub_heartbeat "
                   "ORDER BY hub, tier, source")  # fmt: skip
finally:
    db.close()
now = datetime.now(timezone.utc)
for hub, tier, source, at, batch, rules in beats:
    print(f"heartbeat {hub} {tier} {source}: batch {batch}, {rules} rules, {(now - at).total_seconds():.0f} s ago")
if not beats:
    print("heartbeat none yet (the hubs write one per source once they run a batch)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop everything
# MAGIC
# MAGIC Stops both hub pipelines and the App, so nothing bills while you're away. The registry's Lakebase project
# MAGIC suspends by itself after 5 idle minutes. Start them again from the workspace, or redeploy.

# COMMAND ----------

for name, p in pipelines().items():
    w.pipelines.stop(p.pipeline_id)
    print(f"stopped   {name}")
try:
    w.apps.stop(config.api_app)
    print(f"stopped   {config.api_app}")
except Exception as e:  # noqa: BLE001 - not deployed, or stopped already
    print(f"skipped   {config.api_app}: {e}")
