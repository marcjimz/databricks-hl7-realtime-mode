# Databricks notebook source
# MAGIC %md
# MAGIC # Subscribe as yourself
# MAGIC
# MAGIC This runs the example subscriber (`subscriber.py`, in this folder) as **you**, not a service principal
# MAGIC (`identity=user`). It declares the rules in `RULES` to the Subscription API, waits until the subscription hub runs
# MAGIC them, then prints each notification that arrives on your app's Event Hub for `seconds`. The last cell cancels them.
# MAGIC
# MAGIC **Before you run it**
# MAGIC - The platform team has onboarded you as a subscriber app, with your email as its principal.
# MAGIC - You have **CAN USE** on the Subscription API's Databricks App (the `api_app` widget).
# MAGIC - You have **ACCESS** on your app's Unity Catalog service credential (it issues your Event Hubs token).
# MAGIC - Serverless compute with **Environment version 4** (Python 3.12), set in the notebook's Environment panel. The
# MAGIC   API client needs Python 3.11 or later; environment version 1 is Python 3.10.
# MAGIC - PyPI is reachable: the cells below install the example's packages, then generate its API client from
# MAGIC   `docs/api/openapi.json`. Open this notebook from a Git folder of the repo, so both are there.

# COMMAND ----------

# ruff: noqa: E402, E501, F821, UP036 - a notebook: later imports, a long %pip line, its globals, any Python
import sys

print(sys.version)
if sys.version_info < (3, 11):
    raise RuntimeError(f"Python {sys.version_info.major}.{sys.version_info.minor}: the example needs 3.11 or later. "
                       "Pick Environment version 4 in the notebook's Environment panel, then run again.")  # fmt: skip

# COMMAND ----------

# MAGIC %pip install confluent-kafka==2.16.0 openapi-python-client==0.30.0 httpx2==2.16.0 attrs==26.1.0 databricks-sdk==0.160.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("api_app", "hl7-subscriptions-dev", "Subscription API app")
dbutils.widgets.text("consumer_group", spark.sql("SELECT current_user()").first()[0].split("@")[0] + "-notebook",
                     "Consumer group")  # fmt: skip
dbutils.widgets.text("seconds", "300", "Listen for (seconds)")

# COMMAND ----------

import os
import sys
from pathlib import Path

# This notebook's folder: its working directory in a Git folder, else its own path in the workspace.
here = Path(os.getcwd())
if not (here / "subscriber.py").exists():
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    here = Path(path if path.startswith("/Workspace") else "/Workspace" + path).parent
sys.path.insert(0, str(here))

import subscriber  # generates the API client on first use

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your rules
# MAGIC
# MAGIC Subscription name -> Databricks SQL: `SELECT <columns> FROM <source> WHERE <filter>`, with any Databricks SQL
# MAGIC built-in in the filter. Every run declares exactly this set: a rule you delete is cancelled, and an unchanged one
# MAGIC keeps its revision. Try a rule in the SQL editor against your own view first: it returns what you'll receive.

# COMMAND ----------

RULES = {
    # Edit, then run the next cell again.
    "adt-registrations": """
        SELECT patient_id, msg_type, patient_point_of_care FROM epic_realtime
        WHERE msg_type LIKE 'ADT^A04%'""",
}

# COMMAND ----------

api_app = dbutils.widgets.get("api_app")
try:
    subscriber.main([f"api.app={api_app}", "identity=user", f"consumer.group={dbutils.widgets.get('consumer_group')}",
                     f"--duration={dbutils.widgets.get('seconds')}"], rules=RULES)  # fmt: skip
except SystemExit as e:
    if e.code:  # 0 is done; anything else is the ERROR line above
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC
# MAGIC Declaring an empty set cancels every subscription you declared.

# COMMAND ----------

try:
    subscriber.main([f"api.app={dbutils.widgets.get('api_app')}", "identity=user"], rules={})
except SystemExit as e:
    if e.code:
        raise
