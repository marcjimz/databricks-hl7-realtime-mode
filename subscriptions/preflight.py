# Databricks notebook source
# MAGIC %md
# MAGIC # Preflight: can the subscription layer run in this workspace?
# MAGIC
# MAGIC Run this **before** deploying anything (subscriptions/README.md, step 1). It's read-only: it creates, changes,
# MAGIC and starts nothing. It checks the source table the hub would read, the platform features the subscription layer
# MAGIC needs (Lakebase, Databricks Apps, a SQL warehouse, UC service credentials, network to Event Hubs and PyPI), and
# MAGIC your grants.
# MAGIC
# MAGIC **It prints no row values.** Only schema, table metadata, counts, rates, and null percentages; the one exception
# MAGIC is the count per `msg_type` code (e.g. `ADT^A04`), which helps write a first rule: turn `show_msg_types` off to
# MAGIC skip it.
# MAGIC
# MAGIC 1. Attach **serverless** compute.
# MAGIC 2. Set the widgets: the source table (`catalog.schema.table`) is required; the Event Hubs namespace and a UC
# MAGIC    service credential are optional (they test the network path and your ACCESS on it).
# MAGIC 3. **Run all**, then share the summary at the bottom (the table and the JSON under it).

# COMMAND ----------

import json
import os
import socket
import sys
from collections import Counter
from importlib import metadata

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from pyspark.sql import functions as F

dbutils.widgets.text("source_table", "", "1. Source table (catalog.schema.table)")
dbutils.widgets.text("subscriptions_catalog", "", "2. Catalog for the subscription objects (blank: the source's)")
dbutils.widgets.text("eventhubs_namespace", "", "3. Event Hubs namespace name (optional)")
dbutils.widgets.text("eventhubs_credential", "", "4. UC service credential to test (optional)")
dbutils.widgets.dropdown("show_msg_types", "yes", ["yes", "no"], "5. Count per msg_type code")
dbutils.widgets.text("max_scan_gb", "50", "6. Skip the table scans above this size (GB)")

# COMMAND ----------

w = WorkspaceClient()
SOURCE = dbutils.widgets.get("source_table").strip()
NAMESPACE = dbutils.widgets.get("eventhubs_namespace").strip().removesuffix(".servicebus.windows.net")
CREDENTIAL = dbutils.widgets.get("eventhubs_credential").strip()
SHOW_MSG_TYPES = dbutils.widgets.get("show_msg_types") == "yes"
MAX_SCAN_BYTES = float(dbutils.widgets.get("max_scan_gb") or 50) * 1e9
PARTS = SOURCE.split(".")
CATALOG, SCHEMA, TABLE = PARTS if len(PARTS) == 3 else (None, None, None)
SUBS_CATALOG = dbutils.widgets.get("subscriptions_catalog").strip() or CATALOG
Q = ".".join(f"`{p}`" for p in PARTS)

REPORT = []  # (section, check, status, detail): the table at the bottom
FACTS = {}  # the JSON at the bottom
_SECRETISH = ("secret", "password", "passwd", "token", "sasl", "jaas", "connectionstring", "connection_string", "sas",
              "key", "credential_json")  # fmt: skip


def record(section, check, status, detail=""):
    REPORT.append((section, check, status, str(detail)[:600]))
    print(f"{status:<5} {section:<9} {check}: {detail}")


def attempt(section, check, fn):
    """Each check stands alone: a failure is recorded and the notebook goes on."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        record(section, check, "FAIL", f"{type(e).__name__}: {(str(e).strip().splitlines() or [''])[0][:400]}")
        return None


def rest(method, path, **kw):
    """Plain REST through the SDK's auth: works whatever SDK version this runtime ships."""
    return w.api_client.do(method, path, **kw)


def redact(conf: dict) -> dict:
    return {k: ("<redacted>" if any(s in k.lower() for s in _SECRETISH) else v) for k, v in (conf or {}).items()}


def pct(values, p):
    s = sorted(values)
    return s[min(len(s) - 1, int(p * len(s)))] if s else None

# COMMAND ----------

# MAGIC %md ## A. You and the workspace

# COMMAND ----------

me = w.current_user.me()
GROUPS = sorted(g.display for g in (me.groups or []) if g.display)
FACTS["user"] = me.user_name
FACTS["workspace_admin"] = "admins" in GROUPS
FACTS["groups"] = GROUPS
FACTS["entitlements"] = sorted(e.value for e in (me.entitlements or []) if e.value)
record("workspace", "you", "INFO", f"{me.user_name}; workspace admin: {FACTS['workspace_admin']}; "
                                   f"entitlements: {FACTS['entitlements']}")  # fmt: skip
FACTS["workspace_id"] = attempt("workspace", "workspace id", w.get_workspace_id)
summary = attempt("workspace", "metastore", w.metastores.summary)
if summary:
    FACTS["cloud"], FACTS["region"], FACTS["metastore_id"] = summary.cloud, summary.region, summary.metastore_id
    record("workspace", "metastore", "INFO", f"{summary.cloud} {summary.region}")
FACTS["runtime"] = {"python": sys.version.split()[0], "environment": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
                    "spark": spark.version}  # fmt: skip
for pkg in ("databricks-sdk", "sqlglot", "pg8000", "confluent-kafka"):
    try:
        FACTS["runtime"][pkg] = metadata.version(pkg)
    except metadata.PackageNotFoundError:
        FACTS["runtime"][pkg] = None
record("workspace", "runtime", "INFO", FACTS["runtime"])

# COMMAND ----------

# MAGIC %md ## B. The source table

# COMMAND ----------

TABLE_OK = False
SCHEMA_FIELDS = []
if not CATALOG:
    record("table", "source_table", "FAIL", "set the source_table widget to catalog.schema.table")
else:
    info = attempt("table", "get", lambda: w.tables.get(SOURCE))
    if info:
        TABLE_OK = True
        props = {k: v for k, v in (info.properties or {}).items() if k.startswith(("delta.", "pipelines."))}
        FACTS["table"] = {
            "type": info.table_type.value if info.table_type else None,
            "format": info.data_source_format.value if info.data_source_format else None,
            "pipeline_id": getattr(info, "pipeline_id", None),
            "properties": redact(props),
        }
        record("table", "type", "INFO", FACTS["table"])
        SCHEMA_FIELDS = attempt("table", "schema", lambda: list(spark.table(SOURCE).schema)) or []
        FACTS["columns"] = [[f.name, f.dataType.simpleString()] for f in SCHEMA_FIELDS]
        record("table", "columns", "INFO", f"{len(SCHEMA_FIELDS)} columns: {[c for c, _ in FACTS['columns']]}")
        attempt("table", "read", lambda: spark.sql(f"SELECT 1 FROM {Q} LIMIT 1").collect()) is not None and record(
            "table", "read", "PASS", "you can read it")

# COMMAND ----------

DETAIL = {}
if TABLE_OK:
    d = attempt("table", "describe detail", lambda: spark.sql(f"DESCRIBE DETAIL {Q}").collect()[0].asDict())
    if d:
        DETAIL = {k: d.get(k) for k in ("format", "numFiles", "sizeInBytes", "partitionColumns", "clusteringColumns",
                                         "minReaderVersion", "minWriterVersion", "tableFeatures")}  # fmt: skip
        FACTS["detail"] = DETAIL
        features = set(DETAIL.get("tableFeatures") or [])
        props = FACTS["table"]["properties"]
        FACTS["row_tracking"] = "rowTracking" in features or props.get("delta.enableRowTracking") == "true"
        FACTS["change_data_feed"] = "changeDataFeed" in features or props.get("delta.enableChangeDataFeed") == "true"
        gb = (DETAIL.get("sizeInBytes") or 0) / 1e9
        record("table", "detail", "INFO", f"{DETAIL.get('numFiles')} files, {gb:.2f} GB, features {sorted(features)}")
        record("table", "row tracking", "PASS" if FACTS["row_tracking"] else "WARN",
               "on: _metadata.row_id is a stable row ID" if FACTS["row_tracking"] else
               "off: the hub needs another stable row ID (see the verdict)")  # fmt: skip

# COMMAND ----------

NON_APPEND = {"MERGE", "UPDATE", "DELETE", "RESTORE", "TRUNCATE"}
if TABLE_OK:
    h = attempt("table", "history", lambda: spark.sql(f"DESCRIBE HISTORY {Q} LIMIT 200").select(
        "timestamp", "operation", "operationParameters", "operationMetrics").collect())  # fmt: skip
    if h:
        ops = Counter(r.operation for r in h)
        overwrites = sum(1 for r in h if (r.operationParameters or {}).get("mode") == "Overwrite")
        stamps = sorted(r.timestamp for r in h)
        gaps = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:], strict=False)]
        rows = [int((r.operationMetrics or {}).get("numOutputRows", 0)) for r in h if r.operation == "STREAMING UPDATE"]
        append_only = not (set(ops) & NON_APPEND) and not overwrites
        FACTS["history"] = {
            "commits_read": len(h), "operations": dict(ops), "overwrites": overwrites, "append_only": append_only,
            "latest_commit": stamps[-1].isoformat(), "commit_gap_s_p50": pct(gaps, 0.5),
            "commit_gap_s_p90": pct(gaps, 0.9), "rows_per_streaming_commit_p50": pct(rows, 0.5),
        }  # fmt: skip
        record("table", "append-only", "PASS" if append_only else "WARN",
               f"operations {dict(ops)}" + ("" if append_only else
               ": the hub reads appends only (skipChangeCommits); changed rows aren't re-notified"))  # fmt: skip
        record("table", "commit cadence", "INFO", f"p50 {pct(gaps, 0.5)} s, p90 {pct(gaps, 0.9)} s between commits; "
                                                  f"p50 {pct(rows, 0.5)} rows per streaming commit")  # fmt: skip

# COMMAND ----------

# MAGIC %md
# MAGIC Which Delta file and row metadata a **streaming** read resolves. The hub needs a stable integer row ID
# MAGIC (notification IDs are derived from it, so a retried batch sends the same IDs). This only analyses the query
# MAGIC (`.schema`); nothing runs.

# COMMAND ----------

META = ["row_id", "row_commit_version", "file_path", "row_index", "file_modification_time"]
FACTS["metadata"] = {}
if TABLE_OK:
    for field in META:
        found = {}
        for mode, df in (("batch", lambda: spark.read.table(SOURCE)),
                         ("stream", lambda: spark.readStream.option("startingVersion", "latest")
                          .option("skipChangeCommits", "true").table(SOURCE))):  # fmt: skip
            try:
                _ = df().select(F.col(f"_metadata.{field}")).schema  # analysis only
                found[mode] = True
            except Exception as e:  # noqa: BLE001
                found[mode] = f"no ({type(e).__name__})"
        FACTS["metadata"][field] = found
        record("table", f"_metadata.{field}", "PASS" if found["stream"] is True else "WARN", found)
    if FACTS["metadata"]["row_id"]["batch"] is True:
        n = attempt("table", "row_id filled", lambda: spark.read.table(SOURCE).select(
            F.col("_metadata.row_id").alias("r")).limit(1000).agg(F.count("r"), F.count("*")).collect()[0])  # fmt: skip
        if n:
            FACTS["row_id_filled"] = f"{n[0]} of {n[1]} sampled rows"
            record("table", "row_id filled", "PASS" if n[0] == n[1] and n[1] else "WARN", FACTS["row_id_filled"])

# COMMAND ----------

# MAGIC %md Freshness, rate, the pipeline's own latency, null rates, and ID candidates: aggregates over the last hour.

# COMMAND ----------

TS_COLS = [f.name for f in SCHEMA_FIELDS if f.dataType.typeName() in ("timestamp", "timestamp_ntz")]
TS = next((c for c in ("_ingested_at", "kafka_timestamp") if c in TS_COLS), TS_COLS[0] if TS_COLS else None)
ID_HINTS = ("id", "offset", "partition", "seq")
ID_COLS = [f.name for f in SCHEMA_FIELDS if f.name.lower().split("_")[-1] in ID_HINTS
           and f.dataType.typeName() in ("integer", "long", "string")][:12]  # fmt: skip
too_big = (DETAIL.get("sizeInBytes") or 0) > MAX_SCAN_BYTES
if TABLE_OK and TS and not too_big:
    lag = ("unix_millis(`_ingested_at`) - unix_millis(`kafka_timestamp`)"
           if {"_ingested_at", "kafka_timestamp"} <= set(TS_COLS) else None)  # fmt: skip
    exprs = [F.count("*").alias("n"), F.max(TS).alias("latest"),
             F.count_if(F.col(TS) >= F.expr("current_timestamp() - INTERVAL 5 MINUTES")).alias("n5")]  # fmt: skip
    exprs += [F.count(f.name).alias(f"nn_{i}") for i, f in enumerate(SCHEMA_FIELDS)]
    exprs += [F.countDistinct(c).alias(f"d_{i}") for i, c in enumerate(ID_COLS)]
    if lag:
        exprs += [F.expr(f"percentile({lag}, array(0.5, 0.9, 0.99))").alias("lag"),
                  F.expr(f"max({lag})").alias("lag_max")]  # fmt: skip
    r = attempt("table", "last hour", lambda: spark.table(SOURCE).where(
        F.col(TS) >= F.expr("current_timestamp() - INTERVAL 1 HOUR")).agg(*exprs).collect()[0])  # fmt: skip
    if r:
        n = r["n"]
        FACTS["last_hour"] = {"by": TS, "rows": n, "rows_last_5_min": r["n5"], "per_second": round(n / 3600, 2),
                              "latest": str(r["latest"])}  # fmt: skip
        record("table", "rate", "PASS" if n else "WARN",
               f"{n} rows in the last hour by {TS} ({n / 3600:.2f}/s), {r['n5']} in the last 5 min, "
               f"latest {r['latest']}")  # fmt: skip
        if n:
            FACTS["null_pct"] = {f.name: round(100 * (1 - r[f"nn_{i}"] / n), 1) for i, f in enumerate(SCHEMA_FIELDS)}
            FACTS["distinct_of_rows"] = {c: f"{r[f'd_{i}']} of {n}" for i, c in enumerate(ID_COLS)}
            record("table", "null %", "INFO", {k: v for k, v in FACTS["null_pct"].items() if v})
            record("table", "ID candidates", "INFO", FACTS["distinct_of_rows"])
            if lag and r["lag"]:
                p50, p90, p99 = r["lag"]
                FACTS["pipeline_lag_ms"] = {"p50": p50, "p90": p90, "p99": p99, "max": r["lag_max"]}
                record("table", "kafka_timestamp -> _ingested_at", "INFO", FACTS["pipeline_lag_ms"])
        else:
            latest = attempt("table", "latest row", lambda: spark.table(SOURCE).agg(F.max(TS)).collect()[0][0])
            FACTS["latest_row"] = str(latest)
            record("table", "latest row", "WARN", f"no rows in the last hour; latest {latest}")
elif TABLE_OK:
    record("table", "last hour", "WARN", "skipped: " + ("no timestamp column" if not TS else
           f"the table is over {MAX_SCAN_BYTES / 1e9:.0f} GB (max_scan_gb)"))  # fmt: skip

# COMMAND ----------

if TABLE_OK and SHOW_MSG_TYPES and TS and not too_big and "msg_type" in {f.name for f in SCHEMA_FIELDS}:
    rows = attempt("table", "msg_type", lambda: spark.table(SOURCE).where(
        F.col(TS) >= F.expr("current_timestamp() - INTERVAL 1 HOUR")).groupBy("msg_type").count()
        .orderBy(F.desc("count")).limit(15).collect())  # fmt: skip
    if rows is not None:
        FACTS["msg_types_last_hour"] = {r["msg_type"]: r["count"] for r in rows}
        record("table", "msg_type", "INFO", FACTS["msg_types_last_hour"])

# COMMAND ----------

if TABLE_OK:
    others = attempt("table", "schema's tables", lambda: list(w.tables.list(catalog_name=CATALOG, schema_name=SCHEMA)))
    if others is not None:
        FACTS["schema_tables"] = {t.name: [t.table_type.value if t.table_type else None, len(t.columns or [])]
                                  for t in others}  # fmt: skip
        record("table", "schema's tables", "INFO", FACTS["schema_tables"])
    pid = FACTS.get("table", {}).get("pipeline_id")
    if pid:
        p = attempt("table", "producing pipeline", lambda: rest("GET", f"/api/2.0/pipelines/{pid}"))
        if p:
            spec = p.get("spec", {})
            FACTS["pipeline"] = {
                "name": spec.get("name"), "state": p.get("state"), "continuous": spec.get("continuous", False),
                "serverless": spec.get("serverless", False), "channel": spec.get("channel"),
                "target": f"{spec.get('catalog')}.{spec.get('schema') or spec.get('target')}",
                "configuration": redact(spec.get("configuration")),
                "libraries": [next(iter(lib.values())) for lib in spec.get("libraries", [])],
            }  # fmt: skip
            record("table", "producing pipeline", "INFO", FACTS["pipeline"])

# COMMAND ----------

# MAGIC %md ## C. Platform: Lakebase, Apps, warehouses, service credentials, network

# COMMAND ----------

projects = attempt("platform", "Lakebase", lambda: rest("GET", "/api/2.0/postgres/projects"))
if projects is not None:
    FACTS["lakebase_projects"] = len(projects.get("projects", []))
    record("platform", "Lakebase", "PASS", f"Lakebase Autoscaling available ({FACTS['lakebase_projects']} projects)")
apps = attempt("platform", "Databricks Apps", lambda: rest("GET", "/api/2.0/apps"))
if apps is not None:
    FACTS["apps"] = len(apps.get("apps", []))
    record("platform", "Databricks Apps", "PASS", f"available ({FACTS['apps']} apps)")


def _my_level(kind, oid):
    acl = rest("GET", f"/api/2.0/permissions/{kind}/{oid}").get("access_control_list", [])
    mine = [a for a in acl if a.get("user_name") == me.user_name or a.get("group_name") in GROUPS]
    return sorted({p.get("permission_level") for a in mine for p in a.get("all_permissions", [])})


whs = attempt("platform", "SQL warehouses", lambda: rest("GET", "/api/2.0/sql/warehouses").get("warehouses", []))
if whs is not None:
    FACTS["warehouses"] = [{"name": x.get("name"), "type": x.get("warehouse_type"),
                            "serverless": x.get("enable_serverless_compute"), "size": x.get("cluster_size"),
                            "state": x.get("state"), "you": attempt("platform", f"warehouse {x.get('name')}",
                                                                     lambda x=x: _my_level("warehouses", x["id"]))}
                           for x in whs]  # fmt: skip
    record("platform", "SQL warehouses", "PASS" if whs else "FAIL", FACTS["warehouses"])
creds = attempt("platform", "service credentials", lambda: rest(
    "GET", "/api/2.1/unity-catalog/credentials", query={"purpose": "SERVICE"}).get("credentials", []))  # fmt: skip
if creds is not None:
    FACTS["service_credentials"] = sorted(c.get("name") for c in creds)
    record("platform", "service credentials", "INFO", FACTS["service_credentials"])
if CREDENTIAL and NAMESPACE:
    scope = f"https://{NAMESPACE}.servicebus.windows.net/.default"
    t = attempt("platform", f"token from {CREDENTIAL}", lambda: rest(
        "POST", "/api/2.1/unity-catalog/temporary-service-credentials",
        body={"credential_name": CREDENTIAL, "azure_options": {"resources": [scope]}}))  # fmt: skip
    if t is not None:
        FACTS["eventhubs_token"] = bool(t.get("azure_aad", {}).get("aad_token"))
        record("platform", f"token from {CREDENTIAL}", "PASS" if FACTS["eventhubs_token"] else "FAIL",
               "UC issued an Event Hubs token (not printed): you have ACCESS on it")  # fmt: skip

# COMMAND ----------

FACTS["network"] = {}
targets = [("pypi.org", 443), ("files.pythonhosted.org", 443)]
if NAMESPACE:
    targets.insert(0, (f"{NAMESPACE}.servicebus.windows.net", 9093))
for host, port in targets:
    try:
        socket.create_connection((host, port), timeout=5).close()
        FACTS["network"][f"{host}:{port}"] = True
        record("network", f"{host}:{port}", "PASS", "reachable from serverless")
    except OSError as e:
        FACTS["network"][f"{host}:{port}"] = False
        record("network", f"{host}:{port}", "FAIL", f"{type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md ## D. Your grants (directly, through your groups, or inherited)

# COMMAND ----------


def privileges(securable, name):
    """Yours, directly or through an account group. Workspace-local groups (e.g. admins) aren't UC principals."""
    out, found = set(), False
    for principal in [me.user_name, *GROUPS[:25]]:
        try:
            res = rest("GET", f"/api/2.1/unity-catalog/effective-permissions/{securable}/{name}",
                       query={"principal": principal})  # fmt: skip
        except NotFound:
            continue
        found = True
        for a in res.get("privilege_assignments", []):
            out |= {p.get("privilege") for p in a.get("privileges", [])}
    if not found:
        raise RuntimeError("no principal of yours is known to Unity Catalog")
    return out


_GET = {"catalog": w.catalogs.get, "schema": w.schemas.get, "table": w.tables.get}


def owned(securable, name):
    """Owning the object or any parent grants everything on it (effective permissions don't list ownership)."""
    parts = name.split(".")
    chain = [("catalog", parts[0]), ("schema", ".".join(parts[:2])), ("table", name)][: len(parts)]
    mine = {me.user_name, *GROUPS}
    return next((f"owner of {kind} {n}" for kind, n in chain if getattr(_GET[kind](n), "owner", None) in mine), None)


FACTS["grants"] = {}
checks = []
if TABLE_OK:
    checks += [("table", SOURCE, {"SELECT"}), ("schema", f"{CATALOG}.{SCHEMA}", {"USE_SCHEMA"}),
               ("catalog", CATALOG, {"USE_CATALOG"})]  # fmt: skip
if SUBS_CATALOG and SUBS_CATALOG != CATALOG:
    checks.append(("catalog", SUBS_CATALOG, {"USE_CATALOG", "CREATE_SCHEMA"}))
elif SUBS_CATALOG:
    checks[-1] = ("catalog", CATALOG, {"USE_CATALOG", "CREATE_SCHEMA"})
for securable, name, need in checks:
    have = attempt("grants", f"{securable} {name}", lambda s=securable, n=name: privileges(s, n))
    if have is None:
        continue
    owner = attempt("grants", f"owner of {name}", lambda s=securable, n=name: owned(s, n))
    FACTS["grants"][f"{securable} {name}"] = [owner] if owner else sorted(have)
    ok = bool(owner) or "ALL_PRIVILEGES" in have or need <= have
    record("grants", f"{securable} {name}", "PASS" if ok else "WARN",
           (owner or f"have {sorted(have)}") + ("" if ok else f"; needs {sorted(need - have)}"))  # fmt: skip
if FACTS.get("metastore_id"):
    have = attempt("grants", "metastore", lambda: privileges("metastore", FACTS["metastore_id"]))
    if have is not None:
        FACTS["grants"]["metastore"] = sorted(have)
        record("grants", "metastore", "INFO", f"have {sorted(have)} (CREATE_CATALOG: only for the optional registry "
                                              "catalog; CREATE_SERVICE_CREDENTIAL: your admin's step)")  # fmt: skip

# COMMAND ----------

# MAGIC %md ## Verdict and summary (share this)

# COMMAND ----------

meta = FACTS.get("metadata", {})
stream_ok = {f for f, v in meta.items() if v.get("stream") is True}
int_ids = [c for c in ID_COLS if c in FACTS.get("distinct_of_rows", {})
           and FACTS["distinct_of_rows"][c].split(" of ")[0] == FACTS["distinct_of_rows"][c].split(" of ")[1]
           and dict(FACTS.get("columns", [])).get(c) in ("int", "bigint")]  # fmt: skip
if "row_id" in stream_ok and FACTS.get("row_tracking"):
    offset = "_metadata.row_id"
elif int_ids:
    offset = int_ids[0]
elif {"file_path", "row_index"} <= stream_ok:
    offset = "xxhash64(_metadata.file_path, _metadata.row_index)"
else:
    offset = None
FACTS["suggested"] = {
    "subscription_table_offset": offset,
    "subscription_table_partition": "0",
    "subscription_table_ts": "_ingested_at" if "_ingested_at" in TS_COLS else TS,
}
record("verdict", "row ID for the hub", "PASS" if offset else "FAIL",
       offset or "no stable row ID: enable row tracking (ALTER TABLE ... SET TBLPROPERTIES "
       "('delta.enableRowTracking' = 'true')) or add an integer ID column")  # fmt: skip
record("verdict", "source timestamp", "PASS" if FACTS["suggested"]["subscription_table_ts"] else "FAIL",
       FACTS["suggested"]["subscription_table_ts"] or "no timestamp column")
for need, ok in (("Lakebase", "lakebase_projects" in FACTS), ("Databricks Apps", "apps" in FACTS),
                 ("a SQL warehouse", bool(FACTS.get("warehouses"))),
                 ("PyPI from serverless", FACTS["network"].get("pypi.org:443", False))):  # fmt: skip
    record("verdict", need, "PASS" if ok else "FAIL", "available" if ok else "missing: see above")

display(spark.createDataFrame(REPORT, "section string, check string, status string, detail string"))

# COMMAND ----------

print(json.dumps(FACTS, indent=1, default=str))
