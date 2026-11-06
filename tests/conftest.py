import json
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Collected timestamps are converted to the process zone; keep it equal to the session zone.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("hl7rt-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")  # set by the pipeline; see epic_json.SPARK_CONF
        .getOrCreate()
    )
    yield session
    session.stop()


def epic_payload(columns: dict, **envelope) -> str:
    """Build an Epic Event Hubs JSON payload in the shape the outside system emits."""
    body = {
        "introspector": {"source": "epic"},
        "schema": envelope.get("schema", "EPIC"),
        "table": envelope.get("table", "HL7_EVENTS"),
        "txType": envelope.get("txType", "INSERT"),
        "columns": [{"name": k, "value": v} for k, v in columns.items()],
    }
    return json.dumps(body)


@pytest.fixture
def bronze_df(spark):
    """Factory: rows of event_value strings -> a DataFrame shaped like the bronze table."""

    def _make(*event_values: str):
        rows = [
            (
                f"key{i}",
                v,
                "real_time_mode_demo_catalog.hl7_realtime.epic_near_realtime_json_from_eventhub",
                0,
                i,
                "2026-10-23 12:00:00",
                1,
                "2026-10-23 12:00:01",
            )
            for i, v in enumerate(event_values)
        ]
        return spark.createDataFrame(
            rows,
            "event_key string, event_value string, topic string, partition int, offset long, "
            "kafka_timestamp string, kafka_timestamp_type int, _ingested_at string",
        ).selectExpr(
            "event_key",
            "event_value",
            "topic",
            "partition",
            "offset",
            "cast(kafka_timestamp as timestamp) as kafka_timestamp",
            "kafka_timestamp_type",
            "cast(_ingested_at as timestamp) as _ingested_at",
        )

    return _make


# A throwaway PostgreSQL with the registry applied: two onboarded apps (NOLOGIN on Lakebase; LOGIN here so tests can
# act as one directly), a `stranger` with no binding, and `api`, the Subscription API's role.
APPS = {
    "app_a": ("app-a", "sp-a", "cat.subscribers.app-a", ["gold_observations"],
              ["patient_id", "value_num", "observation_cd_desc"]),
    "app_b": ("app-b", "sp-b", "cat.subscribers.app-b", ["gold_observations", "gold_encounters"], ["patient_id"]),
}  # fmt: skip


def _bin(name):
    # Prefer a server install's own tools (libpq ships a pg_ctl that may not match the server version).
    server = sorted(Path("/opt/homebrew/opt").glob(f"postgresql*/bin/{name}")) or sorted(
        Path("/usr/lib/postgresql").glob(f"*/bin/{name}")
    )
    found = server[-1] if server else shutil.which(name)
    return str(found) if found else None


@pytest.fixture(scope="module")
def pg(tmp_path_factory):
    pg8000 = pytest.importorskip("pg8000.native")
    initdb, pg_ctl = _bin("initdb"), _bin("pg_ctl")
    if not (initdb and pg_ctl):
        pytest.skip("no local PostgreSQL server")
    data = tmp_path_factory.mktemp("pgdata")
    sock = Path(tempfile.mkdtemp(prefix="pg", dir="/tmp"))  # Unix socket paths are limited to ~104 characters
    subprocess.run([initdb, "-D", data, "-U", "owner", "--auth=trust"], check=True, capture_output=True)
    subprocess.run(
        [pg_ctl, "-D", data, "-o", f"-k {sock} -c listen_addresses=''", "-w", "start", "-l", data / "log"],
        check=True, capture_output=True,
    )  # fmt: skip
    owner = pg8000.Connection("owner", unix_sock=f"{sock}/.s.PGSQL.5432", database="postgres")
    owner.run((Path(__file__).resolve().parents[1] / "src/registry/registry.sql").read_text())
    owner.run("CREATE ROLE api LOGIN IN ROLE subscription_service")  # the Subscription API's own role
    for role, (name, sp, topic, sources, fields) in APPS.items():
        owner.run(f"CREATE ROLE {role} LOGIN IN ROLE subscriber")  # NOLOGIN on Lakebase; LOGIN here to test it alone
        owner.run(f"GRANT {role} TO subscription_service")
        owner.run("INSERT INTO subscription_registry.apps (app_role, app_name, destination_topic, allowed_sources, "
                  "allowed_fields, service_principal, eventhubs_credential) VALUES (:r, :n, :t, :s, :f, :sp, :c)",
                  r=role, n=name, t=topic, s=sources, f=fields, sp=sp,
                  c=f"sub_{name.replace('-', '_')}_eventhubs")  # fmt: skip
    owner.run("CREATE ROLE stranger LOGIN IN ROLE subscriber")  # a subscriber role with no app binding
    yield lambda role="owner": pg8000.Connection(role, unix_sock=f"{sock}/.s.PGSQL.5432", database="postgres")
    owner.close()
    subprocess.run([pg_ctl, "-D", data, "-m", "immediate", "stop"], capture_output=True)
    shutil.rmtree(sock, ignore_errors=True)
