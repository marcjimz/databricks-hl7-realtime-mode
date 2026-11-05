"""The subscription registry (src/registry/registry.sql) against a real, throwaway PostgreSQL.

Apps are separate Postgres roles in `subscriber`, as on Lakebase where the Data API runs each call as the caller's role.
Skipped when no local PostgreSQL server (initdb/pg_ctl) is installed.
"""

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

pg8000 = pytest.importorskip("pg8000.native")

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "src/registry/registry.sql").read_text()
APPS = {
    "app_a": (
        "app-a",
        "cat.subscribers.app-a",
        ["gold_observations"],
        ["patient_id", "value_num", "observation_cd_desc"],
    ),
    "app_b": ("app-b", "cat.subscribers.app-b", ["gold_observations", "gold_encounters"], ["patient_id"]),
}
COLUMNS = [
    ("gold_observations", "patient_id", "string"),
    ("gold_observations", "observation_cd_desc", "string"),
    ("gold_observations", "value_num", "double"),
    ("gold_observations", "set_id", "int"),
    ("gold_observations", "observation_dt_tm", "timestamp"),
    ("gold_observations", "units", "string"),
    ("gold_encounters", "patient_id", "string"),
]


def _bin(name):
    # Prefer a server install's own tools (libpq ships a pg_ctl that may not match the server version).
    server = sorted(Path("/opt/homebrew/opt").glob(f"postgresql*/bin/{name}")) or sorted(
        Path("/usr/lib/postgresql").glob(f"*/bin/{name}")
    )
    found = server[-1] if server else shutil.which(name)
    return str(found) if found else None


@pytest.fixture(scope="module")
def pg(tmp_path_factory):
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
    owner.run(SQL)
    for table_row in COLUMNS:
        owner.run("INSERT INTO subscription_registry.source_columns VALUES (:s, :c, :t)", s=table_row[0],
                  c=table_row[1], t=table_row[2])  # fmt: skip
    for role, (name, topic, sources, fields) in APPS.items():
        owner.run(f"CREATE ROLE {role} LOGIN IN ROLE subscriber")
        owner.run("INSERT INTO subscription_registry.apps VALUES (:r, :n, :t, :s, :f)", r=role, n=name, t=topic,
                  s=sources, f=fields)  # fmt: skip
    owner.run("CREATE ROLE stranger LOGIN IN ROLE subscriber")  # a subscriber role with no app binding
    yield lambda role="owner": pg8000.Connection(role, unix_sock=f"{sock}/.s.PGSQL.5432", database="postgres")
    owner.close()
    subprocess.run([pg_ctl, "-D", data, "-m", "immediate", "stop"], capture_output=True)
    shutil.rmtree(sock, ignore_errors=True)


def subscribe(conn, name, predicate, fields=("patient_id",), source="gold_observations"):
    return conn.run(
        "SELECT subscription_api.subscribe(:n, :s, CAST(:p AS jsonb), CAST(:f AS text[]))",
        n=name, s=source, p=json.dumps(predicate), f=list(fields),
    )[0][0]  # fmt: skip


def error(conn, *args, **kwargs):
    with pytest.raises(pg8000.DatabaseError) as e:
        subscribe(conn, *args, **kwargs)
    return e.value.args[0]["M"].split(":")[0]


LOW_HGB = {"and": [{"field": "observation_cd_desc", "op": "eq", "value": "Hemoglobin"},
                   {"field": "value_num", "op": "lt", "value": 8}]}  # fmt: skip


def test_a_valid_rule_is_accepted_with_a_sql_rendering(pg):
    r = subscribe(pg("app_a"), "low-hgb", LOW_HGB, ["patient_id", "value_num"])
    assert r["state"] == "ACCEPTED"
    assert r["revision"] == 1
    assert r["destination"] == "cat.subscribers.app-a"
    assert r["predicateSql"] == "(observation_cd_desc = 'Hemoglobin') AND (value_num < 8)"


def test_resubscribing_the_same_rule_keeps_the_revision_and_a_change_adds_one(pg):
    a = pg("app_a")
    first = subscribe(a, "rev-test", {"field": "value_num", "op": "gt", "value": 6})
    assert subscribe(a, "rev-test", {"field": "value_num", "op": "gt", "value": 6})["revision"] == first["revision"]
    assert subscribe(a, "rev-test", {"field": "value_num", "op": "gt", "value": 7})["revision"] == first["revision"] + 1


@pytest.mark.parametrize(
    "predicate, code",
    [
        ({"field": "nope", "op": "eq", "value": "x"}, "FIELD_NOT_IN_SOURCE"),
        ({"field": "units", "op": "eq", "value": "g/dL"}, "FIELD_DENIED"),  # a real column, but not granted
        ({"field": "value_num", "op": "eq", "value": "high"}, "TYPE_MISMATCH"),
        ({"field": "patient_id", "op": "lt", "value": "M"}, "OPERATOR_NOT_ALLOWED"),
        ({"field": "value_num", "op": "contains", "value": "6"}, "OPERATOR_NOT_ALLOWED"),
        ({"field": "value_num", "op": "regex", "value": ".*"}, "OPERATOR_NOT_ALLOWED"),
        ({"field": "value_num", "op": "gt"}, "PREDICATE_INVALID"),
        ({"field": "value_num", "op": "is_null", "value": 1}, "PREDICATE_INVALID"),
        ({"field": "value_num", "op": "gt", "value": 1, "sql": "1=1"}, "PREDICATE_INVALID"),
        ({"and": [{"field": "value_num", "op": "gt", "value": 1}]}, "PREDICATE_INVALID"),
        ({"field": "patient_id", "op": "in", "value": []}, "PREDICATE_INVALID"),
        ({"field": "patient_id", "op": "in", "value": ["a", 1]}, "TYPE_MISMATCH"),
        ("value_num > (SELECT max(value_num) FROM gold_observations)", "PREDICATE_INVALID"),
    ],
)
def test_invalid_rules_are_refused_in_the_same_call(pg, predicate, code):
    assert error(pg("app_a"), "bad", predicate, ["patient_id"]) == code


def test_deep_or_large_rules_are_refused(pg):
    p = {"field": "value_num", "op": "gt", "value": 1}
    for _ in range(9):
        p = {"not": p}
    assert error(pg("app_a"), "deep", p) == "PREDICATE_TOO_COMPLEX"
    leaf = {"field": "value_num", "op": "gt", "value": 1}
    wide = {"or": [{"and": [leaf, leaf]} for _ in range(40)]}
    assert error(pg("app_a"), "wide", wide) == "PREDICATE_TOO_COMPLEX"


def test_grants_bound_sources_and_output_fields(pg):
    assert error(pg("app_a"), "x", LOW_HGB, source="gold_encounters") == "SOURCE_DENIED"
    assert error(pg("app_a"), "x", LOW_HGB, ["patient_id", "units"]) == "FIELDS_DENIED"
    assert error(pg("stranger"), "x", LOW_HGB) == "APP_NOT_ONBOARDED"


def test_literals_are_quoted_in_the_rendering(pg):
    r = subscribe(pg("app_a"), "quote", {"field": "observation_cd_desc", "op": "eq", "value": "O'Brien'); DROP"})
    assert r["predicateSql"] == "observation_cd_desc = 'O''Brien''); DROP'"


def test_timestamps_must_be_iso_8601(pg):
    a = pg("app_a")
    # observation_dt_tm is in the source but not granted to app_a; app_b has neither, so grant it for this check.
    owner = pg()
    owner.run("UPDATE subscription_registry.apps SET allowed_fields = allowed_fields || '{observation_dt_tm}' "
              "WHERE app_role = 'app_a'")  # fmt: skip
    ok = subscribe(a, "ts", {"field": "observation_dt_tm", "op": "ge", "value": "2026-11-03T00:00:00Z"})
    assert ok["predicateSql"] == "observation_dt_tm >= TIMESTAMP '2026-11-03T00:00:00Z'"
    assert error(a, "ts2", {"field": "observation_dt_tm", "op": "ge", "value": "yesterday"}) == "TYPE_MISMATCH"
    assert (
        error(a, "ts3", {"field": "observation_dt_tm", "op": "ge", "value": "2026-02-30T00:00:00Z"}) == "TYPE_MISMATCH"
    )


def test_apps_cannot_see_or_take_over_each_others_subscriptions(pg):
    a, b = pg("app_a"), pg("app_b")
    subscribe(b, "b-only", {"field": "patient_id", "op": "eq", "value": "P1"})
    names = [s["name"] for s in a.run("SELECT subscription_api.list_subscriptions()")[0][0]["subscriptions"]]
    assert "b-only" not in names
    assert a.run("SELECT count(*) FROM subscription_registry.subscriptions WHERE name = 'b-only'")[0][0] == 0
    # Writing directly (skipping the API) still can't claim another owner: the trigger stamps the caller.
    a.run(
        "INSERT INTO subscription_registry.subscriptions (owner, name, source, predicate, fields) "
        "VALUES ('app_b', 'spoof', 'gold_observations', "
        '\'{"field":"patient_id","op":"is_not_null"}\', \'{patient_id}\')'
    )
    assert pg().run("SELECT owner FROM subscription_registry.subscriptions WHERE name = 'spoof'")[0][0] == "app_a"


def test_direct_writes_are_validated_too(pg):
    with pytest.raises(pg8000.DatabaseError, match="FIELD_DENIED"):
        pg("app_a").run(
            'UPDATE subscription_registry.subscriptions SET predicate = \'{"field":"units","op":"is_null"}\' '
            "WHERE name = 'low-hgb'"
        )


def test_apps_cannot_write_status_or_onboarding(pg):
    a = pg("app_a")
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        a.run("UPDATE subscription_registry.apps SET allowed_fields = '{units}'")
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        a.run("INSERT INTO subscription_registry.status SELECT subscription_id, revision, 'ACTIVE' "
              "FROM subscription_registry.subscriptions")  # fmt: skip


def test_list_reports_the_hub_verdict_and_heartbeat(pg):
    a, owner = pg("app_a"), pg()
    sid = subscribe(a, "hb", {"field": "value_num", "op": "gt", "value": 6})["subscriptionId"]
    listed = {s["name"]: s for s in a.run("SELECT subscription_api.list_subscriptions()")[0][0]["subscriptions"]}
    assert listed["hb"]["state"] == "PENDING"
    owner.run("INSERT INTO subscription_registry.status (subscription_id, revision, state) "
              "VALUES (CAST(:s AS uuid), 1, 'ACTIVE')",
              s=sid)  # fmt: skip
    owner.run("INSERT INTO subscription_registry.hub_heartbeat VALUES ('gold_observations', now(), 7, 3) "
              "ON CONFLICT (source) DO UPDATE SET last_batch_at = now()")  # fmt: skip
    out = a.run("SELECT subscription_api.list_subscriptions()")[0][0]
    assert {s["name"]: s for s in out["subscriptions"]}["hb"]["state"] == "ACTIVE"
    assert "gold_observations" in out["hub"]


def test_unsubscribe_cancels_only_your_own(pg):
    a, b = pg("app_a"), pg("app_b")
    subscribe(a, "cancel-me", {"field": "value_num", "op": "gt", "value": 6})
    with pytest.raises(pg8000.DatabaseError, match="NOT_FOUND_OR_INACCESSIBLE"):
        b.run("SELECT subscription_api.unsubscribe('cancel-me')")
    r = a.run("SELECT subscription_api.unsubscribe('cancel-me')")[0][0]
    assert r["state"] == "CANCELLING" and r["revision"] == 2


def test_the_monitor_shows_every_apps_rules_and_the_hubs_verdict(pg):
    a, b, owner = pg("app_a"), pg("app_b"), pg()
    sid = subscribe(a, "mon-a", {"field": "value_num", "op": "gt", "value": 6})["subscriptionId"]
    subscribe(b, "mon-b", {"field": "patient_id", "op": "is_not_null"})
    subscribe(a, "mon-gone", {"field": "value_num", "op": "lt", "value": 1})
    a.run("SELECT subscription_api.unsubscribe('mon-gone')")
    owner.run("INSERT INTO subscription_registry.status (subscription_id, revision, state) "
              "VALUES (CAST(:s AS uuid), 1, 'ACTIVE')", s=sid)  # fmt: skip

    rows = owner.run("SELECT subscription, app_name, service_principal, rule, state, seconds_to_confirm "
                     "FROM subscription_monitor.subscriptions WHERE subscription LIKE 'mon-%'")  # fmt: skip
    seen = {r[0]: r for r in rows}
    assert seen["mon-a"][1:5] == ["app-a", "app_a", "value_num > 6", "ACTIVE"] and seen["mon-a"][5] is not None
    assert seen["mon-b"][1] == "app-b" and seen["mon-b"][4] == "PENDING", "both apps' rules, in one view"
    assert seen["mon-gone"][4] == "CANCELLING", "cancelled, waiting for the hub to stop it"
    assert owner.run("SELECT count(*) FROM subscription_monitor.hub")[0][0] >= 0


def test_apps_cannot_read_the_monitor(pg):
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        pg("app_a").run("SELECT * FROM subscription_monitor.subscriptions")


def test_registry_sql_is_idempotent(pg):
    pg().run(SQL)  # applied on every `make up`
