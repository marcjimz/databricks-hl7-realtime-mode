"""The Subscription API (src/subscription_api/app.py) against the throwaway PostgreSQL registry.

Databricks' compile step (DESCRIBE QUERY on a SQL warehouse) is stubbed with a tiny fake that knows each app's view
columns; the identity header is what the Databricks Apps proxy sets (and overwrites if a client forges it).
"""

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from subscription_api import app as api  # noqa: E402

VIEW_COLUMNS = {"cat.sub_app_a.gold_observations": {"patient_id", "value_num", "observation_cd_desc"}}
POTASSIUM = (
    "SELECT patient_id, value_num FROM gold_observations WHERE observation_cd_desc = 'Potassium' AND value_num > 6"
)


def fake_databricks(sql, schema):
    """Stands in for DESCRIBE QUERY: TABLE_OR_VIEW_NOT_FOUND / UNRESOLVED_COLUMN against the app's views."""
    from subscription_api.rules import columns, parse

    try:
        r = parse(sql, {"gold_observations", "gold_encounters", "gold_orders"}, schema)
    except Exception:  # noqa: BLE001
        return {"code": "PARSE_SYNTAX_ERROR", "message": "[PARSE_SYNTAX_ERROR] Syntax error"}
    cols = VIEW_COLUMNS.get(f"{schema}.{r.source}")
    if cols is None:
        return {"code": "TABLE_OR_VIEW_NOT_FOUND", "message": f"[TABLE_OR_VIEW_NOT_FOUND] {r.source}"}
    missing = (set(r.fields) | columns(r.where)) - cols
    if missing:
        return {"code": "UNRESOLVED_COLUMN.WITH_SUGGESTION", "message": f"[UNRESOLVED_COLUMN] {sorted(missing)[0]}"}
    return None


@pytest.fixture
def client(pg, monkeypatch):
    monkeypatch.setattr(api, "pool", api.Pool(lambda: pg("api")))
    monkeypatch.setattr(api, "CATALOG", "cat")
    compiled = []
    monkeypatch.setattr(api, "compile_with_databricks", lambda sql, schema: compiled.append(sql) or
                        fake_databricks(sql, schema))  # fmt: skip
    c = TestClient(api.app)
    c.compiled = compiled
    return c


A = {"X-Forwarded-Email": "sp-a"}
B = {"X-Forwarded-Email": "sp-b"}


def declare(client, items, who=A):
    return client.put("/api/v1/subscriptions", json={"subscriptions": [{"name": n, "sql": s} for n, s in items]},
                      headers=who)  # fmt: skip


def test_declaring_registers_the_canonical_sql_and_says_what_changed(client):
    r = declare(client, [("critical-k", POTASSIUM)])
    assert r.status_code == 200 and r.json()["applied"]
    got = r.json()["accepted"][0]
    assert got["state"] == "ACCEPTED" and got["destination"] == "cat.subscribers.app-a" and not got["unchanged"]
    assert got["query"] == POTASSIUM

    again = declare(client, [("critical-k", POTASSIUM)]).json()["accepted"][0]
    assert again["unchanged"] and again["revision"] == got["revision"]
    assert client.compiled.count(POTASSIUM) == 1, "an unchanged rule isn't compiled again"


def test_databricks_errors_pass_through_and_nothing_is_applied(client):
    r = declare(client, [("good", POTASSIUM.replace("> 6", "> 7")),
                         ("typo", "SELECT patient_id FROM gold_observations WHERE value_numm > 6"),
                         ("ungranted", "SELECT patient_id FROM gold_observations WHERE units = 'x'"),
                         ("not-mine", "SELECT patient_id FROM gold_encounters")])  # fmt: skip
    assert r.status_code == 422 and r.json()["applied"] is False
    codes = {x["name"]: x["code"] for x in r.json()["refused"]}
    assert codes == {"typo": "UNRESOLVED_COLUMN.WITH_SUGGESTION", "ungranted": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
                     "not-mine": "TABLE_OR_VIEW_NOT_FOUND"}  # fmt: skip
    names = [s["name"] for s in client.get("/api/v1/subscriptions", headers=A).json()["subscriptions"]]
    assert "good" not in names, "atomic: one refusal applies nothing"


def test_shapes_the_hub_wont_run_are_refused_before_databricks_sees_them(client):
    r = declare(
        client,
        [
            (
                "sub",
                "SELECT patient_id FROM gold_observations WHERE value_num > "
                "(SELECT max(value_num) FROM gold_observations)",
            ),
            ("net", "SELECT patient_id FROM gold_observations WHERE http_request('GET', 'x') IS NULL"),
        ],
    )
    assert {x["code"] for x in r.json()["refused"]} == {"SQL_NOT_ALLOWED"}
    assert not any("http_request" in s for s in client.compiled)


def test_declaring_cancels_what_the_code_no_longer_declares(client):
    declare(client, [("keep", POTASSIUM), ("drop-me", POTASSIUM.replace("> 6", "> 8"))])
    r = declare(client, [("keep", POTASSIUM)]).json()
    assert [c["name"] for c in r["cancelled"]] == ["drop-me"]


def test_validate_applies_nothing(client):
    r = client.post("/api/v1/validate", json={"subscriptions": [{"name": "dry", "sql": POTASSIUM}]}, headers=A).json()
    assert r["valid"] and r["accepted"][0]["query"] == POTASSIUM
    assert "dry" not in [s["name"] for s in client.get("/api/v1/subscriptions", headers=A).json()["subscriptions"]]


def test_the_caller_is_the_verified_identity_only(client):
    assert client.get("/api/v1/subscriptions").status_code == 401
    r = client.get("/api/v1/subscriptions", headers={"X-Forwarded-Email": "not-onboarded"})
    assert r.status_code == 403 and r.json()["code"] == "APP_NOT_ONBOARDED"


def test_a_dropped_registry_connection_is_replaced_not_reused(client, pg):
    """Lakebase closes idle connections (and suspends when idle): the next call must open a new one, not fail on the
    dead one, and a connection that failed must never go back to the pool."""
    assert declare(client, [("critical-k", POTASSIUM)]).status_code == 200
    pg().run("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'api'")

    for _ in range(3):
        assert client.get("/api/v1/subscriptions", headers=A).status_code == 200
        assert client.get("/api/v1/connection", headers=A).status_code in (200, 503)
    assert declare(client, [("critical-k", POTASSIUM)]).json()["accepted"][0]["unchanged"]


def test_the_connection_is_everything_an_app_needs_to_read_its_own_topic(client, monkeypatch):
    """An app configures only its service principal and the API: its topic and Kafka connection come from here."""
    monkeypatch.setattr(api, "EVENTHUBS_NAMESPACE", "ns1")
    a = client.get("/api/v1/connection", headers=A).json()
    assert a == {
        "app": "app-a",
        "topic": "cat.subscribers.app-a",
        "bootstrapServers": "ns1.servicebus.windows.net:9093",
        "credential": "sub_app_a_eventhubs",
        "tokenScope": "https://ns1.servicebus.windows.net/.default",
    }
    b = client.get("/api/v1/connection", headers=B).json()
    assert (b["topic"], b["credential"]) == ("cat.subscribers.app-b", "sub_app_b_eventhubs"), (
        "each app gets only its own"
    )
    assert declare(client, [("k", POTASSIUM)]).json()["accepted"][0]["destination"] == a["topic"], "= declare's topic"
    assert client.get("/api/v1/connection").status_code == 401
    r = client.get("/api/v1/connection", headers={"X-Forwarded-Email": "not-onboarded"})
    assert r.status_code == 403 and r.json()["code"] == "APP_NOT_ONBOARDED"


def test_the_connection_says_so_when_onboarding_is_unfinished(client, monkeypatch):
    monkeypatch.setattr(api, "EVENTHUBS_NAMESPACE", "")
    r = client.get("/api/v1/connection", headers=A)
    assert r.status_code == 503 and r.json()["code"] == "NOT_CONFIGURED"


def test_apps_only_see_and_cancel_their_own(client):
    declare(client, [("mine", POTASSIUM)])
    assert "mine" not in [s["name"] for s in client.get("/api/v1/subscriptions", headers=B).json()["subscriptions"]]
    r = client.delete("/api/v1/subscriptions/mine", headers=B)
    assert r.status_code == 404 and r.json()["code"] == "NOT_FOUND_OR_INACCESSIBLE"


def test_wait_reports_pending_then_ready_once_the_hub_runs_the_revision(client, pg):
    sid = declare(client, [("wait-me", POTASSIUM)]).json()["accepted"][0]["subscriptionId"]
    assert client.get("/api/v1/subscriptions", headers=A).json()["state"] == "PENDING"
    owner = pg()
    rev = owner.run("SELECT revision FROM subscription_registry.subscriptions WHERE subscription_id = CAST(:s AS uuid)",
                    s=sid)[0][0]  # fmt: skip
    owner.run("INSERT INTO subscription_registry.status (subscription_id, revision, state) VALUES "
              "(CAST(:s AS uuid), :r, 'ACTIVE') ON CONFLICT (subscription_id) DO UPDATE SET revision = :r, "
              "state = 'ACTIVE'", s=sid, r=rev)  # fmt: skip
    for sub in owner.run("SELECT subscription_id::text, revision FROM subscription_registry.subscriptions "
                         "WHERE owner = 'app_a' AND enabled"):  # fmt: skip
        owner.run("INSERT INTO subscription_registry.status (subscription_id, revision, state) VALUES "
                  "(CAST(:s AS uuid), :r, 'ACTIVE') ON CONFLICT (subscription_id) DO UPDATE SET revision = :r, "
                  "state = 'ACTIVE'", s=sub[0], r=sub[1])  # fmt: skip
    owner.run("INSERT INTO subscription_registry.hub_heartbeat VALUES ('gold_observations', now(), 1, 1) "
              "ON CONFLICT (source) DO UPDATE SET last_batch_at = now()")  # fmt: skip
    assert client.get("/api/v1/subscriptions?wait=5", headers=A).json()["state"] == "READY"


def test_the_rule_cap_is_a_refusal_and_applies_nothing(client, pg):
    owner = pg()
    enabled = owner.run("SELECT count(*) FROM subscription_registry.subscriptions WHERE enabled")[0][0]
    owner.run("UPDATE subscription_registry.limits SET value = :v WHERE name = 'max_enabled_rules'", v=enabled + 1)
    try:
        r = declare(client, [("cap-a", POTASSIUM), ("cap-b", POTASSIUM.replace("> 6", "> 9"))])
        assert r.status_code == 422 and r.json()["applied"] is False
        assert r.json()["refused"][0]["code"] == "RULE_LIMIT_REACHED"
        names = [s["name"] for s in client.get("/api/v1/subscriptions", headers=A).json()["subscriptions"]]
        assert "cap-a" not in names, "atomic: the first rule fit, but nothing was applied"
    finally:
        owner.run("UPDATE subscription_registry.limits SET value = 1000 WHERE name = 'max_enabled_rules'")


def test_names_are_checked_before_anything_else(client):
    r = declare(client, [("Bad Name!", POTASSIUM)])
    assert r.status_code == 422 and r.json()["refused"][0]["code"] == "INVALID_NAME"
    r = declare(client, [("same", POTASSIUM), ("same", POTASSIUM)])
    assert r.status_code == 422 and r.json()["code"] == "DUPLICATE_NAME"
    assert not client.compiled, "nothing reached Databricks"


def test_declaring_nothing_cancels_everything(client):
    declare(client, [("gone-1", POTASSIUM), ("gone-2", POTASSIUM.replace("> 6", "> 8"))])
    r = declare(client, []).json()
    assert r["applied"] and {c["name"] for c in r["cancelled"]} >= {"gone-1", "gone-2"}
    assert client.get("/api/v1/subscriptions", headers=A).json()["state"] == "READY", "nothing left to wait for"


def test_a_case_insensitive_rule_is_stored_in_canonical_form(client):
    r = declare(client, [("upper", "SELECT PATIENT_ID FROM GOLD_OBSERVATIONS WHERE VALUE_NUM > 6")])
    assert r.json()["accepted"][0]["query"] == "SELECT patient_id FROM gold_observations WHERE value_num > 6"
