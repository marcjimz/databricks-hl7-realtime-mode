"""The subscription registry (src/registry/registry.sql) against a real, throwaway PostgreSQL.

Apps are NOLOGIN roles in `subscriber`; the Subscription API connects as a member of `subscription_service` and acts as
the calling app with SET LOCAL ROLE, as on Lakebase. (Tests also connect as an app role directly, to show that even a
caller who got that far can't reach another app's rows.) Skipped when no local PostgreSQL server is installed.
"""

from pathlib import Path

import pytest

pg8000 = pytest.importorskip("pg8000.native")

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "src/registry/registry.sql").read_text()
Q = "SELECT patient_id, value_num FROM gold_observations WHERE observation_cd_desc = 'Hemoglobin' AND value_num < 8"
WHERE = "observation_cd_desc = 'Hemoglobin' AND value_num < 8"


def subscribe(conn, name, query=Q, where=WHERE, fields=("patient_id", "value_num"), source="gold_observations"):
    return conn.run(
        "SELECT subscription_api.subscribe(:n, :s, :q, :w, CAST(:f AS text[]))",
        n=name, s=source, q=query, w=where, f=list(fields),
    )[0][0]  # fmt: skip


def error(conn, *args, **kwargs):
    with pytest.raises(pg8000.DatabaseError) as e:
        subscribe(conn, *args, **kwargs)
    return e.value.args[0]["M"].split(":")[0]


def as_app(api, role):
    """The Subscription API acting for one caller: one transaction, SET LOCAL ROLE to that app."""
    api.run("BEGIN")
    api.run(f"SET LOCAL ROLE {role}")
    return api


def test_a_rule_is_stored_with_its_canonical_query(pg):
    r = subscribe(pg("app_a"), "low-hgb")
    assert r["state"] == "ACCEPTED" and r["revision"] == 1
    assert r["destination"] == "cat.subscribers.app-a"
    assert r["query"] == Q


def test_resubscribing_the_same_rule_keeps_the_revision_and_a_change_adds_one(pg):
    a = pg("app_a")
    first = subscribe(a, "rev-test", query="SELECT patient_id FROM gold_observations WHERE value_num > 6",
                      where="value_num > 6", fields=["patient_id"])  # fmt: skip
    same = subscribe(a, "rev-test", query="SELECT patient_id FROM gold_observations WHERE value_num > 6",
                     where="value_num > 6", fields=["patient_id"])  # fmt: skip
    changed = subscribe(a, "rev-test", query="SELECT patient_id FROM gold_observations WHERE value_num > 7",
                        where="value_num > 7", fields=["patient_id"])  # fmt: skip
    assert same["revision"] == first["revision"] and changed["revision"] == first["revision"] + 1


def test_grants_bound_sources_and_output_fields(pg):
    assert error(pg("app_a"), "x", source="gold_encounters") == "SOURCE_DENIED"
    assert error(pg("app_a"), "x", fields=["patient_id", "units"]) == "FIELDS_DENIED"
    assert error(pg("stranger"), "x") == "APP_NOT_ONBOARDED"


def test_the_service_acts_only_as_the_calling_app(pg):
    api = as_app(pg("api"), "app_a")
    r = subscribe(api, "via-api")
    api.run("COMMIT")
    assert pg().run("SELECT owner FROM subscription_registry.subscriptions WHERE subscription_id = CAST(:s AS uuid)",
                    s=r["subscriptionId"])[0][0] == "app_a"  # fmt: skip

    api.run("BEGIN")
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        api.run("SET LOCAL ROLE stranger")  # not an app the service was granted
    api.run("ROLLBACK")
    with pytest.raises(pg8000.DatabaseError, match="APP_NOT_ONBOARDED"):
        subscribe(pg("api"), "as-itself")  # without SET ROLE the service owns nothing


def test_the_service_maps_a_verified_caller_to_its_app_but_sees_no_subscriptions_as_itself(pg):
    subscribe(
        pg("app_b"), "b-rule", query="SELECT patient_id FROM gold_observations", where=None, fields=["patient_id"]
    )
    api = pg("api")
    assert api.run("SELECT app_role FROM subscription_registry.apps WHERE service_principal = 'sp-b'")[0][0] == "app_b"
    assert api.run("SELECT count(*) FROM subscription_registry.subscriptions")[0][0] == 0  # RLS: owner is the app


def test_apps_cannot_see_or_take_over_each_others_subscriptions(pg):
    a, b = pg("app_a"), pg("app_b")
    subscribe(b, "b-only", query="SELECT patient_id FROM gold_observations", where=None, fields=["patient_id"])
    names = [s["name"] for s in a.run("SELECT subscription_api.list_subscriptions()")[0][0]["subscriptions"]]
    assert "b-only" not in names
    assert a.run("SELECT count(*) FROM subscription_registry.subscriptions WHERE name = 'b-only'")[0][0] == 0
    # Writing directly (skipping the API) still can't claim another owner: the trigger stamps the caller.
    a.run(
        "INSERT INTO subscription_registry.subscriptions (owner, name, source, query, fields) "
        "VALUES ('app_b', 'spoof', 'gold_observations', 'SELECT patient_id FROM gold_observations', '{patient_id}')"
    )
    assert pg().run("SELECT owner FROM subscription_registry.subscriptions WHERE name = 'spoof'")[0][0] == "app_a"


def test_the_same_name_in_two_apps_is_two_subscriptions(pg):
    a = subscribe(pg("app_a"), "shared-name")
    b = subscribe(pg("app_b"), "shared-name", query="SELECT patient_id FROM gold_observations", where=None,
                  fields=["patient_id"])  # fmt: skip
    assert a["subscriptionId"] != b["subscriptionId"]


def test_direct_writes_are_validated_too(pg):
    with pytest.raises(pg8000.DatabaseError, match="FIELDS_DENIED"):
        pg("app_a").run("UPDATE subscription_registry.subscriptions SET fields = '{units}' WHERE name = 'low-hgb'")


def test_apps_cannot_write_status_or_onboarding(pg):
    a = pg("app_a")
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        a.run("UPDATE subscription_registry.apps SET allowed_fields = '{units}'")
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        a.run("INSERT INTO subscription_registry.status SELECT subscription_id, revision, 'ACTIVE' "
              "FROM subscription_registry.subscriptions")  # fmt: skip


def test_list_reports_the_hub_verdict_and_heartbeat(pg):
    a, owner = pg("app_a"), pg()
    sid = subscribe(a, "hb")["subscriptionId"]
    listed = {s["name"]: s for s in a.run("SELECT subscription_api.list_subscriptions()")[0][0]["subscriptions"]}
    assert listed["hb"]["state"] == "PENDING" and listed["hb"]["query"] == Q
    owner.run("INSERT INTO subscription_registry.status (subscription_id, revision, state) "
              "VALUES (CAST(:s AS uuid), 1, 'ACTIVE')", s=sid)  # fmt: skip
    owner.run("INSERT INTO subscription_registry.hub_heartbeat VALUES ('gold_observations', now(), 7, 3) "
              "ON CONFLICT (source) DO UPDATE SET last_batch_at = now()")  # fmt: skip
    out = a.run("SELECT subscription_api.list_subscriptions()")[0][0]
    assert {s["name"]: s for s in out["subscriptions"]}["hb"]["state"] == "ACTIVE"
    assert "gold_observations" in out["hub"]


def test_unsubscribe_cancels_only_your_own(pg):
    a, b = pg("app_a"), pg("app_b")
    subscribe(a, "cancel-me")
    with pytest.raises(pg8000.DatabaseError, match="NOT_FOUND_OR_INACCESSIBLE"):
        b.run("SELECT subscription_api.unsubscribe('cancel-me')")
    r = a.run("SELECT subscription_api.unsubscribe('cancel-me')")[0][0]
    assert r["state"] == "CANCELLING" and r["revision"] == 2


def test_the_monitor_shows_every_apps_rules_and_the_hubs_verdict(pg):
    a, b, owner = pg("app_a"), pg("app_b"), pg()
    sid = subscribe(a, "mon-a")["subscriptionId"]
    subscribe(b, "mon-b", query="SELECT patient_id FROM gold_observations", where=None, fields=["patient_id"])
    subscribe(a, "mon-gone")
    a.run("SELECT subscription_api.unsubscribe('mon-gone')")
    owner.run("INSERT INTO subscription_registry.status (subscription_id, revision, state) "
              "VALUES (CAST(:s AS uuid), 1, 'ACTIVE')", s=sid)  # fmt: skip

    rows = owner.run("SELECT subscription, app_name, service_principal, rule, state, seconds_to_confirm "
                     "FROM subscription_monitor.subscriptions WHERE subscription LIKE 'mon-%'")  # fmt: skip
    seen = {r[0]: r for r in rows}
    assert seen["mon-a"][1:5] == ["app-a", "sp-a", Q, "ACTIVE"] and seen["mon-a"][5] is not None
    assert seen["mon-b"][1] == "app-b" and seen["mon-b"][4] == "PENDING", "both apps' rules, in one view"
    assert seen["mon-gone"][4] == "CANCELLING", "cancelled, waiting for the hub to stop it"
    assert owner.run("SELECT count(*) FROM subscription_monitor.hub")[0][0] >= 0


def test_apps_and_the_service_cannot_read_the_monitor(pg):
    for role in ("app_a", "api"):
        with pytest.raises(pg8000.DatabaseError, match="permission denied"):
            pg(role).run("SELECT * FROM subscription_monitor.subscriptions")


def test_registry_sql_is_idempotent(pg):
    pg().run(SQL)  # applied on every `make registry`
