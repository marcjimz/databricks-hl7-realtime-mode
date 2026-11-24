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
Q = "SELECT patient_id, value_num FROM silver_observations WHERE observation_cd_desc = 'Hemoglobin' AND value_num < 8"
WHERE = "observation_cd_desc = 'Hemoglobin' AND value_num < 8"


def subscribe(conn, name, query=Q, where=WHERE, fields=("patient_id", "value_num"), source="silver_observations"):
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


def verdict(conn, sid, state, hub="edp_shared", revision=1):
    """The hub's verdict for a revision, as the hub writes it (its status row, keyed by the hub that runs it)."""
    conn.run("INSERT INTO subscription_registry.status (subscription_id, hub, revision, state) "
             "VALUES (CAST(:s AS uuid), :h, :r, :st) ON CONFLICT (subscription_id, hub) DO UPDATE "
             "SET revision = excluded.revision, state = excluded.state, observed_at = now()",
             s=sid, h=hub, r=revision, st=state)  # fmt: skip


def beat(conn, source="silver_observations", hub="edp_shared", tier="realtime", batch_id=7, rules=3):
    """A hub's heartbeat for one source, as the hub writes it every batch."""
    conn.run("INSERT INTO subscription_registry.hub_heartbeat (hub, tier, source, last_batch_at, batch_id, rules) "
             "VALUES (:h, :t, :s, now(), :b, :n) ON CONFLICT (hub, tier, source) DO UPDATE "
             "SET last_batch_at = now(), batch_id = excluded.batch_id, rules = excluded.rules",
             h=hub, t=tier, s=source, b=batch_id, n=rules)  # fmt: skip


def listing(conn):
    return conn.run("SELECT subscription_api.list_subscriptions()")[0][0]


def test_a_rule_is_stored_with_its_canonical_query(pg):
    r = subscribe(pg("app_a"), "low-hgb")
    assert r["state"] == "ACCEPTED" and r["revision"] == 1
    assert r["destination"] == "cat.subscribers.app-a"
    assert r["query"] == Q


def test_resubscribing_the_same_rule_keeps_the_revision_and_a_change_adds_one(pg):
    a = pg("app_a")
    first = subscribe(a, "rev-test", query="SELECT patient_id FROM silver_observations WHERE value_num > 6",
                      where="value_num > 6", fields=["patient_id"])  # fmt: skip
    same = subscribe(a, "rev-test", query="SELECT patient_id FROM silver_observations WHERE value_num > 6",
                     where="value_num > 6", fields=["patient_id"])  # fmt: skip
    changed = subscribe(a, "rev-test", query="SELECT patient_id FROM silver_observations WHERE value_num > 7",
                        where="value_num > 7", fields=["patient_id"])  # fmt: skip
    assert same["revision"] == first["revision"] and changed["revision"] == first["revision"] + 1


def test_grants_bound_sources_and_output_fields(pg):
    assert error(pg("app_a"), "x", source="silver_encounters") == "SOURCE_DENIED"
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
        pg("app_b"), "b-rule", query="SELECT patient_id FROM silver_observations", where=None, fields=["patient_id"]
    )
    api = pg("api")
    assert api.run("SELECT app_role FROM subscription_registry.apps WHERE service_principal = 'sp-b'")[0][0] == "app_b"
    assert api.run("SELECT count(*) FROM subscription_registry.subscriptions")[0][0] == 0  # RLS: owner is the app


def test_apps_cannot_see_or_take_over_each_others_subscriptions(pg):
    a, b = pg("app_a"), pg("app_b")
    subscribe(b, "b-only", query="SELECT patient_id FROM silver_observations", where=None, fields=["patient_id"])
    names = [s["name"] for s in a.run("SELECT subscription_api.list_subscriptions()")[0][0]["subscriptions"]]
    assert "b-only" not in names
    assert a.run("SELECT count(*) FROM subscription_registry.subscriptions WHERE name = 'b-only'")[0][0] == 0
    # Writing directly (skipping the API) still can't claim another owner: the trigger stamps the caller.
    a.run(
        "INSERT INTO subscription_registry.subscriptions (owner, name, source, query, fields) "
        "VALUES ('app_b', 'spoof', 'silver_observations', 'SELECT patient_id FROM silver_observations', '{patient_id}')"
    )
    assert pg().run("SELECT owner FROM subscription_registry.subscriptions WHERE name = 'spoof'")[0][0] == "app_a"


def test_the_same_name_in_two_apps_is_two_subscriptions(pg):
    a = subscribe(pg("app_a"), "shared-name")
    b = subscribe(pg("app_b"), "shared-name", query="SELECT patient_id FROM silver_observations", where=None,
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
        a.run(
            "INSERT INTO subscription_registry.status (subscription_id, hub, revision, state) "
            "SELECT subscription_id, 'edp_shared', revision, 'ACTIVE' FROM subscription_registry.subscriptions"
        )
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        a.run("UPDATE subscription_registry.apps SET hub = 'bu_demo'")  # the platform assigns an app's hub


def test_list_reports_the_hub_verdict_and_heartbeat(pg):
    a, owner = pg("app_a"), pg()
    sid = subscribe(a, "hb")["subscriptionId"]
    listed = {s["name"]: s for s in a.run("SELECT subscription_api.list_subscriptions()")[0][0]["subscriptions"]}
    assert listed["hb"]["state"] == "PENDING" and listed["hb"]["query"] == Q
    verdict(owner, sid, "ACTIVE")
    beat(owner)
    out = listing(a)
    assert {s["name"]: s for s in out["subscriptions"]}["hb"]["state"] == "ACTIVE"
    assert {s["name"]: s for s in out["subscriptions"]}["hb"]["hub"] == "edp_shared", "the hub that runs it"
    assert "silver_observations" in out["hub"] and out["hubName"] == "edp_shared"


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
    subscribe(b, "mon-b", query="SELECT patient_id FROM silver_observations", where=None, fields=["patient_id"])
    subscribe(a, "mon-gone")
    a.run("SELECT subscription_api.unsubscribe('mon-gone')")
    verdict(owner, sid, "ACTIVE")

    rows = owner.run("SELECT subscription, app_name, service_principal, rule, state, seconds_to_confirm, hub, tier "
                     "FROM subscription_monitor.subscriptions WHERE subscription LIKE 'mon-%'")  # fmt: skip
    seen = {r[0]: r for r in rows}
    assert seen["mon-a"][1:5] == ["app-a", "sp-a", Q, "ACTIVE"] and seen["mon-a"][5] is not None
    assert seen["mon-a"][6:] == ["edp_shared", "realtime"], "the hub that runs it"
    assert seen["mon-b"][1] == "app-b" and seen["mon-b"][4] == "PENDING", "both apps' rules, in one view"
    assert seen["mon-gone"][4] == "CANCELLING", "cancelled, waiting for the hub to stop it"
    beat(owner, tier="deferred")
    hub = owner.run("SELECT source, hub, tier FROM subscription_monitor.hub WHERE hub = 'edp_shared'")
    assert ["deferred:silver_observations", "edp_shared", "deferred"] in hub, "deferred sources keep their prefix"
    assert owner.run("SELECT count(*) FROM subscription_monitor.delivery")[0][0] >= 0


def test_apps_and_the_service_cannot_read_the_monitor(pg):
    for role in ("app_a", "api"):
        with pytest.raises(pg8000.DatabaseError, match="permission denied"):
            pg(role).run("SELECT * FROM subscription_monitor.subscriptions")


def cap(owner, hub, value):
    owner.run("UPDATE subscription_registry.hubs SET max_enabled_rules = :v WHERE name = :h", v=value, h=hub)


def enabled(owner, hub, tier):
    return owner.run("SELECT subscription_registry.enabled_rules(:h, :t)", h=hub, t=tier)[0][0]


def test_each_hub_runs_at_most_a_capped_number_of_rules_across_its_apps(pg):
    owner, a, b = pg(), pg("app_a"), pg("app_b")
    cap(owner, "edp_shared", enabled(owner, "edp_shared", "realtime") + 2)
    try:
        subscribe(a, "cap-1")
        subscribe(b, "cap-2", query="SELECT patient_id FROM silver_observations", where=None, fields=["patient_id"])
        assert error(a, "cap-3") == "RULE_LIMIT_REACHED", "the cap counts every app's rules on the hub"
        assert (
            subscribe(a, "cap-1", query=Q.replace("< 8", "< 9"), where=WHERE.replace("< 8", "< 9"))["revision"] == 2
        ), "changing an enabled rule doesn't take another slot"
        a.run("SELECT subscription_api.unsubscribe('cap-1')")
        assert subscribe(a, "cap-3")["state"] == "ACCEPTED", "cancelling frees a slot"
        assert error(a, "cap-1") == "RULE_LIMIT_REACHED", "re-enabling a cancelled rule takes a slot again"
        assert subscribe(pg("app_c"), "cap-team")["hub"] == "bu_demo", "a team's hub has a cap of its own"
    finally:
        cap(owner, "edp_shared", 2000)


def test_the_cap_is_per_hub_and_tier_and_a_tier_change_takes_a_slot_on_its_target(pg):
    owner, a, c = pg(), pg("app_a"), pg("app_c")
    subscribe_as(c, "tier-move")  # realtime: runs on bu_demo
    cap(owner, "edp_shared", enabled(owner, "edp_shared", "deferred") + 1)
    try:
        assert subscribe_as(a, "deferred-1", tier="deferred")["hub"] == "edp_shared"
        with pytest.raises(pg8000.DatabaseError) as e:
            subscribe_as(a, "deferred-2", tier="deferred")
        message = e.value.args[0]["M"]
        assert message.startswith("RULE_LIMIT_REACHED:") and "edp_shared" in message and "deferred" in message
        assert refused(c, "tier-move", tier="deferred") == "RULE_LIMIT_REACHED", (
            "a team hub's deferred rules run on the shared hub's deferred pipeline: moving one there takes a slot"
        )
        assert enabled(owner, "bu_demo", "realtime") >= 1 and enabled(owner, "bu_demo", "deferred") == 0
    finally:
        cap(owner, "edp_shared", 2000)
    assert subscribe_as(c, "tier-move", tier="deferred")["hub"] == "edp_shared"


def test_registry_sql_is_idempotent(pg):
    pg().run(SQL)  # applied on every `make registry`


def subscribe_as(conn, name, template=None, tier="realtime", fields=("patient_id", "value_num"), where="value_num > 6"):
    return conn.run(
        "SELECT subscription_api.subscribe(:n, 'silver_observations', :q, :w, CAST(:f AS text[]), :t, :tier)",
        n=name, q=f"SELECT {', '.join(fields)} FROM silver_observations WHERE {where}", w=where, f=list(fields),
        t=template, tier=tier,
    )[0][0]  # fmt: skip


def refused(conn, *args, **kwargs):
    with pytest.raises(pg8000.DatabaseError) as e:
        subscribe_as(conn, *args, **kwargs)
    return e.value.args[0]["M"].split(":")[0]


def test_a_subscription_naming_a_template_reads_only_that_templates_fields(pg):
    """Templates are the platform's named grants per workflow: a subscription that names one is held to it."""
    a = pg("app_a")
    r = subscribe_as(a, "tpl-ok", template="lab-values")
    assert (r["template"], r["tier"]) == ("lab-values", "realtime")
    # granted to the app, but not to the template
    assert refused(a, "tpl-wide", template="lab-values", fields=("patient_id", "observation_cd_desc")) == (
        "TEMPLATE_DENIED"
    )
    assert refused(a, "tpl-none", template="no-such-template") == "TEMPLATE_DENIED"
    assert refused(pg("app_b"), "tpl-other", template="lab-values", fields=("patient_id",)) == "TEMPLATE_DENIED"


def test_a_subscription_has_a_tier_realtime_by_default(pg):
    a = pg("app_a")
    assert subscribe_as(a, "tier-default")["tier"] == "realtime"
    deferred = subscribe_as(a, "tier-default", tier="deferred")
    assert deferred["tier"] == "deferred" and deferred["revision"] == 2, "a tier change is a new revision"
    assert refused(a, "tier-bad", tier="urgent") == "TIER_INVALID"
    listed = {s["name"]: s for s in a.run("SELECT subscription_api.list_subscriptions()")[0][0]["subscriptions"]}
    assert (listed["tier-default"]["tier"], listed["tier-default"]["template"]) == ("deferred", None)


def test_apps_see_only_their_own_templates(pg):
    assert pg("app_b").run("SELECT count(*) FROM subscription_registry.templates")[0][0] == 0
    assert pg("app_a").run("SELECT name FROM subscription_registry.templates") == [["lab-values"]]


def test_hub_progress_records_a_restarted_hubs_batches_per_hub(pg):
    """A new checkpoint numbers batches from 0 again: its batches replace the old rows, they aren't dropped. Two hubs on
    the same source keep their own rows."""
    conn = pg()

    def progress(hub, rules, collect_ms, age=None, spark=None):
        conn.run("INSERT INTO subscription_registry.hub_progress (hub, tier, source, batch_id, rules, refresh_ms, "
                 "collect_ms, match_ms, deliver_ms, age_ms, spark_ms) VALUES (:h, 'realtime', 'silver_orders', 0, :n, "
                 "1, :c, 3, 40, :age, CAST(:spark AS jsonb)) ON CONFLICT (hub, tier, source, batch_id) DO UPDATE "
                 "SET at = now(), rules = excluded.rules, collect_ms = excluded.collect_ms, age_ms = excluded.age_ms, "
                 "spark_ms = excluded.spark_ms", h=hub, n=rules, c=collect_ms, age=age, spark=spark)  # fmt: skip

    progress("edp_shared", 1, 900)
    progress("edp_shared", 2, 800, age=180, spark='{"addBatch": 700}')
    progress("bu_demo", 5, 50)
    assert conn.run("SELECT hub, rules, collect_ms, age_ms, spark_ms->>'addBatch' FROM subscription_registry."
                    "hub_progress WHERE source = 'silver_orders' AND batch_id = 0 ORDER BY hub") == [
        ["bu_demo", 5, 50, None, None], ["edp_shared", 2, 800, 180, "700"]]  # fmt: skip


# -- Hubs: edp_shared (the default) and a team's own -------------------------------------------------------------------
def test_every_app_has_a_hub_and_edp_shared_is_the_default(pg):
    owner = pg()
    assert owner.run("SELECT name, runs_deferred FROM subscription_registry.hubs ORDER BY name") == [
        ["bu_demo", False], ["edp_shared", True]]  # fmt: skip
    assert owner.run("SELECT app_role, hub FROM subscription_registry.apps ORDER BY app_role") == [
        ["app_a", "edp_shared"], ["app_b", "edp_shared"], ["app_c", "bu_demo"]]  # fmt: skip
    default = owner.run("SELECT column_default FROM information_schema.columns WHERE table_schema = "
                        "'subscription_registry' AND table_name = 'apps' AND column_name = 'hub'")  # fmt: skip
    assert default == [["'edp_shared'::text"]]
    assert owner.run(
        "SELECT subscription_registry.runs_on('bu_demo', 'realtime'), "
        "subscription_registry.runs_on('bu_demo', 'deferred')"
    ) == [["bu_demo", "edp_shared"]]
    with pytest.raises(pg8000.DatabaseError):
        owner.run("UPDATE subscription_registry.apps SET hub = 'no_such_hub' WHERE app_role = 'app_a'")
    with pytest.raises(pg8000.DatabaseError):  # one hub runs every app's deferred rules
        owner.run("UPDATE subscription_registry.hubs SET runs_deferred = true WHERE name = 'bu_demo'")
    assert owner.run("SELECT subscription_registry.hubs_of('hub_sp')") == [["bu_demo"]]
    assert owner.run("SELECT subscription_registry.hub_apps('hub_sp')") == [["app_c"]]


def test_onboarding_a_hub_is_idempotent(pg):
    owner = pg()
    for _ in range(2):
        owner.run("SELECT subscription_registry.onboard_hub('bu_other', 'sp-x', 'hub_bu_other_eventhubs', 500)")
    assert owner.run("SELECT principal, eventhubs_credential, max_enabled_rules, runs_deferred FROM "
                     "subscription_registry.hubs WHERE name = 'bu_other'") == [["sp-x", "hub_bu_other_eventhubs", 500,
                                                                                 False]]  # fmt: skip
    assert owner.run("SELECT pg_has_role('hub_bu_other', 'hub', 'MEMBER')") == [[True]]
    with pytest.raises(pg8000.DatabaseError):
        owner.run("SELECT subscription_registry.onboard_hub('Bad-Name', 'sp-x', 'c')")
    owner.run("DELETE FROM subscription_registry.hubs WHERE name = 'bu_other'")
    owner.run("DROP ROLE hub_bu_other")


def test_a_team_hubs_identity_reads_only_its_apps_realtime_rules(pg):
    c, a = pg("app_c"), pg("app_a")
    subscribe_as(c, "team-rt")
    subscribe_as(c, "team-deferred", tier="deferred")
    subscribe_as(a, "shared-rt")
    hub = pg("hub_sp")
    assert hub.run("SELECT DISTINCT owner FROM subscription_registry.subscriptions") == [["app_c"]]
    names = {r[0] for r in hub.run("SELECT name FROM subscription_registry.subscriptions")}
    assert "team-rt" in names and "team-deferred" not in names, "its apps' deferred rules run on the shared hub"
    assert hub.run("SELECT app_role FROM subscription_registry.apps") == [["app_c"]]
    assert hub.run("SELECT name FROM subscription_registry.hubs") == [["bu_demo"]]
    assert hub.run("SELECT count(*) FROM subscription_registry.templates")[0][0] == 0
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        hub.run("UPDATE subscription_registry.subscriptions SET enabled = false")
    with pytest.raises(pg8000.DatabaseError, match="permission denied"):
        hub.run("SELECT * FROM subscription_monitor.subscriptions")
    shared = pg()  # the shared hub runs as the registry owner, and filters by hub itself
    assert {
        r[0]
        for r in shared.run(
            "SELECT subscription_registry.executing_hub(owner, tier) FROM "
            "subscription_registry.subscriptions WHERE owner = 'app_c'"
        )
    } == {"bu_demo", "edp_shared"}


def test_a_team_hub_writes_only_its_own_rows(pg):
    hub, owner = pg("hub_sp"), pg()
    sid = subscribe_as(pg("app_c"), "team-writes")["subscriptionId"]
    beat(hub, hub="bu_demo")
    verdict(hub, sid, "PLACED", hub="bu_demo")
    hub.run(
        "INSERT INTO subscription_registry.hub_progress (hub, tier, source, batch_id, rules, refresh_ms) "
        "VALUES ('bu_demo', 'realtime', 'silver_observations', 1, 1, 2)"
    )
    hub.run(
        "INSERT INTO subscription_registry.revisions (hub, subscription_id, revision, owner, name, source, "
        "where_sql, fields, topic, granted) VALUES ('bu_demo', CAST(:s AS uuid), 1, 'app_c', 'team-writes', "
        "'silver_observations', 'value_num > 6', '{patient_id}', 't', '{patient_id}')",
        s=sid,
    )
    hub.run(
        "INSERT INTO subscription_registry.bindings (binding_id, hub, lane, app, source, query_id, batch_id, "
        "offsets, revisions, n, digest) VALUES ('bu_demo:realtime:app_c:silver_observations:q:1', 'bu_demo', "
        "'realtime', 'app_c', 'silver_observations', 'q', 1, '{}', '{}', 0, 'd')"
    )
    hub.run(
        "INSERT INTO subscription_registry.replays (binding_id, hub, live_same, digest_match, n) "
        "VALUES ('bu_demo:realtime:app_c:silver_observations:q:1', 'bu_demo', true, true, 0)"
    )
    hub.run(
        "INSERT INTO subscription_registry.pending (hub, lane, app, topic, key, value, binding_id, "
        "notification_id) VALUES ('bu_demo', 'realtime', 'app_c', 'cat.subscribers.app-c', 'P1', '{}', 'b1', 'n1')"
    )
    hub.run(
        "INSERT INTO subscription_registry.app_delivery (hub, lane, app, state, pending) "
        "VALUES ('bu_demo', 'realtime', 'app_c', 'OPEN', 1)"
    )
    hub.run(
        "INSERT INTO subscription_registry.app_delivery_log (hub, lane, app, state, pending) "
        "VALUES ('bu_demo', 'realtime', 'app_c', 'OPEN', 1)"
    )
    for table, columns, values in (
        ("hub_heartbeat", "hub, tier, source, last_batch_at, batch_id, rules",
         "'edp_shared', 'realtime', 'silver_observations', now(), 1, 1"),
        ("status", "subscription_id, hub, revision, state", f"CAST('{sid}' AS uuid), 'edp_shared', 1, 'ACTIVE'"),
        ("pending", "hub, lane, app, topic, key, value", "'edp_shared', 'realtime', 'app_a', 't', 'k', 'v'"),
        ("app_delivery", "hub, lane, app, state, pending", "'edp_shared', 'realtime', 'app_a', 'OPEN', 0"),
    ):  # fmt: skip
        with pytest.raises(pg8000.DatabaseError, match="row-level security"):
            hub.run(f"INSERT INTO subscription_registry.{table} ({columns}) VALUES ({values})")
    beat(owner, hub="edp_shared")  # the shared hub's own heartbeat (it runs as the registry owner)
    assert {r[0] for r in hub.run("SELECT hub FROM subscription_registry.hub_heartbeat")} == {"bu_demo"}
    assert hub.run("SELECT count(*) FROM subscription_registry.pending WHERE app = 'app_c'")[0][0] == 1
    hub.run("DELETE FROM subscription_registry.pending WHERE hub = 'bu_demo'")  # delivered: the drainer removes them
    assert owner.run("SELECT app_name, state, pending FROM subscription_monitor.delivery WHERE hub = 'bu_demo'") == [
        ["app-c", "OPEN", 1]]  # fmt: skip


def test_an_app_sees_only_the_heartbeats_of_the_hubs_that_run_its_rules(pg):
    owner = pg()
    for hub, tier in (("edp_shared", "realtime"), ("edp_shared", "deferred"), ("bu_demo", "realtime")):
        beat(owner, source="silver_orders", hub=hub, tier=tier)
    shared, team = listing(pg("app_a")), listing(pg("app_c"))
    pairs = {(h["hub"], h["tier"]) for h in shared["heartbeats"] if h["source"] == "silver_orders"}
    assert pairs == {("edp_shared", "realtime"), ("edp_shared", "deferred")}
    pairs = {(h["hub"], h["tier"]) for h in team["heartbeats"] if h["source"] == "silver_orders"}
    assert pairs == {("bu_demo", "realtime"), ("edp_shared", "deferred")}, "its own hub, plus where deferred rules run"
    assert team["hubName"] == "bu_demo" and {"silver_orders", "deferred:silver_orders"} <= set(team["hub"])
    rows = pg("app_c").run("SELECT DISTINCT hub, tier FROM subscription_registry.hub_heartbeat ORDER BY 1, 2")
    assert rows == [["bu_demo", "realtime"], ["edp_shared", "deferred"]], "row-level security, not just the listing"
    assert all(set(h) == {"hub", "tier", "source", "lastBatchAt", "batchId", "rules"} for h in team["heartbeats"])


def test_a_verdict_counts_only_from_the_hub_that_runs_the_rule(pg):
    owner, c = pg(), pg("app_c")
    sid = subscribe_as(c, "wrong-hub")["subscriptionId"]
    verdict(owner, sid, "ACTIVE", hub="edp_shared")  # a hub that doesn't run app-c's realtime rules
    assert {s["name"]: s for s in listing(c)["subscriptions"]}["wrong-hub"]["state"] == "PENDING"
    verdict(owner, sid, "PLACED", hub="bu_demo")
    got = {s["name"]: s for s in listing(c)["subscriptions"]}["wrong-hub"]
    assert (got["state"], got["hub"]) == ("PLACED", "bu_demo")


def test_placed_at_is_kept_when_the_revision_becomes_active(pg):
    owner, a = pg(), pg("app_a")
    sid = subscribe_as(a, "placed")["subscriptionId"]

    def row():
        return owner.run("SELECT state, placed_at, observed_at FROM subscription_registry.status "
                         "WHERE subscription_id = CAST(:s AS uuid) AND hub = 'edp_shared'", s=sid)[0]  # fmt: skip

    verdict(owner, sid, "PLACED")
    state, placed, _ = row()
    assert state == "PLACED" and placed is not None
    verdict(owner, sid, "ACTIVE")
    state, kept, active_at = row()
    assert (state, kept) == ("ACTIVE", placed) and active_at >= placed
    verdict(owner, sid, "PLACED")  # placed again (a restarted hub): the revision it runs is still ACTIVE
    assert row()[:2] == ["ACTIVE", placed]
    moved = subscribe_as(a, "placed", where="value_num > 7")
    verdict(owner, sid, "PLACED", revision=moved["revision"])
    assert row()[0] == "PLACED" and row()[1] > placed, "a new revision is placed afresh"
    seconds = owner.run("SELECT seconds_to_place FROM subscription_monitor.subscriptions WHERE subscription = 'placed'")
    assert seconds[0][0] is not None


def test_the_registry_migrates_from_before_hubs(pg):
    """An existing registry (one shared hub, keys without a hub) upgrades in place: its rows move to edp_shared."""
    owner = pg()
    owner.run("DROP DATABASE IF EXISTS before_hubs")
    owner.run("CREATE DATABASE before_hubs")
    old = pg(database="before_hubs")
    old.run((ROOT / "tests/fixtures/registry_before_hubs.sql").read_text())
    old.run(
        "INSERT INTO subscription_registry.apps (app_role, app_name, destination_topic, allowed_sources, "
        "allowed_fields) VALUES ('app_a', 'app-a', 't', '{silver_observations}', '{patient_id,value_num}')"
    )
    app = pg("app_a", database="before_hubs")
    rt = subscribe(app, "old-rt", query="SELECT patient_id FROM silver_observations", where=None, fields=["patient_id"])
    deferred = app.run(
        "SELECT subscription_api.subscribe('old-deferred', 'silver_observations', "
        "'SELECT patient_id FROM silver_observations', NULL, '{patient_id}', NULL, 'deferred')"
    )[0][0]
    for s in (rt, deferred):
        old.run("INSERT INTO subscription_registry.status (subscription_id, revision, state) "
                "VALUES (CAST(:s AS uuid), 1, 'ACTIVE')", s=s["subscriptionId"])  # fmt: skip
    old.run(
        "INSERT INTO subscription_registry.hub_heartbeat VALUES ('silver_observations', now(), 9, 2), "
        "('deferred:silver_observations', now(), 4, 1)"
    )
    old.run(
        "INSERT INTO subscription_registry.hub_progress (source, batch_id, rules, refresh_ms, prepare_ms, "
        "write_ms, report_ms) VALUES ('silver_observations', 9, 2, 1, 0, 500, 5), "
        "('deferred:silver_observations', 9, 1, 1, 0, 300, 5)"
    )

    for _ in range(2):  # and again: a migrated registry is idempotent too
        old.run(SQL)
    assert old.run("SELECT hub, tier, source, batch_id FROM subscription_registry.hub_heartbeat ORDER BY tier") == [
        ["edp_shared", "deferred", "silver_observations", 4],
        ["edp_shared", "realtime", "silver_observations", 9],
    ]
    assert old.run("SELECT hub, tier, source, write_ms FROM subscription_registry.hub_progress ORDER BY tier") == [
        ["edp_shared", "deferred", "silver_observations", 300],
        ["edp_shared", "realtime", "silver_observations", 500],
    ]
    assert old.run("SELECT DISTINCT hub, state FROM subscription_registry.status") == [["edp_shared", "ACTIVE"]]
    assert old.run("SELECT hub FROM subscription_registry.apps") == [["edp_shared"]]
    assert old.run("SELECT to_regclass('subscription_registry.limits')") == [[None]], "the cap lives on hubs now"
    states = {s["name"]: s["state"] for s in listing(app)["subscriptions"]}
    assert states == {"old-rt": "ACTIVE", "old-deferred": "ACTIVE"}, "verdicts survive the upgrade"
    app.close()
    old.close()
    owner.run("DROP DATABASE before_hubs")
