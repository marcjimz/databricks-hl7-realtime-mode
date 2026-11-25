"""`make registry` onboards the subscription hubs, against a real PostgreSQL with src/registry/registry.sql applied:
edp_shared and each team hub of terraform var subscriber_hubs, which hub serves each app, and the team hub's own login
role, so its pipeline reads the registry as itself, not as the owner. Then the apps themselves (from Terraform or a
target file of the subscriptions bundle): each one's NOLOGIN role, binding, and templates."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def registry():
    spec = importlib.util.spec_from_file_location("registry_bootstrap", ROOT / "infra/registry.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def db(pg):
    conn = pg()
    yield conn
    conn.close()


def _hub(app_id, cap=2000):
    return {"application_id": app_id, "max_enabled_rules": cap}


def _apps(**hubs):
    """terraform output subscriber_apps as onboarding reads it: app name -> its hub (conftest's apps a, b, c)."""
    return {name: {"subscription_hub": hub} for name, hub in hubs.items()}


TEAM = _apps(a="edp_shared", b="bu_demo", c="bu_demo")  # app-c is conftest's bu_demo app


def _hubs(db):
    rows = db.run("SELECT name, principal, eventhubs_credential, max_enabled_rules, runs_deferred "
                  "FROM subscription_registry.hubs")  # fmt: skip
    return {r[0]: tuple(r[1:]) for r in rows}


def _app_hubs(db):
    return dict(db.run("SELECT app_name, hub FROM subscription_registry.apps"))


def _member(db, role, of):
    return bool(db.run("SELECT pg_has_role(:r, :g, 'MEMBER')", r=role, g=of)[0][0])


def _role_exists(db, role):
    return bool(db.run("SELECT 1 FROM pg_roles WHERE rolname = :r", r=role))


def test_a_team_hub_is_registered_with_its_own_login_and_serves_only_its_apps(registry, db):
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub", 500)}, TEAM, "owner", "cat")
    assert _hubs(db) == {
        "edp_shared": ("owner", "cat_eventhubs", 2000, True),  # the platform's: runs as the deployer, runs deferred
        "bu_demo": ("sp-hub", "hub_bu_demo_eventhubs", 500, False),
    }
    assert _app_hubs(db) == {"app-a": "edp_shared", "app-b": "bu_demo", "app-c": "bu_demo"}
    assert _member(db, "sp-hub", "hub_bu_demo") and _member(db, "hub_bu_demo", "hub")
    assert not _member(db, "sp-hub", "subscriber") and not _member(db, "sp-hub", "subscription_service")


def test_onboarding_again_changes_nothing(registry, db):
    before = (_hubs(db), _app_hubs(db))
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub", 500)}, TEAM, "owner", "cat")
    assert (_hubs(db), _app_hubs(db)) == before


def test_a_new_service_principal_for_a_hub_replaces_the_old_one(registry, db):
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub-2", 500)}, TEAM, "owner", "cat")
    assert _hubs(db)["bu_demo"][0] == "sp-hub-2"
    assert _member(db, "sp-hub-2", "hub_bu_demo") and not _member(db, "sp-hub", "hub_bu_demo")


def test_a_hub_removed_from_terraform_is_deleted_after_its_apps_move_back_to_edp_shared(registry, db):
    registry.onboard_hubs(db, {}, _apps(a="edp_shared", b="edp_shared", c="edp_shared"), "owner", "cat")
    assert set(_hubs(db)) == {"edp_shared"}
    assert _app_hubs(db) == {"app-a": "edp_shared", "app-b": "edp_shared", "app-c": "edp_shared"}
    assert not _role_exists(db, "hub_bu_demo")


def test_an_app_without_a_hub_is_served_by_edp_shared(registry, db):
    apps = {"a": {}, "b": {"subscription_hub": "bu_demo"}, "c": {}}
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub")}, apps, "owner", "cat")
    assert _app_hubs(db) == {"app-a": "edp_shared", "app-b": "bu_demo", "app-c": "edp_shared"}


def test_edp_shared_writes_with_the_credential_it_is_given(registry, db):
    """The subscription layer's bundle names edp_shared's credential (its target's eh_service_credential)."""
    registry.onboard_hubs(db, {}, _apps(a="edp_shared", b="edp_shared", c="edp_shared"), "owner", "cat", "lab_hub_eh")
    assert _hubs(db)["edp_shared"] == ("owner", "lab_hub_eh", 2000, True)


# Apps (onboard_apps), as both inputs give them: here a user, as the subscription layer's notebook test onboards one.
NOTEBOOK = {
    "notebook-test": {
        "principal": "You@Example.com", "topic": "lab.subscribers.notebook-test", "eventhubs_credential": "sub_nb_eh",
        "sources": ["epic_realtime"], "fields": ["patient_id", "msg_type"], "subscription_hub": "edp_shared",
        "templates": {"lab-values": {"sources": ["epic_realtime"], "fields": ["patient_id"]}},
    }
}  # fmt: skip


def _app(db, name):
    return db.run("SELECT app_role, service_principal, destination_topic, allowed_sources, allowed_fields, "
                  "eventhubs_credential, hub FROM subscription_registry.apps WHERE app_name = :n", n=name)  # fmt: skip


def test_a_user_principal_is_stored_lowercase_and_its_app_gets_its_role(registry, db):
    done = registry.onboard_apps(db, NOTEBOOK)
    assert _app(db, "notebook-test") == [[
        "app_notebook_test", "you@example.com", "lab.subscribers.notebook-test", ["epic_realtime"],
        ["patient_id", "msg_type"], "sub_nb_eh", "edp_shared",
    ]]  # fmt: skip
    assert _member(db, "app_notebook_test", "subscriber") and _member(db, "subscription_service", "app_notebook_test")
    assert db.run("SELECT rolcanlogin FROM pg_roles WHERE rolname = 'app_notebook_test'") == [[False]]
    templates = "SELECT name, sources, fields FROM subscription_registry.templates WHERE app_role = 'app_notebook_test'"
    assert db.run(templates) == [["lab-values", ["epic_realtime"], ["patient_id"]]]
    assert set(_app_hubs(db)) == {"notebook-test"}, "apps no longer configured lose their binding"
    assert "removed  app app-a (no longer in subscriber_apps, or its old binding)" in done


def test_onboarding_apps_again_changes_nothing(registry, db):
    before = _app(db, "notebook-test")
    assert registry.onboard_apps(db, NOTEBOOK) == []
    assert _app(db, "notebook-test") == before


def test_a_service_principal_is_stored_as_its_application_id(registry, db):
    sp = {**NOTEBOOK["notebook-test"], "principal": "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d", "templates": {}}
    registry.onboard_apps(db, {"notebook-test": sp})
    assert _app(db, "notebook-test")[0][1] == "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"
    assert not db.run("SELECT 1 FROM subscription_registry.templates WHERE app_role = 'app_notebook_test'")
