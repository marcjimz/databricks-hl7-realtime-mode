"""`make registry` onboards the subscription hubs, against a real PostgreSQL with src/registry/registry.sql applied:
edp_shared and each team hub of terraform var subscriber_hubs, which hub serves each app, and the team hub's own login
role, so its pipeline reads the registry as itself, not as the owner."""

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


# TODO(merge): drop this stand-in once registry.sql has the hubs table (workstream A); it mirrors A's contract.
_STAND_IN = """
CREATE TABLE subscription_registry.hubs (name text PRIMARY KEY, principal text, eventhubs_credential text,
  max_enabled_rules int NOT NULL DEFAULT 2000, runs_deferred bool NOT NULL DEFAULT false, created_at timestamptz);
INSERT INTO subscription_registry.hubs (name, runs_deferred) VALUES ('edp_shared', true);
ALTER TABLE subscription_registry.apps
  ADD COLUMN hub text NOT NULL DEFAULT 'edp_shared' REFERENCES subscription_registry.hubs;
CREATE ROLE hub NOLOGIN;
"""


@pytest.fixture
def db(pg):
    conn = pg()
    if not conn.run("SELECT to_regclass('subscription_registry.hubs')")[0][0]:
        conn.run(_STAND_IN)
    yield conn
    conn.close()


def _hub(app_id, cap=2000):
    return {"application_id": app_id, "max_enabled_rules": cap}


def _apps(**hubs):
    """terraform output subscriber_apps as onboarding reads it: app name -> its hub (conftest's apps a and b)."""
    return {name: {"subscription_hub": hub} for name, hub in hubs.items()}


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
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub", 500)}, _apps(a="edp_shared", b="bu_demo"), "owner", "cat")
    assert _hubs(db) == {
        "edp_shared": ("owner", "cat_eventhubs", 2000, True),  # the platform's: runs as the deployer, runs deferred
        "bu_demo": ("sp-hub", "hub_bu_demo_eventhubs", 500, False),
    }
    assert _app_hubs(db) == {"app-a": "edp_shared", "app-b": "bu_demo"}
    assert _member(db, "sp-hub", "hub_bu_demo") and _member(db, "hub_bu_demo", "hub")
    assert not _member(db, "sp-hub", "subscriber") and not _member(db, "sp-hub", "subscription_service")


def test_onboarding_again_changes_nothing(registry, db):
    before = (_hubs(db), _app_hubs(db))
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub", 500)}, _apps(a="edp_shared", b="bu_demo"), "owner", "cat")
    assert (_hubs(db), _app_hubs(db)) == before


def test_a_new_service_principal_for_a_hub_replaces_the_old_one(registry, db):
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub-2", 500)}, _apps(a="edp_shared", b="bu_demo"), "owner", "cat")
    assert _hubs(db)["bu_demo"][0] == "sp-hub-2"
    assert _member(db, "sp-hub-2", "hub_bu_demo") and not _member(db, "sp-hub", "hub_bu_demo")


def test_a_hub_removed_from_terraform_is_deleted_after_its_apps_move_back_to_edp_shared(registry, db):
    registry.onboard_hubs(db, {}, _apps(a="edp_shared", b="edp_shared"), "owner", "cat")
    assert set(_hubs(db)) == {"edp_shared"}
    assert _app_hubs(db) == {"app-a": "edp_shared", "app-b": "edp_shared"}
    assert not _role_exists(db, "hub_bu_demo")


def test_an_app_without_a_hub_is_served_by_edp_shared(registry, db):
    apps = {"a": {}, "b": {"subscription_hub": "bu_demo"}}
    registry.onboard_hubs(db, {"bu_demo": _hub("sp-hub")}, apps, "owner", "cat")
    assert _app_hubs(db) == {"app-a": "edp_shared", "app-b": "bu_demo"}
