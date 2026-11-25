"""infra/registry.py's two inputs: Terraform's outputs (the platform's dev environment, `make registry`) or a target
file of the subscription layer's bundle (subscriptions/targets/<env>.yml, read as the Databricks CLI reads it), and the
pure parts of what it then creates: each app's grant, views, and API access."""

import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SP = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"


@pytest.fixture(scope="module")
def registry():
    spec = importlib.util.spec_from_file_location("registry_bootstrap", ROOT / "infra/registry.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------------------------------------------------
# Resolving variables as the CLI does: ${var.<name>} and ${bundle.target}, nothing else.


def test_references_resolve_through_each_other_and_the_target(registry):
    values = {"catalog": "c", "schema": "s", "topic": "${var.catalog}.${var.schema}.t", "app": "api-${bundle.target}",
              "credential": "${var.other}", "other": "${var.catalog}_eventhubs"}  # fmt: skip
    assert registry.resolve_variables(values, "prod") == {
        "catalog": "c", "schema": "s", "topic": "c.s.t", "app": "api-prod", "credential": "c_eventhubs",
        "other": "c_eventhubs",
    }  # fmt: skip


def test_a_whole_reference_keeps_its_type_and_complex_values_resolve_inside(registry):
    values = {"n": 2, "count": "${var.n}", "label": "x${var.n}", "on": True, "flag": "is ${var.on}",
              "apps": {"a": {"topic": "${var.catalog}.a", "fields": ["${var.catalog}"]}}, "catalog": "c",
              "copy": "${var.apps}", "first": "${var.apps.a.topic}"}  # fmt: skip
    got = registry.resolve_variables(values, "t")
    assert got["count"] == 2 and got["label"] == "x2" and got["flag"] == "is true"
    assert got["apps"] == {"a": {"topic": "c.a", "fields": ["c"]}} == got["copy"]
    assert got["first"] == "c.a"


def test_other_references_are_left_for_the_cli(registry):
    values = {"root": "/Users/${workspace.current_user.userName}/x", "id": "${resources.pipelines.p.id}"}
    assert registry.resolve_variables(values, "t") == values


def test_a_reference_to_an_undeclared_variable_or_a_cycle_is_refused(registry):
    with pytest.raises(registry.ConfigError, match="catlog"):
        registry.resolve_variables({"topic": "${var.catlog}.x"}, "t")
    with pytest.raises(registry.ConfigError, match="cycle"):
        registry.resolve_variables({"a": "${var.b}", "b": "${var.a}"}, "t")


def _bundle(tmp_path: Path, variables: dict, target_variables: dict, env="lab", file=None) -> Path:
    bundle = tmp_path / "subscriptions"
    (bundle / "targets").mkdir(parents=True)
    (bundle / "databricks.yml").write_text(yaml.safe_dump({"bundle": {"name": "b"}, "variables": variables}))
    target = {"targets": {env: {"workspace": {"host": "https://h"}, "variables": target_variables}}}
    (bundle / "targets" / f"{file or env}.yml").write_text(yaml.safe_dump(target))
    return bundle


DECLARED = {
    "catalog": {"description": "required"},
    "app": {"default": "api-${bundle.target}"},
    "credential": {"default": "${var.catalog}_eventhubs"},
    "warehouse": {"default": "Starter"},
    "warehouse_id": {"lookup": {"warehouse": "${var.warehouse}"}},
    "apps": {"type": "complex", "default": {}},
}


def test_a_target_overrides_the_defaults_in_either_form_and_lookups_are_left_out(registry, tmp_path):
    bundle = _bundle(tmp_path, DECLARED, {"catalog": {"default": "c"}, "warehouse": "Mine",
                                          "apps": {"a": {"principal": SP}}})  # fmt: skip
    variables, workspace = registry.read_target("lab", bundle)
    assert variables == {"catalog": "c", "app": "api-lab", "credential": "c_eventhubs", "warehouse": "Mine",
                         "apps": {"a": {"principal": SP}}}  # fmt: skip
    assert workspace == {"host": "https://h"}


def test_a_target_that_sets_the_lookup_variable_itself_is_used(registry, tmp_path):
    bundle = _bundle(tmp_path, DECLARED, {"catalog": "c", "warehouse_id": "abc"})
    assert registry.read_target("lab", bundle)[0]["warehouse_id"] == "abc"


def test_a_target_file_that_cant_be_used_says_what_to_fix(registry, tmp_path):
    with pytest.raises(registry.ConfigError, match="target.example.yml"):
        registry.read_target("missing", _bundle(tmp_path / "1", DECLARED, {"catalog": "c"}))
    with pytest.raises(registry.ConfigError, match="no target lab"):
        registry.read_target("lab", _bundle(tmp_path / "2", DECLARED, {"catalog": "c"}, env="other", file="lab"))
    with pytest.raises(registry.ConfigError, match="catalog"):
        registry.read_target("lab", _bundle(tmp_path / "3", DECLARED, {}))
    with pytest.raises(registry.ConfigError, match="undeclared"):
        registry.read_target("lab", _bundle(tmp_path / "4", DECLARED, {"catalog": "c", "undeclared": 1}))


# ---------------------------------------------------------------------------------------------------------------------
# Config from a target file of the subscriptions bundle.

BUNDLE = yaml.safe_load((ROOT / "subscriptions/databricks.yml").read_text())["variables"]


def _target(tmp_path, registry, **overrides):
    lab_values = {"sources": ["epic_realtime"], "fields": ["patient_id", "value"]}
    variables = {"catalog": "lab", "eh_namespace": "ns", "eh_service_credential": "lab_hub_eh",
                 "subscription_tables": "main.clinical.epic_realtime, `main`.clinical.adt_events",
                 "subscriber_templates": {"lab-values": lab_values},
                 "subscriber_apps": {
                     "notebook-test": {"principal": "You@Example.com", "topic": "t1", "eventhubs_credential": "c1",
                                       "sources": ["adt_events"], "fields": ["Patient_ID", "msg_type"],
                                       "templates": ["lab-values"]},
                     "lab-alerts": {"principal": SP, "topic": "t2", "eventhubs_credential": "c2",
                                    "sources": ["epic_realtime"], "fields": ["*"]}}}  # fmt: skip
    variables.update(overrides)
    return registry.Config.from_target("lab", _bundle(tmp_path, BUNDLE, variables))


def test_a_target_configures_the_registry_from_the_bundles_own_variables(registry, tmp_path):
    config = _target(tmp_path, registry)
    assert (config.project, config.endpoint, config.branch, config.database) == (
        "hl7-subscriptions", "projects/hl7-subscriptions/branches/production/endpoints/primary",
        "projects/hl7-subscriptions/branches/production", "databricks_postgres",
    )  # fmt: skip
    assert config.api_app == "hl7-subscriptions-lab" and config.warehouse == "Serverless Starter Warehouse"
    assert (config.catalog, config.hub_schema, config.hub_credential) == ("lab", "hl7_subscriptions", "lab_hub_eh")
    assert config.registry_catalog == "" and config.hubs == {} and config.workspace == {"host": "https://h"}
    assert config.tables == {"epic_realtime": "main.clinical.epic_realtime", "adt_events": "`main`.clinical.adt_events"}


@pytest.mark.parametrize("tables", ["main.clinical.Epic_Realtime", "clinical.epic_realtime", "a.b.c, x.y.c"])
def test_a_source_is_a_tables_lowercase_name_once(registry, tmp_path, tables):
    """Rules name their source in lowercase (identifiers are case-insensitive), as Unity Catalog stores table names."""
    with pytest.raises(registry.ConfigError, match="subscription_tables"):
        _target(tmp_path, registry, subscription_tables=tables, subscriber_apps={})


def test_an_apps_grant_is_its_own_plus_its_templates_as_in_terraform(registry, tmp_path):
    apps = _target(tmp_path, registry).apps
    assert apps["notebook-test"] == {
        "principal": "you@example.com",  # a user: its email, lowercase (the API matches callers case-insensitively)
        "topic": "t1", "eventhubs_credential": "c1", "subscription_hub": "edp_shared",
        "sources": ["adt_events", "epic_realtime"], "fields": ["patient_id", "msg_type", "value"],
        "templates": {"lab-values": {"sources": ["epic_realtime"], "fields": ["patient_id", "value"]}},
    }  # fmt: skip
    assert apps["lab-alerts"]["principal"] == SP and apps["lab-alerts"]["fields"] == ["*"]


@pytest.mark.parametrize(
    "app, error",
    [
        ({"sources": ["nope"]}, "nope"),  # not one of subscription_tables
        ({"templates": ["nope"]}, "nope"),
        ({"subscription_hub": "bu_lab"}, "bu_lab"),  # not edp_shared or one of subscriber_hubs
        ({"principal": ""}, "principal"),
        ({"topic": None}, "topic"),
    ],
)
def test_an_app_the_registry_cant_onboard_is_refused_by_name(registry, tmp_path, app, error):
    base = {"principal": SP, "topic": "t", "eventhubs_credential": "c", "sources": ["epic_realtime"], "fields": ["a"]}
    with pytest.raises(registry.ConfigError, match=f"bad-app.*{error}"):
        _target(tmp_path, registry, subscriber_apps={"bad-app": {**base, **app}})


def test_an_app_name_must_be_a_registry_name(registry, tmp_path):
    with pytest.raises(registry.ConfigError, match="Bad_App"):
        _target(tmp_path, registry, subscriber_apps={"Bad_App": {}})


def test_this_bundle_deploys_edp_shared_only(registry, tmp_path):
    with pytest.raises(registry.ConfigError, match="edp_shared"):
        _target(tmp_path, registry, subscription_hub="bu_lab")


def test_without_tables_an_apps_sources_are_the_platforms_silver_tables(registry, tmp_path):
    apps = {"a": {"principal": SP, "topic": "t", "eventhubs_credential": "c", "sources": ["silver_orders"],
                  "fields": ["patient_id"]}}  # fmt: skip
    config = _target(tmp_path, registry, subscription_tables="", schema="silver", subscriber_apps=apps)
    assert config.tables == {"silver_orders": "lab.silver.silver_orders"}


# ---------------------------------------------------------------------------------------------------------------------
# The dev path: Terraform's outputs, as before.

TF = {
    "catalog": "cat",
    "schema": "hl7_realtime",
    "workspace_id": 1234,
    "subscriber_hubs": {"bu_demo": {"application_id": "sp-hub", "max_enabled_rules": 500}},
    "subscriber_apps": {"app-a": {"application_id": SP, "hub": "cat.subscribers.app-a", "sources": ["silver_orders"],
                                  "fields": ["patient_id"], "templates": {}, "subscription_hub": "bu_demo"}},
}  # fmt: skip


def test_the_dev_path_still_reads_terraform(registry, monkeypatch):
    monkeypatch.setattr(registry, "tf_output", TF.__getitem__)
    monkeypatch.delenv("HL7_API_APP", raising=False)
    config = registry.Config.from_terraform("hl7-subscriptions")
    assert config.target is None and config.workspace_id == "1234"
    assert (config.catalog, config.schema, config.hub_schema) == ("cat", "hl7_realtime", None)
    assert config.endpoint == "projects/hl7-subscriptions/branches/production/endpoints/primary"
    assert (config.api_app, config.warehouse) == ("hl7-subscriptions-dev", "Serverless Starter Warehouse")
    assert (config.hub_credential, config.registry_catalog) == ("cat_eventhubs", "cat_registry")
    assert config.tables["silver_orders"] == "cat.hl7_realtime.silver_orders" and len(config.tables) == 5
    assert config.hubs == TF["subscriber_hubs"]
    assert config.apps == {"app-a": {
        "principal": SP, "topic": "cat.subscribers.app-a", "eventhubs_credential": "sub_app_a_eventhubs",
        "sources": ["silver_orders"], "fields": ["patient_id"], "templates": {}, "subscription_hub": "bu_demo",
    }}  # fmt: skip


def test_the_dev_path_names_the_api_app_from_the_environment(registry, monkeypatch):
    monkeypatch.setattr(registry, "tf_output", TF.__getitem__)
    monkeypatch.setenv("HL7_API_APP", "hl7-subscriptions-qa")
    assert registry.Config.from_terraform("p").api_app == "hl7-subscriptions-qa"


def test_the_dev_paths_columns_are_silvers_own(registry):
    from hl7rt.subscriptions import SOURCES

    columns = registry.silver_columns()
    assert columns == {s: [c.name for c in schema] for s, schema in SOURCES.items()}


# ---------------------------------------------------------------------------------------------------------------------
# What the registry creates from a grant: pure functions.

COLUMNS = ["patient_id", "Msg_Type", "patient_point_of_care", "raw `odd` name"]


def test_an_apps_view_selects_its_granted_columns_in_the_tables_order(registry):
    sql = registry.view_sql("lab.sub_notebook_test", "epic_realtime", "main.clinical.epic_realtime", COLUMNS,
                            ["patient_point_of_care", "msg_type", "patient_id"])  # fmt: skip
    assert sql == ("CREATE OR REPLACE VIEW lab.sub_notebook_test.epic_realtime AS SELECT `patient_id`, `Msg_Type`, "
                   "`patient_point_of_care` FROM main.clinical.epic_realtime")  # fmt: skip


def test_every_column_is_granted_with_a_star(registry):
    sql = registry.view_sql("v", "s", "t", COLUMNS, ["*"])
    assert sql == "CREATE OR REPLACE VIEW v.s AS SELECT `patient_id`, `Msg_Type`, `patient_point_of_care`, " \
                  "`raw ``odd`` name` FROM t"  # fmt: skip


def test_a_view_with_no_granted_column_is_refused(registry):
    with pytest.raises(registry.ConfigError, match="none of"):
        registry.view_sql("v", "s", "t", COLUMNS, ["nope"])


def test_a_grant_is_stored_lowercase_and_a_star_is_every_column_of_its_sources(registry):
    columns = {"a": ["Patient_ID", "x"], "b": ["patient_id", "y"]}
    assert registry.granted(["patient_id", "Y"], ["a", "b"], columns) == ["patient_id", "y"]
    assert registry.granted(["*"], ["a", "b"], columns) == ["patient_id", "x", "y"]
    assert registry.granted(["*"], ["b"], columns) == ["patient_id", "y"]


def test_api_access_goes_to_the_service_principal_or_the_user(registry):
    sp, user = registry.can_use(SP), registry.can_use("you@example.com")
    assert (sp.service_principal_name, sp.user_name) == (SP, None)
    assert (user.user_name, user.service_principal_name) == ("you@example.com", None)
    assert sp.permission_level.value == user.permission_level.value == "CAN_USE"
