"""The subscription layer's own bundle (subscriptions/): the registry's Subscription API and edp_shared, deployed into a
workspace that already has its data, with no Terraform. It deploys the platform's own resource files (no copies), its
environments stay out of git, and the registry setup reads exactly what the bundle deploys."""

import importlib.util
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "subscriptions"
SHARED = ["resources/apps/subscription_api.yml", "hub/resources/subscription_hub.yml",
          "resources/pipelines/subscription_hub.yml"]  # fmt: skip


def _yaml(path: Path):
    return yaml.safe_load(path.read_text())


@pytest.fixture(scope="module")
def registry():
    spec = importlib.util.spec_from_file_location("registry_bootstrap", ROOT / "infra/registry.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_bundle_deploys_the_platforms_own_resource_files_and_your_targets():
    bundle = _yaml(BUNDLE / "databricks.yml")
    assert bundle["include"] == [f"../{f}" for f in SHARED] + ["targets/*.yml"]
    assert all((ROOT / f).is_file() for f in SHARED)
    assert not (BUNDLE / "resources").exists(), "no copies: the platform's files are the one definition"
    assert bundle["sync"]["paths"] == ["../src"], "the API's and the hub's code is src/"


def test_the_bundle_declares_every_variable_its_resources_use():
    used = set()
    for f in SHARED:
        used |= set(re.findall(r"\$\{var\.(\w+)\}", (ROOT / f).read_text()))
    declared = _yaml(BUNDLE / "databricks.yml")["variables"]
    assert used <= set(declared), f"undeclared: {sorted(used - set(declared))}"


def test_the_bundle_has_no_environment_of_its_own():
    bundle = _yaml(BUNDLE / "databricks.yml")
    assert "targets" not in bundle, "environments are subscriptions/targets/<env>.yml (gitignored)"
    example = _yaml(BUNDLE / "target.example.yml")["targets"].values()
    assert all("mode" not in t for t in example), "development mode turns off continuous"
    assert bundle["workspace"]["root_path"].startswith("/Workspace/Users/${workspace.current_user.userName}/")
    v = bundle["variables"]
    for key in ("catalog", "eh_namespace", "eh_service_credential"):
        assert "default" not in v[key], f"{key}: environment-specific, set per target"
    assert v["subscription_hub"]["default"] == "edp_shared"
    assert v["registry_catalog"]["default"] == "", "empty: don't register the registry in Unity Catalog"
    assert v["warehouse_id"]["lookup"] == {"warehouse": "${var.warehouse}"}
    for key in ("subscriber_apps", "subscriber_templates", "subscriber_hubs"):
        assert v[key]["type"] == "complex" and v[key]["default"] == {}


# Settings the platform documents in the root databricks.yml: the same defaults here, so both deploy the same hub.
SAME_DEFAULTS = [
    "registry_project", "registry_endpoint", "registry_database", "subscription_api_app", "app_min_instances",
    "app_max_instances", "subscription_hub", "hub_rules_per_projection", "hub_max_records_per_batch",
    "hub_trigger_interval_seconds", "hub_deferred_interval_seconds", "hub_registry_connections", "hub_on_failure",
]  # fmt: skip


def test_the_hub_and_api_settings_default_as_on_the_platform():
    ours, platform = _yaml(BUNDLE / "databricks.yml")["variables"], _yaml(ROOT / "databricks.yml")["variables"]
    assert {k: ours[k]["default"] for k in SAME_DEFAULTS} == {k: platform[k]["default"] for k in SAME_DEFAULTS}
    team = _yaml(ROOT / "hub/databricks.yml")["variables"]
    topics = [k for k in team if k.endswith("_topic")]
    assert {k: ours[k]["default"] for k in topics} == {k: team[k]["default"] for k in topics}, "named as the platform"


def test_environments_stay_out_of_git_and_the_example_doesnt():
    ignored = subprocess.run(["git", "check-ignore", "-q", "subscriptions/targets/x.yml"], cwd=ROOT)
    assert ignored.returncode == 0, "subscriptions/targets/ holds your environments: .gitignore it"
    example = subprocess.run(["git", "check-ignore", "-q", "subscriptions/target.example.yml"], cwd=ROOT)
    assert example.returncode == 1, "the example is committed"


EXAMPLE = _yaml(BUNDLE / "target.example.yml")


def test_the_example_sets_everything_a_deployer_sets():
    ((name, target),) = EXAMPLE["targets"].items()
    declared = _yaml(BUNDLE / "databricks.yml")["variables"]
    required = {k for k, v in declared.items() if "default" not in v and "lookup" not in v}
    deployer = {
        "catalog", "hub_schema", "subscription_tables", "subscription_table_partition", "subscription_table_offset",
        "subscription_table_ts", "subscription_table_max_files", "eh_namespace", "eh_service_credential",
        "registry_project", "subscription_api_app", "warehouse", "app_min_instances", "app_max_instances",
        "subscriber_apps",
    }  # fmt: skip
    assert required | deployer <= set(target["variables"])
    assert set(target["variables"]) <= set(declared)
    assert re.fullmatch(r"https://adb-<workspace-id>\.<n>\.azuredatabricks\.net", target["workspace"]["host"])
    (app,) = target["variables"]["subscriber_apps"].values()
    assert {"principal", "topic", "eventhubs_credential", "sources", "fields", "templates", "subscription_hub"} <= set(
        app
    )


def test_the_example_holds_placeholders_only():
    text = (BUNDLE / "target.example.yml").read_text()
    assert not re.search(r"adb-\d+", text) and "@databricks.com" not in text
    assert re.search(r"principal: you@example\.com", text)


def _example_bundle(tmp_path: Path, host: str | None = None) -> Path:
    """A copy of subscriptions/ with the example as its one target (its host replaced when given), beside links to
    the platform's files, so the CLI and the loader read the same tree."""
    for d in ("resources", "hub", "src"):
        (tmp_path / d).symlink_to(ROOT / d)
    bundle = tmp_path / "subscriptions"
    (bundle / "targets").mkdir(parents=True)
    shutil.copy(BUNDLE / "databricks.yml", bundle)
    text = (BUNDLE / "target.example.yml").read_text()
    if host:
        text = re.sub(r"host: \S+", f"host: {host}", text)
    (bundle / "targets/test.yml").write_text(text)
    return bundle


def test_the_example_resolves(registry, tmp_path):
    config = registry.Config.from_target("test", _example_bundle(tmp_path))
    assert config.target == "test" and config.catalog == "<catalog>" and config.hub_schema == "hl7_subscriptions"
    assert config.tables == {"epic_realtime": "<catalog>.<schema>.epic_realtime"}
    assert config.api_app == "hl7-subscriptions-test" and config.warehouse == "<SQL warehouse name>"
    assert config.endpoint == "projects/hl7-subscriptions/branches/production/endpoints/primary"
    assert config.hub_credential == "<UC service credential>", "edp_shared writes the apps' topics with it"
    assert config.registry_catalog == "" and config.hubs == {}
    assert config.apps["notebook-test"]["principal"] == "you@example.com"
    assert config.apps["notebook-test"]["sources"] == ["epic_realtime"]


def test_the_loader_resolves_the_example_as_the_databricks_cli_does(registry, tmp_path):
    """Only with the CLI and a workspace to validate against (DATABRICKS_HOST, e.g. from the Makefile); else skipped.
    The warehouse lookup is given (--var), so validate makes no lookup and resolves every other variable."""
    host = os.environ.get("DATABRICKS_HOST")
    if not (shutil.which("databricks") and host):
        pytest.skip("needs the Databricks CLI and DATABRICKS_HOST")
    bundle = _example_bundle(tmp_path, host)
    env = {k: v for k, v in os.environ.items() if not k.startswith("BUNDLE_VAR_") and k != "DATABRICKS_CONFIG_PROFILE"}
    out = subprocess.run(["databricks", "bundle", "validate", "-t", "test", "-o", "json", "--var", "warehouse_id=x"],
                         cwd=bundle, env=env, capture_output=True, text=True, timeout=300)  # fmt: skip
    if out.returncode != 0:
        if re.search(r"auth|profile|login|token|credential", out.stderr, re.I):
            pytest.skip(f"no workspace auth: {out.stderr.strip().splitlines()[0]}")
        pytest.fail(out.stderr)
    cli = {k: v["value"] for k, v in json.loads(out.stdout)["variables"].items() if k != "warehouse_id"}
    ours, _ = registry.read_target("test", bundle)
    assert {k: ours[k] for k in cli} == cli


def test_make_runs_the_subscription_layer_with_only_its_target():
    """The Makefile exports dev's host and BUNDLE_VAR_catalog from Terraform; either would silently win over the target
    file (BUNDLE_VAR_catalog over its catalog), so the subscriptions recipes drop them first."""
    make = (ROOT / "Makefile").read_text()
    for goal in ("subscriptions", "subscriptions-stop"):
        recipe = re.search(rf"^{goal}:.*\n((?:\t.*\n)+)", make, re.M).group(1)
        assert "$(SUBS_ENV)" in recipe, f"{goal} must run with $(SUBS_ENV)"
    unset = re.search(r"^SUBS_ENV\s*=(.*)$", make, re.M).group(1)
    assert "DATABRICKS_HOST" in unset and "DATABRICKS_AUTH_TYPE" in unset and "BUNDLE_VAR_" in unset
