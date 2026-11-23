"""Durable guard: every config key a pipeline needs is supplied by its bundle resource.

Pipelines read settings via hl7rt.config.conf(spark, "<key>"[, default]). A key read without a
default (in the pipeline's sources or any hl7rt module they import, transitively) must be set under
`configuration:` as `hl7.<key>` in the pipeline's resources/pipelines/*.yml (or hub/resources/), so nothing
environment-specific can be hardcoded or silently missing.
"""

import ast
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text())


def _required_keys(path: Path) -> set[str]:
    keys = set()
    for node in ast.walk(_tree(path)):
        if (
            isinstance(node, ast.Call)
            and getattr(node.func, "id", getattr(node.func, "attr", None)) == "conf"
            and len(node.args) == 2
            and isinstance(node.args[1], ast.Constant)
        ):
            keys.add(node.args[1].value)
    return keys


def _hl7rt_imports(path: Path) -> set[Path]:
    """hl7rt module files imported by `path` (`from hl7rt import x` / `from hl7rt.x import y`)."""
    out = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "hl7rt":
            if node.module == "hl7rt":
                out |= {SRC / "hl7rt" / f"{a.name}.py" for a in node.names}
            else:
                out.add(SRC / Path(*node.module.split(".")).with_suffix(".py"))
    return {p for p in out if p.exists()}


def _keys_for(sources: list[Path]) -> set[str]:
    seen, todo, keys = set(), list(sources), set()
    while todo:
        p = todo.pop()
        if p in seen:
            continue
        seen.add(p)
        keys |= _required_keys(p)
        todo += list(_hl7rt_imports(p))
    return keys


# Pipeline definitions: the main bundle's resources/pipelines/, plus the subscription hub's, which hub/resources/ holds
# for both bundles (the main one includes it for edp_shared; hub/databricks.yml deploys it as a team's own hub).
HUB_DEFINITION = ROOT / "hub/resources/subscription_hub.yml"


def _pipeline_resources():
    for f in [*sorted((ROOT / "resources" / "pipelines").glob("*.yml")), HUB_DEFINITION]:
        for name, spec in yaml.safe_load(f.read_text())["resources"]["pipelines"].items():
            yield f, name, spec


def _source_dir(spec) -> str:
    """The src/pipelines/<dir> a resource runs (from its libraries glob)."""
    (lib,) = spec["libraries"]
    return lib["glob"]["include"].split("/pipelines/")[1].split("/")[0]


def test_every_pipeline_has_resource():
    used = {_source_dir(spec) for _, _, spec in _pipeline_resources()}
    sources = {p.name for p in (SRC / "pipelines").iterdir() if p.is_dir() and not p.name.startswith("_")}
    assert sources <= used, f"pipeline source dirs without a bundle resource: {sorted(sources - used)}"


def test_required_config_keys_are_wired():
    for f, name, spec in _pipeline_resources():
        configured = {k.removeprefix("hl7.") for k in spec.get("configuration", {}) if k.startswith("hl7.")}
        needed = _keys_for(sorted((SRC / "pipelines" / _source_dir(spec)).rglob("*.py")))
        missing = needed - configured
        assert not missing, f"{f.name}:{name} is missing configuration for: {sorted('hl7.' + k for k in missing)}"


# The subscription hub's settings, read in its sources by name (B2's engine); every hub pipeline sets each of them.
HUB_KEYS = {
    "hl7.subscriptions.hub", "hl7.subscriptions.tier", "hl7.subscriptions.on_failure", "hl7.subscriptions.chunk",
    "hl7.subscriptions.max_records_per_batch", "hl7.eh.namespace", "hl7.eh.service_credential",
    "hl7.registry.endpoint", "hl7.registry.database", "hl7.registry.connections", "pipelines.trigger.interval",
}  # fmt: skip


def _yaml(path: str):
    return yaml.safe_load((ROOT / path).read_text())


def _hubs():
    """Every hub pipeline: the shared definition (edp_shared's realtime hub, or a team's), and edp_shared's deferred."""
    hubs = dict(_yaml("hub/resources/subscription_hub.yml")["resources"]["pipelines"])
    hubs.update(_yaml("resources/pipelines/subscription_hub.yml")["resources"]["pipelines"])
    return hubs


def test_every_hub_sets_its_settings_and_a_topic_for_every_source():
    """The hub reads `topics.<source>` in a loop over its sources, which the literal-key check above can't see."""
    from hl7rt import subscriptions

    for name, hub in _hubs().items():
        needed = HUB_KEYS | {f"hl7.topics.{s}" for s in subscriptions.SOURCES}
        assert needed <= set(hub["configuration"]), f"{name} lacks {sorted(needed - set(hub['configuration']))}"


def test_one_hub_definition_serves_both_patterns():
    """edp_shared (main bundle) and a team's own hub (hub/) deploy the same file: one pipeline, a flow per source."""
    assert "hub/resources/*.yml" in _yaml("databricks.yml")["include"]
    assert _yaml("hub/databricks.yml")["include"] == ["resources/*.yml"]
    assert [p.name for p in (ROOT / "hub/resources").glob("*.yml")] == ["subscription_hub.yml"]
    ((name, hub),) = _yaml("hub/resources/subscription_hub.yml")["resources"]["pipelines"].items()
    conf = hub["configuration"]
    assert name == "subscription_hub" and "${var.subscription_hub}" in hub["name"]
    assert conf["hl7.subscriptions.hub"] == "${var.subscription_hub}"
    assert conf["hl7.subscriptions.tier"] == "realtime"
    assert "hl7.subscriptions.sources" not in conf, "one pipeline serves every source (S7: docs/SCALABILITY.md)"
    assert conf["pipelines.trigger.interval"] == "${var.hub_trigger_interval_seconds} seconds"
    assert hub["schema"] == "${var.hub_schema}" and conf["hl7.eh.service_credential"] == "${var.hub_service_credential}"


def test_the_deferred_hub_is_the_shared_definition_on_a_longer_trigger():
    """Deferred rules of every app (whatever its hub) run on edp_shared's deferred pipeline, so only the main bundle
    has it. Same code and settings as the shared definition; only the name, tier, and trigger differ."""
    hubs = _hubs()
    assert set(hubs) == {"subscription_hub", "subscription_hub_deferred"}
    shared, deferred = hubs["subscription_hub"], hubs["subscription_hub_deferred"]
    assert deferred["configuration"].pop("hl7.subscriptions.tier") == "deferred"
    assert shared["configuration"].pop("hl7.subscriptions.tier") == "realtime"
    assert deferred["configuration"].pop("pipelines.trigger.interval") == "${var.hub_deferred_interval_seconds} seconds"
    shared["configuration"].pop("pipelines.trigger.interval")
    assert {k: v for k, v in deferred.items() if k != "name"} == {k: v for k, v in shared.items() if k != "name"}


def _variables(bundle: str) -> dict:
    return _yaml(bundle)["variables"]


def test_both_bundles_declare_every_variable_the_hub_definition_uses():
    used = set(re.findall(r"\$\{var\.(\w+)\}", HUB_DEFINITION.read_text()))
    for bundle in ("databricks.yml", "hub/databricks.yml"):
        missing = used - set(_variables(bundle))
        assert not missing, f"{bundle} lacks variables the hub definition uses: {sorted(missing)}"


def test_a_team_hub_runs_as_its_own_service_principal_with_its_own_credential_and_schema():
    bundle = _yaml("hub/databricks.yml")
    assert bundle["run_as"] == {"service_principal_name": "${var.hub_principal}"}
    root = bundle["workspace"]["root_path"]
    assert root.startswith("/Workspace/Users/${workspace.current_user.userName}/"), (
        "code run as the hub's SP (which reads all of silver) must not be writable by every user (/Workspace/Shared)"
    )
    assert "${var.subscription_hub}" in root and "${bundle.target}" in root, "one deployment per hub"
    assert bundle["permissions"] == [{"level": "CAN_VIEW", "service_principal_name": "${var.hub_principal}"}]
    assert "../src" in bundle["sync"]["paths"], "the hub's code (its pipeline, hl7rt, the rule checks) is in src/"
    v = bundle["variables"]
    assert "default" not in v["subscription_hub"] and "default" not in v["hub_principal"], "a team names its hub"
    assert v["hub_service_credential"]["default"] == "hub_${var.subscription_hub}_eventhubs"  # infra/uc.sh
    assert v["hub_schema"]["default"] == "hub_${var.subscription_hub}"  # infra/registry.py creates and grants it
    for key in ("catalog", "eh_namespace"):
        assert "default" not in v[key], f"{key}: environment-specific, set per deployment"


def test_edp_shared_is_the_same_hub_in_the_registry_terraform_and_the_bundle():
    assert _variables("databricks.yml")["subscription_hub"]["default"] == "edp_shared"
    assert "'edp_shared'" in (ROOT / "src/registry/registry.sql").read_text(), "the registry seeds the shared hub"
    assert '"edp_shared"' in (ROOT / "infra/azure/variables.tf").read_text()
    assert re.search(r'^SHARED_HUB = "edp_shared"', (ROOT / "infra/registry.py").read_text(), re.M)


def test_make_starts_and_stops_every_hub():
    hubs = re.search(r"^HUBS\s*:=(.*)$", (ROOT / "Makefile").read_text(), re.M).group(1).split()
    assert set(hubs) == set(_hubs())
