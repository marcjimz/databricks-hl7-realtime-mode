"""Durable guard: Real-Time Mode pipelines follow the GA rules (see AGENTS.md).

A pipeline is RTM if its configuration enables spark.databricks.streaming.realTimeMode.enabled.
"""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
RTM_KEY = "spark.databricks.streaming.realTimeMode.enabled"


def _bundle():
    return yaml.safe_load((ROOT / "databricks.yml").read_text())


def _resolve(value, variables):
    """Resolve a single ${var.x} reference to the variable's default."""
    if isinstance(value, str) and (m := re.fullmatch(r"\$\{var\.(\w+)\}", value)):
        return variables[m.group(1)]["default"]
    return value


def _rtm_pipelines():
    variables = _bundle()["variables"]
    for f in sorted((ROOT / "resources" / "pipelines").glob("*.yml")):
        for name, spec in yaml.safe_load(f.read_text())["resources"]["pipelines"].items():
            if str(spec.get("configuration", {}).get(RTM_KEY)).lower() == "true":
                yield name, spec, variables


def test_there_are_rtm_pipelines():
    assert list(_rtm_pipelines()), "expected at least one RTM pipeline"


def test_rtm_pipelines_are_serverless_ga():
    """GA Real-Time Mode on serverless: continuous, Databricks sizes compute (no cluster spec), on the current (GA)
    channel, where each source's reader task count is fixed, so maxPartitions must be set."""
    for name, spec, variables in _rtm_pipelines():
        assert spec.get("continuous") is True, f"{name}: RTM requires continuous: true"
        assert spec.get("serverless") is True, f"{name}: RTM pipelines run on serverless"
        assert not spec.get("clusters"), f"{name}: serverless pipelines take no cluster spec"
        assert spec.get("channel", "CURRENT") == "CURRENT", f"{name}: GA channel only (multi-flow RTM is preview/Beta)"
        assert int(_resolve(spec["configuration"]["hl7.rtm.max_partitions"], variables)) >= 1


def test_bundle_never_uses_development_mode():
    for name, target in _bundle()["targets"].items():
        assert target.get("mode") != "development", f"target {name}: development mode disables continuous pipelines"
