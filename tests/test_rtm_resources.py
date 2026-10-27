"""Durable guard: Real-Time Mode pipelines follow the rules learned in Phase 0 (see AGENTS.md).

A pipeline is RTM if its configuration enables spark.databricks.streaming.realTimeMode.enabled.
"""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
RTM_KEY = "spark.databricks.streaming.realTimeMode.enabled"
# Cores per node for the node types we size against. Extend when adding a node type.
CORES = {"m6id.xlarge": 4, "m6id.2xlarge": 8, "i3.xlarge": 4, "Standard_D4ds_v5": 4, "Standard_D8ds_v5": 8}
HEADROOM = 1.2  # slots >= 1.2x tasks (AGENTS.md: ~20%+ spare; exactly-equal crashes at the epoch handoff)


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


def test_rtm_pipelines_follow_compute_rules():
    for name, spec, variables in _rtm_pipelines():
        assert spec.get("continuous") is True, f"{name}: RTM requires continuous: true"
        assert spec.get("serverless") is False, f"{name}: RTM pipelines here run on classic, fixed-size compute"
        assert spec.get("photon") is False, f"{name}: RTM requires Photon off"
        clusters = [_resolve(c, variables) for c in spec.get("clusters", [])]
        assert clusters, f"{name}: needs an explicit cluster spec"
        for c in clusters:
            assert "autoscale" not in c and isinstance(c.get("num_workers"), int), f"{name}: fixed num_workers only"
            assert c.get("aws_attributes", {}).get("availability", "ON_DEMAND") == "ON_DEMAND", f"{name}: no spot"


def test_rtm_pipelines_have_slot_headroom():
    for name, spec, variables in _rtm_pipelines():
        conf = spec["configuration"]
        partitions = int(_resolve(conf["hl7.rtm.max_partitions"], variables))
        stages = int(conf.get("hl7.rtm.stage_tasks_multiplier", 1))  # stateless single-stage hop = 1
        cluster = _resolve(spec["clusters"][0], variables)
        slots = cluster["num_workers"] * CORES[cluster["node_type_id"]]
        tasks = partitions * stages
        assert slots >= HEADROOM * tasks, f"{name}: {slots} slots for {tasks} tasks; need >= {HEADROOM}x headroom"


def test_bundle_never_uses_development_mode():
    for name, target in _bundle()["targets"].items():
        assert target.get("mode") != "development", f"target {name}: development mode disables continuous pipelines"
