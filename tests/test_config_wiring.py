"""Durable guard: every config key a pipeline needs is supplied by its bundle resource.

Pipelines read settings via hl7rt.config.conf(spark, "<key>"[, default]). A key read without a
default must be set under `configuration:` as `hl7.<key>` in the pipeline's resources/pipelines/*.yml,
so nothing environment-specific can be hardcoded or silently missing.
"""

import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
LIBRARY = ROOT / "src" / "hl7rt"


def _required_keys(path: Path) -> set[str]:
    keys = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if (
            isinstance(node, ast.Call)
            and getattr(node.func, "id", getattr(node.func, "attr", None)) == "conf"
            and len(node.args) == 2
            and isinstance(node.args[1], ast.Constant)
        ):
            keys.add(node.args[1].value)
    return keys


def _pipeline_resources():
    for f in sorted((ROOT / "resources" / "pipelines").glob("*.yml")):
        for name, spec in yaml.safe_load(f.read_text())["resources"]["pipelines"].items():
            yield f, name, spec


def test_every_pipeline_has_resource():
    names = {name for _, name, _ in _pipeline_resources()}
    sources = {p.name for p in (ROOT / "src" / "pipelines").iterdir() if p.is_dir() and not p.name.startswith("_")}
    assert sources <= names, f"pipeline source dirs without a bundle resource: {sorted(sources - names)}"


def test_required_config_keys_are_wired():
    library_keys = set().union(*(_required_keys(p) for p in LIBRARY.rglob("*.py")))
    for f, name, spec in _pipeline_resources():
        configured = {k.removeprefix("hl7.") for k in spec.get("configuration", {}) if k.startswith("hl7.")}
        needed = set().union(*(_required_keys(p) for p in (ROOT / "src" / "pipelines" / name).rglob("*.py")))
        needed |= library_keys
        missing = needed - configured
        assert not missing, f"{f.name}:{name} is missing configuration for: {sorted('hl7.' + k for k in missing)}"
