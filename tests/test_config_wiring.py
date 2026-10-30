"""Durable guard: every config key a pipeline needs is supplied by its bundle resource.

Pipelines read settings via hl7rt.config.conf(spark, "<key>"[, default]). A key read without a
default (in the pipeline's sources or any hl7rt module they import, transitively) must be set under
`configuration:` as `hl7.<key>` in the pipeline's resources/pipelines/*.yml, so nothing
environment-specific can be hardcoded or silently missing.
"""

import ast
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


def _pipeline_resources():
    for f in sorted((ROOT / "resources" / "pipelines").glob("*.yml")):
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
