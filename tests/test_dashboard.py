"""Durable guard: the dashboards stay portable and only read tables or views something in this repo produces."""

import ast
import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = {
    "hl7_latency": ("${var.catalog}", "${resources.schemas.hl7.name}"),
    "hl7_subscriptions": ("${var.registry_catalog}", "subscription_monitor"),
}


def _dash(name):
    return json.loads((ROOT / f"src/dashboards/{name}.lvdash.json").read_text())


def _datasets(name):
    return {d["name"]: "".join(d["queryLines"]) for d in _dash(name)["datasets"]}


def _produced_tables() -> set[str]:
    """Tables written by the lakehouse mirror (constant table names)."""
    tree = ast.parse((ROOT / "src/pipelines/lakehouse/mirror.py").read_text())
    names = {n.args[0].value for n in ast.walk(tree) if isinstance(n, ast.Call) and n.args
             and getattr(n.func, "id", getattr(n.func, "attr", "")) in ("_layer", "create_streaming_table")
             and isinstance(n.args[0], ast.Constant)}  # fmt: skip
    return names


def _monitor_views() -> set[str]:
    """Views the registry defines for platform monitoring (src/registry/registry.sql)."""
    return set(re.findall(r"CREATE VIEW subscription_monitor\.(\w+)", (ROOT / "src/registry/registry.sql").read_text()))


@pytest.mark.parametrize("name", DASHBOARDS)
def test_dashboard_uses_unqualified_tables_and_bundle_sets_catalog_schema(name):
    for ds, sql in _datasets(name).items():
        assert "hl7_rtm_demo" not in sql and "hl7_realtime." not in sql, f"{ds}: hardcoded catalog"
        assert "subscription_monitor." not in sql and "subscription_registry" not in sql, f"{ds}: qualified registry"
    spec = yaml.safe_load((ROOT / f"resources/dashboards/{name}.yml").read_text())["resources"]["dashboards"][name]
    assert (spec["dataset_catalog"], spec["dataset_schema"]) == DASHBOARDS[name]
    assert spec["warehouse_id"] == "${var.warehouse_id}"


@pytest.mark.parametrize(
    "name, produced", [("hl7_latency", _produced_tables()), ("hl7_subscriptions", _monitor_views())]
)
def test_dashboard_reads_only_produced_tables(name, produced):
    assert produced
    for ds, sql in _datasets(name).items():
        tables = set(re.findall(r"\bFROM\s+([a-z_][a-z0-9_]*)\b", sql, re.I)) - {"select"}
        assert tables <= produced, f"{name}.{ds} reads unknown tables {tables - produced}"


@pytest.mark.parametrize("name", DASHBOARDS)
def test_widget_fields_exist_in_their_dataset(name):
    sqls = _datasets(name)
    for page in _dash(name)["pages"]:
        for item in page["layout"]:
            for q in item["widget"].get("queries", []):
                sql = sqls[q["query"]["datasetName"]]
                for f in q["query"]["fields"]:
                    col = f["expression"].strip("`")
                    assert re.search(rf"\b{col}\b", sql), f"{item['widget']['name']}: {col} not in dataset"


def test_the_registry_catalog_is_the_one_make_registry_registers():
    variables = yaml.safe_load((ROOT / "databricks.yml").read_text())["variables"]
    assert variables["registry_catalog"]["default"] == "${var.catalog}_registry"
    assert "_registry" in (ROOT / "infra/registry.py").read_text()
