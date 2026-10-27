"""Durable guard: the dashboard stays portable and only reads tables the pipelines produce."""

import ast
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DASH = ROOT / "src/dashboards/hl7_latency.lvdash.json"


def _datasets():
    return {d["name"]: "".join(d["queryLines"]) for d in json.loads(DASH.read_text())["datasets"]}


def _produced_tables() -> set[str]:
    """Tables written by the lakehouse mirror (constant @dp.table names) and by subscription rules."""
    tree = ast.parse((ROOT / "src/pipelines/lakehouse/mirror.py").read_text())
    names = {n.args[0].value for n in ast.walk(tree) if isinstance(n, ast.Call) and n.args
             and getattr(n.func, "id", getattr(n.func, "attr", "")) in ("_layer", "create_streaming_table")
             and isinstance(n.args[0], ast.Constant)}  # fmt: skip
    rules = {f"sub_{p.stem}" for p in (ROOT / "src/subscriptions").glob("[a-z]*.yml")}
    return names | rules


def test_dashboard_uses_unqualified_tables_and_bundle_sets_catalog_schema():
    for name, sql in _datasets().items():
        assert "real_time_mode_demo_catalog" not in sql and "hl7_realtime." not in sql, f"{name}: hardcoded catalog"
    spec = yaml.safe_load((ROOT / "resources/dashboards/hl7_latency.yml").read_text())["resources"]["dashboards"]
    d = spec["hl7_latency"]
    assert d["dataset_catalog"] == "${var.catalog}" and d["warehouse_id"] == "${var.warehouse_id}"


def test_dashboard_reads_only_produced_tables():
    produced = _produced_tables()
    for name, sql in _datasets().items():
        tables = set(re.findall(r"\bFROM\s+([a-z_][a-z0-9_]*)\b", sql, re.I)) - {"select"}
        assert tables <= produced, f"{name} reads unknown tables {tables - produced}"


def test_widget_fields_exist_in_their_dataset():
    dash = json.loads(DASH.read_text())
    sqls = _datasets()
    for page in dash["pages"]:
        for item in page["layout"]:
            for q in item["widget"].get("queries", []):
                sql = sqls[q["query"]["datasetName"]]
                for f in q["query"]["fields"]:
                    col = f["expression"].strip("`")
                    assert re.search(rf"\b{col}\b", sql), f"{item['widget']['name']}: {col} not in dataset"
