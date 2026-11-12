"""Durable guard: the live Power BI dashboard (src/dashboards/powerbi) reads only its own views, which read only what
this repo produces; the model and report agree with each other and with the views; and nothing environment-specific or
secret is in the definitions (make powerbi fills them in)."""

import base64
import importlib.util
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "src/dashboards/powerbi"
VIEWS_SQL = (DIR / "views.sql").read_text()

_spec = importlib.util.spec_from_file_location("powerbi_definition", DIR / "definition.py")
definition = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(definition)

from test_dashboard import _monitor_views, _produced_tables  # noqa: E402


def _views() -> dict[str, str]:
    """view name -> its CREATE statement."""
    statements = [s for s in VIEWS_SQL.split(";\n\n") if "CREATE OR REPLACE VIEW" in s]
    return {re.search(r"VIEW \$\{catalog\}\.powerbi\.(\w+)", s).group(1): s for s in statements}


def _decoded(parts) -> dict[str, str]:
    return {p["path"]: base64.b64decode(p["payload"]).decode() for p in parts}


def test_views_read_only_produced_tables_and_monitor_views():
    for view, sql in _views().items():
        lakehouse = set(re.findall(r"\$\{catalog\}\.\$\{schema\}\.(\w+)", sql))
        monitor = set(re.findall(r"\$\{registry_catalog\}\.subscription_monitor\.(\w+)", sql))
        assert lakehouse or monitor, f"{view} reads nothing"
        assert lakehouse <= _produced_tables(), f"{view} reads unknown tables {lakehouse - _produced_tables()}"
        assert monitor <= _monitor_views(), f"{view} reads unknown monitor views {monitor - _monitor_views()}"
        qualified = re.findall(r"\bFROM\s+([\w.${}]+)", sql)
        assert all(q.startswith("${") for q in qualified), f"{view}: unqualified source {qualified}"


def test_views_sql_hardcodes_no_catalog():
    assert "hl7_rtm_demo" not in VIEWS_SQL and "hl7_realtime" not in VIEWS_SQL


def test_every_model_table_is_a_view_with_those_columns():
    views = _views()
    assert set(definition.TABLES) == set(views)
    for view, columns in definition.TABLES.items():
        for column in columns:
            assert re.search(rf"\b{column}\b", views[view]), f"{view}.{column} isn't in views.sql"


def test_measures_read_their_own_tables_columns_and_names_are_unique():
    seen = set()
    for table, measures in definition.MEASURES.items():
        assert table in definition.TABLES
        for name, (dax, _) in measures.items():
            for t, c in re.findall(r"(\w+)\[(\w+)\]", dax):
                assert t == table and c in definition.TABLES[t], f"{name}: {t}[{c}]"
            # DAX names are case-insensitive: a measure may not share a name with another measure or its table's columns
            assert name.lower() not in seen and name.lower() not in definition.TABLES[table], name
            seen.add(name.lower())
    table, measure = definition.CHANGE_DETECTION
    assert measure in definition.MEASURES[table]


def test_every_visual_field_is_a_model_column_or_measure():
    for page, visuals in definition.PAGES.items():
        for _, title, _, roles in visuals:
            for fields in roles.values():
                for table, field in fields:
                    assert field in definition.TABLES[table] or field in definition.MEASURES.get(table, {}), (
                        f"{page} / {title}: {table}.{field}"
                    )


def test_visuals_fit_the_page():
    for page, visuals in definition.PAGES.items():
        for _, title, (x, y, w, h), _ in visuals:
            assert x >= 0 and y >= 0 and x + w <= 1280 and y + h <= 720, f"{page} / {title}"


def test_rendering_is_deterministic_and_takes_its_environment_as_arguments():
    model = _decoded(definition.model_parts("host.example", "/sql/1.0/warehouses/abc", "cat_x"))
    assert model == _decoded(definition.model_parts("host.example", "/sql/1.0/warehouses/abc", "cat_x"))
    assert {f"definition/tables/{v}.tmdl" for v in definition.TABLES} <= set(model)
    expressions = model["definition/expressions.tmdl"]
    assert '"host.example"' in expressions and '"/sql/1.0/warehouses/abc"' in expressions and '"cat_x"' in expressions
    for v in definition.TABLES:
        tmdl = model[f"definition/tables/{v}.tmdl"]
        assert "mode: directQuery" in tmdl and f'Name = "{v}", Kind = "View"' in tmdl
        assert "cat_x" not in tmdl and "host.example" not in tmdl, "tables read the parameters, not literals"

    report = _decoded(definition.report_parts("model-123"))
    assert report == _decoded(definition.report_parts("model-123"))
    assert "semanticmodelid=model-123" in report["definition.pbir"]
    pages = [p for p in report if p.endswith("/page.json")]
    assert len(pages) == len(definition.PAGES)
    for p in pages:
        refresh = json.loads(report[p])["objects"]["pageRefresh"][0]["properties"]
        assert refresh["refreshType"]["expr"]["Literal"]["Value"] == "'ChangeDetection'"
    visuals = [p for p in report if p.endswith("/visual.json")]
    assert len(visuals) == sum(len(v) for v in definition.PAGES.values())


@pytest.mark.parametrize("path", sorted(DIR.iterdir()), ids=lambda p: p.name)
def test_no_environment_ids_or_secrets_in_the_definitions(path):
    if path.is_dir():
        return
    text = path.read_text()
    assert not re.search(r"azuredatabricks\.net|/sql/1\.0/warehouses/\w|semanticmodelid=\w", text), path.name
    assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text), "no tenant/IDs"
    assert not re.search(r"client_secret|dose[a-z0-9]{20,}|password\s*=", text, re.I), "no secrets"
