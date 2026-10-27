"""Durable guard: every Event Hubs topic is named after the Delta table it lands in, `<catalog>.<schema>.<table>`.

So `real_time_mode_demo_catalog.hl7_realtime.silver_hl7` is both the hot-path topic and the streaming table
that mirrors it. Enforced across the three places names live: bundle variables, the lakehouse mirror, and the
Terraform hub list.
"""

import ast
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TOPIC = re.compile(r"\$\{var\.catalog\}\.\$\{var\.schema\}\.(\w+)")


def _variables():
    return yaml.safe_load((ROOT / "databricks.yml").read_text())["variables"]


def _topic_tables() -> dict[str, str]:
    """bundle topic variable -> table name from its default."""
    return {n: TOPIC.fullmatch(v["default"]).group(1) for n, v in _variables().items() if n.endswith("_topic")}


def test_topic_variables_are_catalog_schema_table():
    for name, var in _variables().items():
        if name.endswith("_topic"):
            assert TOPIC.fullmatch(str(var.get("default"))), (
                f"{name}: default must be ${{var.catalog}}.${{var.schema}}.<table>, got {var.get('default')!r}"
            )


def _mirror_tables() -> dict[str, str]:
    """lakehouse table name -> the hl7.topics.* config key it reads (`_topic("topics.x")`)."""
    out = {}
    for fn in ast.walk(ast.parse((ROOT / "src/pipelines/lakehouse/mirror.py").read_text())):
        if not isinstance(fn, ast.FunctionDef):
            continue
        names = [k.value.value for d in fn.decorator_list if isinstance(d, ast.Call) for k in d.keywords
                 if k.arg == "name"]  # fmt: skip
        keys = [c.args[0].value for c in ast.walk(fn)
                if isinstance(c, ast.Call) and getattr(c.func, "id", None) == "_topic"]  # fmt: skip
        if names and keys:
            out[names[0]] = keys[0]
    return out


def test_each_mirror_table_reads_the_topic_of_the_same_name():
    conf = yaml.safe_load((ROOT / "resources/pipelines/lakehouse.yml").read_text())
    conf = conf["resources"]["pipelines"]["lakehouse"]["configuration"]
    tables = _topic_tables()
    mirror = _mirror_tables()
    assert mirror, "no mirror tables found"
    for table, key in mirror.items():
        var = re.fullmatch(r"\$\{var\.(\w+)\}", conf[f"hl7.{key}"]).group(1)
        assert tables[var] == table, f"table {table} reads {var} -> topic table {tables[var]!r}"


def test_epic_json_topic_is_named_after_its_bronze_table():
    src = (ROOT / "src/pipelines/epic_json/bronze.py").read_text()
    assert f'"{_topic_tables()["epic_json_topic"]}"' in src


def test_terraform_creates_one_hub_per_table_with_the_same_pattern():
    tf = (ROOT / "infra/eventhubs/variables.tf").read_text()
    block = re.search(r'variable "tables" \{.*?default\s*=\s*\[(.*?)\]', tf, re.S).group(1)
    hubs = set(re.findall(r'"(\w+)"', block))
    assert set(_topic_tables().values()) <= hubs, f"missing hubs: {set(_topic_tables().values()) - hubs}"
    main = (ROOT / "infra/eventhubs/main.tf").read_text()
    assert '"${var.catalog}.${var.schema}.${each.value}"' in main
