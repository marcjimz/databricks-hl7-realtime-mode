"""Durable guard: every Event Hubs topic is named after the Delta table it lands in, `<catalog>.<schema>.<table>`.

So `real_time_mode_demo_catalog.hl7_realtime.silver_hl7` is both the hot-path topic and the streaming table
that mirrors it. Enforced across the three places names live: bundle variables, the lakehouse mirror, and the
Terraform hub list.
"""

import ast
import re
from pathlib import Path

import yaml

from hl7rt import subscriptions

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
    """lakehouse table name -> the hl7.topics.* config key it reads (`_layer("table", "topics.x", ...)`)."""
    tree = ast.parse((ROOT / "src/pipelines/lakehouse/mirror.py").read_text())
    return {
        c.args[0].value: c.args[1].value
        for c in ast.walk(tree)
        if isinstance(c, ast.Call) and getattr(c.func, "id", None) == "_layer"
    }


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
    assert 'fileset("${path.module}/../../src/subscriptions", "[a-z]*.{sql,yml}")' in main


def _rules():
    return [
        subscriptions.load(p)
        for p in sorted((ROOT / "src/subscriptions").glob("[a-z]*.*"))
        if p.suffix in (".yml", ".sql")
    ]


def test_each_subscription_rule_has_a_matching_pipeline():
    for rule in _rules():
        f = ROOT / "resources/pipelines" / f"{rule.table}.yml"
        assert f.exists(), f"{rule.name}: missing {f.relative_to(ROOT)} (make sub-new NAME={rule.name})"
        spec = yaml.safe_load(f.read_text())["resources"]["pipelines"][rule.table]
        conf = spec["configuration"]
        assert conf["hl7.sub.rule"].rsplit("/", 1)[1] in (f"{rule.name}.yml", f"{rule.name}.sql")
        assert (ROOT / "src/subscriptions" / conf["hl7.sub.rule"].rsplit("/", 1)[1]).exists()
        assert conf["hl7.topics.prefix"] == "${var.catalog}.${var.schema}"
        rtm = str(conf.get("spark.databricks.streaming.realTimeMode.enabled")).lower() == "true"
        assert rtm == (rule.mode == "rtm"), f"{rule.name}: mode {rule.mode} but the pipeline RTM flag is {rtm}"
        assert spec.get("serverless") is (rule.mode == "microbatch")
        if rule.is_join and rtm:
            assert int(conf.get("hl7.rtm.stage_tasks_multiplier", 1)) >= 3, f"{rule.name}: size for the join"
            missing = {k: v for k, v in subscriptions.RTM_JOIN_CONF.items() if conf.get(k) != v}
            assert not missing, f"{rule.name}: RTM joins need pipeline configuration {missing}"


def test_every_subscription_pipeline_has_a_rule():
    names = {r.table for r in _rules()}
    for f in (ROOT / "resources/pipelines").glob("sub_*.yml"):
        assert f.stem in names, f"{f.name} has no src/subscriptions/{f.stem.removeprefix('sub_')}.yml"
