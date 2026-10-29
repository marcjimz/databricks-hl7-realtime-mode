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
    assert 'fileset("${path.module}/../../src/subscriptions", "[a-z]*.yml")' in main


def _rules():
    return [subscriptions.load(p) for p in sorted((ROOT / "src/subscriptions").glob("[a-z]*.yml"))]


def test_each_subscription_rule_has_a_matching_resource():
    """rtm + `then` (a stream-stream join) runs as a Structured Streaming job, since Lakeflow pipelines don't support
    RTM stream-stream joins; every other rule is a pipeline."""
    for rule in _rules():
        if rule.mode == "rtm" and rule.is_join:
            f = ROOT / "resources/jobs" / f"{rule.table}.yml"
            assert f.exists(), f"{rule.name}: missing {f.relative_to(ROOT)} (make sub-new NAME={rule.name} JOIN=1)"
            assert not (ROOT / "resources/pipelines" / f"{rule.table}.yml").exists(), (
                f"{rule.name}: RTM join can't be a pipeline"
            )
            job = yaml.safe_load(f.read_text())["resources"]["jobs"][rule.table]
            (task,) = job["tasks"]
            params = task["python_wheel_task"]["parameters"]
            assert task["python_wheel_task"]["entry_point"] == "hl7-subscription"
            assert f"--rule=${{workspace.file_path}}/src/subscriptions/{rule.name}.yml" in params
            assert any(p.startswith("--checkpoint=") and p.endswith(f"/{rule.table}") for p in params)
            assert "--conf=topics.prefix=${var.catalog}.${var.schema}" in params
            assert job["job_clusters"][0]["new_cluster"] == "${var.rtm_job_cluster}"
            continue
        f = ROOT / "resources/pipelines" / f"{rule.table}.yml"
        assert f.exists(), f"{rule.name}: missing {f.relative_to(ROOT)} (make sub-new NAME={rule.name})"
        spec = yaml.safe_load(f.read_text())["resources"]["pipelines"][rule.table]
        conf = spec["configuration"]
        assert conf["hl7.sub.rule"].endswith(f"/src/subscriptions/{rule.name}.yml")
        assert conf["hl7.topics.prefix"] == "${var.catalog}.${var.schema}"
        rtm = str(conf.get("spark.databricks.streaming.realTimeMode.enabled")).lower() == "true"
        assert rtm == (rule.mode == "rtm"), f"{rule.name}: mode {rule.mode} but the pipeline RTM flag is {rtm}"
        assert spec.get("serverless") is (rule.mode == "microbatch")


def test_rtm_job_cluster_follows_rtm_rules_with_headroom():
    variables = yaml.safe_load((ROOT / "databricks.yml").read_text())["variables"]
    c = variables["rtm_job_cluster"]["default"]
    assert c["spark_conf"]["spark.databricks.streaming.realTimeMode.enabled"] == "true"
    assert c["runtime_engine"] == "STANDARD" and "autoscale" not in c and isinstance(c["num_workers"], int)
    assert c["aws_attributes"]["availability"] == "ON_DEMAND"
    assert c["spark_version"].startswith(("18.", "19.")), "RTM stream-stream joins with other streams need DBR 18 LTS+"
    partitions = int(variables["rtm_max_partitions"]["default"])
    tasks = 2 * partitions + 4  # two Kafka sources + 4 join (shuffle) tasks
    from test_rtm_resources import CORES, HEADROOM

    assert c["num_workers"] * CORES[c["node_type_id"]] >= HEADROOM * tasks


def test_subscription_job_parameters_supply_every_config_key():
    from test_config_wiring import _keys_for

    needed = _keys_for([ROOT / "src/hl7rt/subscription_job.py"])
    for f in (ROOT / "resources/jobs").glob("sub_*.yml"):
        (job,) = yaml.safe_load(f.read_text())["resources"]["jobs"].values()
        params = job["tasks"][0]["python_wheel_task"]["parameters"]
        given = {p.removeprefix("--conf=").split("=", 1)[0] for p in params if p.startswith("--conf=")}
        assert needed <= given, f"{f.name} is missing --conf for {sorted(needed - given)}"


def test_every_subscription_resource_has_a_rule():
    names = {r.table for r in _rules()}
    for f in [*(ROOT / "resources/pipelines").glob("sub_*.yml"), *(ROOT / "resources/jobs").glob("sub_*.yml")]:
        assert f.stem in names, f"{f.name} has no src/subscriptions/{f.stem.removeprefix('sub_')}.yml"
