"""Parked from tests/test_topic_names.py."""

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
