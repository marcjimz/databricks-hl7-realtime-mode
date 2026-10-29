"""Specs for the Structured Streaming RTM subscription job (rules Lakeflow pipelines can't run in RTM: joins)."""

import pytest

from hl7rt import subscription_job


def test_args_become_hl7_spark_conf():
    args = subscription_job.parse_args(
        ["--rule=/w/src/subscriptions/x.yml", "--checkpoint=/Volumes/c/s/checkpoints/sub_x",
         "--conf=eh.namespace=ns", "--conf=topics.prefix=c.s", "--conf=rtm.trigger_interval=5 minutes"]
    )  # fmt: skip
    assert args.rule == "/w/src/subscriptions/x.yml"
    assert args.checkpoint == "/Volumes/c/s/checkpoints/sub_x"
    assert subscription_job.hl7_conf(args) == {
        "hl7.eh.namespace": "ns", "hl7.topics.prefix": "c.s", "hl7.rtm.trigger_interval": "5 minutes",
    }  # fmt: skip


def test_bad_conf_arg_fails_clearly():
    args = subscription_job.parse_args(["--rule=r", "--checkpoint=c", "--conf=novalue"])
    with pytest.raises(ValueError, match="key=value"):
        subscription_job.hl7_conf(args)


def test_job_only_runs_rtm_join_rules():
    from hl7rt import subscriptions

    single = subscriptions.parse({"name": "x", "mode": "rtm", "key": "patient_id", "when": {"source": "gold_orders"},
                                  "output": ["patient_id"], "destinations": ["eventhubs"]})  # fmt: skip
    with pytest.raises(ValueError, match="pipeline"):
        subscription_job.check(single)
