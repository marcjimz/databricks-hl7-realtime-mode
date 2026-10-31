"""Specs for the shared RTM Kafka wiring: hung Event Hubs requests are abandoned quickly on the hot path."""

from types import SimpleNamespace

from hl7rt import eventhubs, rtm


def _spark(**conf):
    base = {"hl7.eh.namespace": "ehns", "hl7.eh.service_credential": "cred", **conf}
    return SimpleNamespace(conf=SimpleNamespace(get=lambda k, d=None: base.get(k, d)))


def test_sink_bounds_hung_produce_requests():
    opts = rtm.sink_options(_spark(), "t")
    # Azure: a producer connection can stop answering until request.timeout.ms (60 s by default) expires.
    request = int(opts["kafka.request.timeout.ms"])
    assert 2_000 <= request <= 10_000
    assert int(opts["kafka.delivery.timeout.ms"]) >= int(opts["kafka.linger.ms"]) + request
    assert opts["databricks.serviceCredential"] == "cred" and opts["topic"] == "t"


def test_hang_guard_is_hot_path_only():
    """The mirror and Epic JSON (micro-batch) keep Microsoft's request timeout guidance."""
    assert int(eventhubs.kafka_options("ehns", "cred")["kafka.request.timeout.ms"]) >= 60_000
    assert int(rtm.HUNG_REQUEST_GUARD["kafka.request.timeout.ms"]) < 60_000
