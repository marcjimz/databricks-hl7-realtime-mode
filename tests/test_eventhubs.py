"""Specs for Event Hubs Kafka options: everything comes from config + secret scope."""

from hl7rt import eventhubs


def test_bootstrap_servers_from_namespace():
    assert eventhubs.bootstrap_servers("ehns-demo") == "ehns-demo.servicebus.windows.net:9093"


def test_jaas_config_uses_shaded_plain_login_and_connection_string():
    jaas = eventhubs.jaas_config("Endpoint=sb://x/;SharedAccessKeyName=n;SharedAccessKey=k")
    assert jaas.startswith("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required ")
    assert 'username="$ConnectionString"' in jaas
    assert 'password="Endpoint=sb://x/;SharedAccessKeyName=n;SharedAccessKey=k";' in jaas


def test_kafka_options_shape():
    opts = eventhubs.kafka_options("ehns-demo", "conn")
    assert opts["kafka.bootstrap.servers"] == "ehns-demo.servicebus.windows.net:9093"
    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "PLAIN"
    assert "conn" in opts["kafka.sasl.jaas.config"]


def test_kafka_options_extra_overrides():
    opts = eventhubs.kafka_options("ehns-demo", "conn", {"kafka.request.timeout.ms": "1000"})
    assert opts["kafka.request.timeout.ms"] == "1000"
