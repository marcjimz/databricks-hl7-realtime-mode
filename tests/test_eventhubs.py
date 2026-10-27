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


# Azure closes idle connections after 240 s; clients must refresh metadata and recycle connections before that.
# https://learn.microsoft.com/azure/event-hubs/apache-kafka-configurations
AZURE_IDLE_MS = 240_000


def test_java_client_follows_event_hubs_guidance():
    opts = eventhubs.kafka_options("ehns-demo", "conn")
    assert int(opts["kafka.metadata.max.age.ms"]) < AZURE_IDLE_MS
    assert int(opts["kafka.connections.max.idle.ms"]) < AZURE_IDLE_MS
    assert int(opts["kafka.request.timeout.ms"]) >= 60_000
    assert int(opts["kafka.max.request.size"]) < 1_046_528


def test_librdkafka_producer_follows_event_hubs_guidance():
    cfg = eventhubs.producer_config("ehns-demo", "conn")
    assert cfg["socket.keepalive.enable"] is True
    assert int(cfg["metadata.max.age.ms"]) < AZURE_IDLE_MS
    assert int(cfg["request.timeout.ms"]) >= 60_000
