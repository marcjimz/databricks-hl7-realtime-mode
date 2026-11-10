"""Specs for Event Hubs Kafka options: Entra ID via a Unity Catalog service credential, never a connection string."""

from types import SimpleNamespace

from hl7rt import eventhubs


def test_bootstrap_servers_from_namespace():
    assert eventhubs.bootstrap_servers("ehns-demo") == "ehns-demo.servicebus.windows.net:9093"


def test_token_scope_is_the_namespace():
    assert eventhubs.token_scope("ehns-demo") == "https://ehns-demo.servicebus.windows.net/.default"


def test_kafka_options_use_the_service_credential_only():
    opts = eventhubs.kafka_options("ehns-demo", "hl7_eventhubs")
    assert opts["kafka.bootstrap.servers"] == "ehns-demo.servicebus.windows.net:9093"
    assert opts["databricks.serviceCredential"] == "hl7_eventhubs"
    # Databricks sets up SASL/OAUTHBEARER itself; setting any of these disables the service credential.
    assert not [k for k in opts if k.startswith(("kafka.sasl.", "kafka.security."))]


def test_kafka_options_extra_overrides():
    opts = eventhubs.kafka_options("ehns-demo", "cred", {"kafka.request.timeout.ms": "1000"})
    assert opts["kafka.request.timeout.ms"] == "1000"


# Azure closes idle connections after 240 s; clients must refresh metadata and recycle connections before that.
# https://learn.microsoft.com/azure/event-hubs/apache-kafka-configurations
AZURE_IDLE_MS = 240_000


def test_java_client_follows_event_hubs_guidance():
    opts = eventhubs.kafka_options("ehns-demo", "cred")
    assert int(opts["kafka.metadata.max.age.ms"]) < AZURE_IDLE_MS
    assert int(opts["kafka.connections.max.idle.ms"]) < AZURE_IDLE_MS
    assert int(opts["kafka.request.timeout.ms"]) >= 60_000
    assert int(opts["kafka.max.request.size"]) < 1_046_528


class _Credential:
    """Stands in for dbutils.credentials.getServiceCredentialsProvider(...): an azure-identity TokenCredential."""

    def __init__(self):
        self.scopes = []

    def get_token(self, scope):
        self.scopes.append(scope)
        return SimpleNamespace(token="tok", expires_on=1_900_000_000)


def test_producer_authenticates_with_entra_tokens_from_the_credential():
    cred = _Credential()
    cfg = eventhubs.producer_config("ehns-demo", cred)
    assert cfg["security.protocol"] == "SASL_SSL" and cfg["sasl.mechanism"] == "OAUTHBEARER"
    assert cfg["oauth_cb"](None) == ("tok", 1_900_000_000.0)
    assert cred.scopes == ["https://ehns-demo.servicebus.windows.net/.default"]
    assert not [k for k in cfg if k.startswith("sasl.") and k not in ("sasl.mechanism",)]


def test_librdkafka_producer_follows_event_hubs_guidance():
    cfg = eventhubs.producer_config("ehns-demo", _Credential())
    assert cfg["socket.keepalive.enable"] is True
    assert int(cfg["metadata.max.age.ms"]) < AZURE_IDLE_MS
    assert int(cfg["request.timeout.ms"]) >= 60_000


def test_the_hub_gives_up_on_a_hung_send_quickly():
    """Seen live: a hung Event Hubs connection held the hub's write for 2 minutes (delivery timeout) and failed the
    flow; retried three times, that failed the whole update. Like the RTM sinks, the hub retries a hung request on a new
    connection after 5 s, inside the same delivery timeout."""
    from pathlib import Path

    hub = (Path(__file__).parents[1] / "src/pipelines/subscription_hub/hub.py").read_text()
    write = hub[hub.index("def _write") : hub.index("def _hub")]
    assert "eventhubs.HUNG_REQUEST_GUARD" in write
