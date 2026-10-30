"""Azure Event Hubs over its Kafka endpoint, authenticated with Entra ID through a Unity Catalog service credential.

The service credential wraps the access connector's managed identity (Azure Event Hubs Data Sender / Receiver on the
namespace). There are no connection strings: the namespace has SAS keys disabled.
"""

from pyspark.sql import SparkSession

from hl7rt.config import conf

# Microsoft's Event Hubs client guidance (learn.microsoft.com/azure/event-hubs/apache-kafka-configurations):
# Azure closes idle connections after 240 s, so refresh metadata and recycle connections before that.
_JAVA_CLIENT = {
    "kafka.metadata.max.age.ms": "180000",
    "kafka.connections.max.idle.ms": "180000",
    "kafka.request.timeout.ms": "60000",
    "kafka.max.request.size": "1000000",
}
_LIBRDKAFKA = {"socket.keepalive.enable": True, "metadata.max.age.ms": 180000, "request.timeout.ms": 60000}


def bootstrap_servers(namespace: str) -> str:
    return f"{namespace}.servicebus.windows.net:9093"


def token_scope(namespace: str) -> str:
    return f"https://{namespace}.servicebus.windows.net/.default"


def kafka_options(namespace: str, service_credential: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    """Spark Kafka source/sink options. Databricks configures SASL/OAUTHBEARER from the service credential, so no
    kafka.sasl.* / kafka.security.* options may be set (they would replace it)."""
    return {
        "kafka.bootstrap.servers": bootstrap_servers(namespace),
        "databricks.serviceCredential": service_credential,
        "kafka.session.timeout.ms": "30000",
        **_JAVA_CLIENT,
        **(extra or {}),
    }


def producer_config(namespace: str, credential) -> dict:
    """confluent-kafka producer settings, tuned for latency over batching. `credential` is an azure-identity
    TokenCredential: dbutils.credentials.getServiceCredentialsProvider(<service credential>)."""
    scope = token_scope(namespace)

    def oauth_cb(_config):
        token = credential.get_token(scope)
        return token.token, float(token.expires_on)

    return {
        "bootstrap.servers": bootstrap_servers(namespace),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
        "linger.ms": 0,
        "acks": 1,
        **_LIBRDKAFKA,
    }


def kafka_options_from_conf(spark: SparkSession) -> dict[str, str]:
    """Options for the namespace + service credential named in pipeline config (hl7.eh.*)."""
    return kafka_options(conf(spark, "eh.namespace"), conf(spark, "eh.service_credential"))
