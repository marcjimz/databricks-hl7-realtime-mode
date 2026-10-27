"""Azure Event Hubs over its Kafka endpoint. The connection string only ever comes from a secret scope."""

from pyspark.sql import SparkSession

from hl7rt.config import conf

_LOGIN = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"

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


def jaas_config(connection_string: str) -> str:
    return f'{_LOGIN} required username="$ConnectionString" password="{connection_string}";'


def kafka_options(namespace: str, connection_string: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    return {
        "kafka.bootstrap.servers": bootstrap_servers(namespace),
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config(connection_string),
        "kafka.session.timeout.ms": "30000",
        **_JAVA_CLIENT,
        **(extra or {}),
    }


def producer_config(namespace: str, connection_string: str) -> dict[str, str | int]:
    """confluent-kafka producer settings for Event Hubs, tuned for latency over batching."""
    return {
        "bootstrap.servers": bootstrap_servers(namespace),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": connection_string,
        "linger.ms": 0,
        "acks": 1,
        **_LIBRDKAFKA,
    }


def kafka_options_from_conf(spark: SparkSession) -> dict[str, str]:
    """Options for the namespace + secret named in pipeline config (hl7.eh.*)."""
    from pyspark.dbutils import DBUtils

    secret = DBUtils(spark).secrets.get(conf(spark, "eh.secret_scope"), conf(spark, "eh.secret_key"))
    return kafka_options(conf(spark, "eh.namespace"), secret)
