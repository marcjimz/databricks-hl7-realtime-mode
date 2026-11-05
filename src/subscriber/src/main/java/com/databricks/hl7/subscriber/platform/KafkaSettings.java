package com.databricks.hl7.subscriber.platform;

import java.util.Properties;

/** Kafka client settings for Event Hubs: Entra OAUTHBEARER (no keys) plus Microsoft's Event Hubs client guidance. */
public final class KafkaSettings {
    private KafkaSettings() { }

    public static Properties consumer(String namespace, String groupId) {
        var p = new Properties();
        p.put("bootstrap.servers", namespace + ".servicebus.windows.net:9093");
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "OAUTHBEARER");
        p.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        p.put("sasl.login.callback.handler.class", UcOAuthBearerHandler.class.getName());
        p.put("group.id", groupId);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "latest");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // learn.microsoft.com/azure/event-hubs/apache-kafka-configurations: Azure closes idle connections after 240 s.
        p.put("metadata.max.age.ms", "180000");
        p.put("connections.max.idle.ms", "180000");
        p.put("request.timeout.ms", "60000");
        p.put("session.timeout.ms", "30000");
        return p;
    }
}
