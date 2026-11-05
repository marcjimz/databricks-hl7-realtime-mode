package com.databricks.hl7.subscriber.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class KafkaSettingsTest {

    @Test
    void consumerUsesEntraOAuthBearerAndNoSecrets() {
        var p = KafkaSettings.consumer("ns1", "app-a");

        assertEquals("ns1.servicebus.windows.net:9093", p.get("bootstrap.servers"));
        assertEquals("SASL_SSL", p.get("security.protocol"));
        assertEquals("OAUTHBEARER", p.get("sasl.mechanism"));
        assertEquals(UcOAuthBearerHandler.class.getName(), p.get("sasl.login.callback.handler.class"));
        assertEquals("app-a", p.get("group.id"));
        String jaas = (String) p.get("sasl.jaas.config");
        assertTrue(jaas.startsWith("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"));
        assertFalse(jaas.contains("password"), "no connection strings or keys");
    }

    @Test
    void followsMicrosoftsEventHubsClientGuidance() {
        // Azure closes idle connections after 240 s: refresh metadata and recycle connections before that.
        var p = KafkaSettings.consumer("ns1", "app-a");
        assertTrue(Integer.parseInt((String) p.get("metadata.max.age.ms")) < 240_000);
        assertTrue(Integer.parseInt((String) p.get("connections.max.idle.ms")) < 240_000);
    }

    @Test
    void commitsOnlyAfterTheAppHandledARecord() {
        var p = KafkaSettings.consumer("ns1", "app-a");
        assertEquals("false", p.get("enable.auto.commit"));
    }
}
