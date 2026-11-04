package com.databricks.hl7.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class SubscriberConfigTest {

    static Properties props() {
        var p = new Properties();
        p.setProperty("databricks.profile", "sp-app-a");
        p.setProperty("registry.url", "https://ep-1.database.eastus2.azuredatabricks.net/api/2.0/workspace/1/rest/db");
        p.setProperty("registry.endpoint", "projects/p/branches/production/endpoints/primary");
        p.setProperty("eventhubs.namespace", "ns1");
        p.setProperty("eventhubs.credential", "sub_app_a_eventhubs");
        return p;
    }

    @Test
    void readsEverythingFromTheFileAndDefaultsTheGroupToTheProfile() {
        var c = SubscriberConfig.from(props());
        assertEquals("sp-app-a", c.profile());
        assertEquals("ns1", c.namespace());
        assertEquals("sub_app_a_eventhubs", c.credential());
        assertEquals("sp-app-a", c.groupId());
    }

    @Test
    void namesTheMissingSetting() {
        var p = props();
        p.remove("eventhubs.credential");
        var e = assertThrows(IllegalArgumentException.class, () -> SubscriberConfig.from(p));
        assertEquals("missing eventhubs.credential in subscriber.properties", e.getMessage());
    }

    @Test
    void onlyAServicePrincipalMayRunTheApp() {
        assertEquals(true, SubscriberConfig.isServicePrincipal("38fafc55-d28b-4f48-8d17-299f8d2698ee"));
        assertEquals(false, SubscriberConfig.isServicePrincipal("someone@example.com"));
    }

    @Test
    void refusesSecretsInTheFile() {
        var p = props();
        p.setProperty("client.secret", "x");
        assertThrows(IllegalArgumentException.class, () -> SubscriberConfig.from(p));
    }
}
