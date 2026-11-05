package com.databricks.hl7.subscriber.platform;

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
    void withoutAProfileItUsesTheAmbientIdentity() {
        var p = props();
        p.remove("databricks.profile");
        var c = SubscriberConfig.from(p);
        assertEquals(null, c.profile());
        assertEquals("subscriber", c.groupId());
    }

    @Test
    void argumentsOverrideTheFile() {
        var merged = SubscriberConfig.merge(props(), java.util.List.of("--e2e", "eventhubs.namespace=ns2", "x=a=b"));
        assertEquals("ns2", SubscriberConfig.from(merged).namespace());
        assertEquals("a=b", merged.getProperty("x"));
    }

    @Test
    void namesTheMissingSetting() {
        var p = props();
        p.remove("eventhubs.credential");
        var e = assertThrows(IllegalArgumentException.class, () -> SubscriberConfig.from(p));
        assertEquals("missing eventhubs.credential in subscriber.properties", e.getMessage());
    }

    @Test
    void refusesSecretsInTheFile() {
        var p = props();
        p.setProperty("client.secret", "x");
        assertThrows(IllegalArgumentException.class, () -> SubscriberConfig.from(p));
    }

    @Test
    void findsTheSettingsFromTheRepoRootThroughTheAppsOwnDirectory(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tmp)
            throws java.io.IOException {
        var root = java.nio.file.Files.createDirectories(tmp.resolve("repo"));
        var app = java.nio.file.Files.createDirectories(root.resolve("src/subscriber/apps"));
        var file = java.nio.file.Files.writeString(app.resolve("sp-app-a.properties"), "");
        var home = root.resolve("src/subscriber");

        assertEquals(file, SubscriberConfig.settingsFile(null, java.util.List.of(root, home)), "run from the repo root");
        assertEquals(file, SubscriberConfig.settingsFile(null, java.util.List.of(home, home)), "run from src/subscriber");
        var own = java.nio.file.Files.writeString(root.resolve("subscriber.properties"), "");
        assertEquals(own, SubscriberConfig.settingsFile(null, java.util.List.of(root, home)), "the current directory wins");
        assertEquals(null, SubscriberConfig.settingsFile(null, java.util.List.of(tmp)), "none: all settings as arguments");

        java.nio.file.Files.writeString(app.resolve("sp-app-b.properties"), "");
        assertThrows(IllegalArgumentException.class,
                () -> SubscriberConfig.settingsFile(null, java.util.List.of(home)), "several apps: name one");
    }
}
