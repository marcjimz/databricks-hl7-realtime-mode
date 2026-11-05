package com.databricks.hl7.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.hl7.subscriber.api.SubscriptionApi;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AppTest {

    @Test
    void findsTheSettingsFromTheRepoRootThroughTheAppsOwnDirectory(@TempDir Path tmp) throws IOException {
        var root = Files.createDirectories(tmp.resolve("repo"));
        var apps = Files.createDirectories(root.resolve("src/subscriber/apps"));
        var file = Files.writeString(apps.resolve("sp-app-a.properties"), "");
        var home = root.resolve("src/subscriber");

        assertEquals(file, App.settingsFile(null, List.of(root, home)), "run from the repo root");
        assertEquals(file, App.settingsFile(null, List.of(home, home)), "run from src/subscriber");
        var own = Files.writeString(root.resolve("subscriber.properties"), "");
        assertEquals(own, App.settingsFile(null, List.of(root, home)), "the current directory wins");
        assertNull(App.settingsFile(null, List.of(tmp)), "none: all settings as arguments");

        Files.writeString(apps.resolve("sp-app-b.properties"), "");
        assertThrows(IllegalArgumentException.class, () -> App.settingsFile(null, List.of(home)), "several apps: name one");
    }

    @Test
    void argumentsOverrideTheFileAndSecretsAreRefused(@TempDir Path tmp) throws IOException {
        var f = Files.writeString(tmp.resolve("x.properties"), "api.url = https://a\nconsumer.group = g1\n");
        var p = App.settings(List.of(f.toString(), "consumer.group=g2", "--duration=PT1M"));
        assertEquals("https://a", p.getProperty("api.url"));
        assertEquals("g2", p.getProperty("consumer.group"));
        assertNull(p.getProperty("--duration"));

        assertThrows(IllegalArgumentException.class, () -> App.settings(List.of(f.toString(), "client_secret=x")));
        assertThrows(IllegalArgumentException.class, () -> App.required(p, "eventhubs.namespace"));
    }

    @Test
    void onlyAServicePrincipalMayRunTheApp() {
        assertTrue(App.isServicePrincipal("38fafc55-d28b-4f48-8d17-299f8d2698ee"));
        assertFalse(App.isServicePrincipal("someone@example.com"));
        assertFalse(App.isServicePrincipal(null));
    }

    @Test
    void theApiIsHttpsOnly() {
        assertThrows(IllegalArgumentException.class, () -> new SubscriptionApi("http://example.com", () -> "t"));
    }
}
