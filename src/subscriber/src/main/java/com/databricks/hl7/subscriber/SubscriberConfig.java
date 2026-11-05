package com.databricks.hl7.subscriber;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * Where the hub is and which identity to use: a properties file (written by `make registry`, gitignored) plus key=value
 * arguments. The SP's client secret stays in the Databricks CLI profile (~/.databrickscfg), never here.
 */
public record SubscriberConfig(String profile, String registryUrl, String registryEndpoint, String namespace,
        String credential, String groupId, Properties all) {

    /**
     * The settings file: the one named on the command line, else the first directory (in order) with a
     * subscriber.properties, or with exactly one file in apps/. Null when there's none (all settings as arguments).
     */
    public static Path settingsFile(Path named, List<Path> dirs) throws IOException {
        if (named != null) return named;
        for (Path dir : dirs) {
            if (Files.exists(dir.resolve("subscriber.properties"))) return dir.resolve("subscriber.properties");
            Path apps = dir.resolve("apps");
            if (!Files.isDirectory(apps)) continue;
            try (var files = Files.list(apps)) {
                var found = files.filter(f -> f.toString().endsWith(".properties")).sorted().toList();
                if (found.size() == 1) return found.get(0);
                if (found.size() > 1) throw new IllegalArgumentException("several apps in " + apps + ": name one, e.g. " + found.get(0));
            }
        }
        return null;
    }

    /**
     * databricks.profile is optional: without it the app uses the ambient identity, e.g. the job's run-as service
     * principal when it runs as a workspace job.
     */
    public static SubscriberConfig from(Properties p) {
        for (String key : p.stringPropertyNames()) {
            if (key.toLowerCase().contains("secret") || key.toLowerCase().contains("password")) {
                throw new IllegalArgumentException(key + ": keep secrets in the Databricks CLI profile, not here");
            }
        }
        String profile = p.getProperty("databricks.profile", "").trim();
        return new SubscriberConfig(profile.isEmpty() ? null : profile, required(p, "registry.url"),
                required(p, "registry.endpoint"), required(p, "eventhubs.namespace"), required(p, "eventhubs.credential"),
                p.getProperty("consumer.group", profile.isEmpty() ? "subscriber" : profile).trim(), p);
    }

    /** An optional setting (e.g. the E2E checks' forbidden credential and topic). */
    public String get(String key) {
        String v = all.getProperty(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    /** key=value arguments override the file, so a workspace job can pass its settings as task parameters. */
    public static Properties merge(Properties file, java.util.List<String> args) {
        var p = new Properties();
        p.putAll(file);
        for (String a : args) {
            int i = a.indexOf('=');
            if (i > 0) p.setProperty(a.substring(0, i).trim(), a.substring(i + 1).trim());
        }
        return p;
    }

    /** A service principal's Databricks user name is its application ID (a UUID); a person's is an email. */
    public static boolean isServicePrincipal(String userName) {
        return userName != null && userName.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    private static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("missing " + key + " in subscriber.properties");
        return v.trim();
    }
}
