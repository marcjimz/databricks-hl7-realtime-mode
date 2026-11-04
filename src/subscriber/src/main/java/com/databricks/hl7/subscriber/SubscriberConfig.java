package com.databricks.hl7.subscriber;

import java.util.Properties;

/**
 * Where the hub is and which identity to use, from subscriber.properties (gitignored). The SP's client secret stays in
 * the Databricks CLI profile (~/.databrickscfg), never in this file.
 */
public record SubscriberConfig(String profile, String registryUrl, String registryEndpoint, String namespace,
        String credential, String groupId) {

    public static SubscriberConfig from(Properties p) {
        for (String key : p.stringPropertyNames()) {
            if (key.toLowerCase().contains("secret") || key.toLowerCase().contains("password")) {
                throw new IllegalArgumentException(key + ": keep secrets in the Databricks CLI profile, not here");
            }
        }
        String profile = required(p, "databricks.profile");
        return new SubscriberConfig(profile, required(p, "registry.url"), required(p, "registry.endpoint"),
                required(p, "eventhubs.namespace"), required(p, "eventhubs.credential"),
                p.getProperty("consumer.group", profile));
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
