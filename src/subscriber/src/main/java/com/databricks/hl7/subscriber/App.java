package com.databricks.hl7.subscriber;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.iam.MeRequest;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Subscriber app. Signs in as its own Databricks service principal, declares {@link MySubscriptions} to the Subscription
 * API, waits until the hub runs exactly those revisions, then prints the notifications that arrive on its own Event Hub.
 * The API does the rest: Databricks compiles each rule, the registry keeps it, the hub runs it.
 *
 * <p>Nothing is silent. Exit 2: a rule was refused (Databricks' error, or a shape the hub won't run). 3: the hub can't run
 * one. 4: the hub didn't confirm in time or isn't processing. 5: not signed in as a service principal. 6: this service
 * principal isn't an onboarded subscriber app.
 *
 * <pre>
 *   java -jar hl7-subscriber.jar [file.properties] [key=value ...] [--duration=PT5M] [--e2e]
 * </pre>
 *
 * Settings: the file (default subscriber.properties, else the one file in apps/, which `make registry` writes; looked up
 * in the current directory, then next to the jar) plus key=value arguments. --e2e runs the end-to-end checks instead.
 */
public final class App {
    static final int WAIT_SECONDS = 120;
    private static final Duration HEALTH_CHECK_EVERY = Duration.ofSeconds(30);

    public static void main(String[] args) throws Exception {
        List<String> argv = List.of(args);
        Properties cfg = settings(argv);
        WorkspaceClient workspace = signIn(cfg);
        var api = new SubscriptionApi(required(cfg, "api.url"), () -> bearer(workspace));
        if (argv.contains("--e2e")) System.exit(E2E.run(workspace, api, cfg));
        Duration runFor = argv.stream().filter(a -> a.startsWith("--duration=")).findFirst()
                .map(a -> Duration.parse(a.substring("--duration=".length()))).orElse(null);

        // 1. Declare what the code says. Databricks compiles each rule in this call: ACCEPTED, or refused with its error.
        var declared = api.declare(MySubscriptions.all());
        if (declared.status() == 403) exit(6, declared.body().path("message").asText() + " (make registry onboards apps)");
        if (declared.status() == 422) {
            for (JsonNode r : declared.body().path("refused")) {
                System.out.printf("refused     %-22s %s: %s%n", r.path("name").asText(), r.path("code").asText(),
                        firstLine(r.path("message").asText()));
            }
            exit(2, declared.body().path("refused").size() + " subscription(s) refused; fix MySubscriptions.java");
        }
        if (!declared.ok()) exit(3, "the Subscription API answered " + declared.status() + ": " + declared.body());
        Map<String, Integer> current = new HashMap<>();  // subscriptionId -> revision
        String topic = null;
        for (JsonNode s : declared.body().path("accepted")) {
            current.put(s.path("subscriptionId").asText(), s.path("revision").asInt());
            topic = s.path("destination").asText();
            System.out.printf("subscribed  %-22s revision %d ACCEPTED%s%n            %s%n", s.path("name").asText(),
                    s.path("revision").asInt(), s.path("unchanged").asBoolean() ? " (unchanged)" : "", s.path("query").asText());
        }
        for (JsonNode s : declared.body().path("cancelled")) {
            System.out.printf("cancelled   %-22s (no longer in MySubscriptions)%n", s.path("name").asText());
        }
        if (current.isEmpty()) exit(0, "no subscriptions declared in MySubscriptions.java");

        // 2. Wait until the hub runs exactly these revisions (the API waits; READY, FAILED, or not yet).
        JsonNode status = api.status(WAIT_SECONDS).body();
        for (JsonNode s : status.path("subscriptions")) {
            if (s.path("enabled").asBoolean() && !"PENDING".equals(s.path("state").asText())) {
                System.out.printf("hub         %-22s revision %d %s%n", s.path("name").asText(), s.path("revision").asInt(),
                        s.path("state").asText());
            }
        }
        switch (status.path("state").asText()) {
            case "READY" -> { }
            case "FAILED" -> exit(3, "the hub can't run " + status.path("problems"));
            default -> exit(4, status.path("state").asText() + ": " + status.path("problems")
                    + " (is the subscription_hub pipeline running? make demo dev)");
        }

        // 3. Consume this app's hub with a token Unity Catalog mints from its service credential (no Azure secret here).
        EventHubs.useCredential(workspace, required(cfg, "eventhubs.credential"), required(cfg, "eventhubs.namespace"));
        String group = cfg.getProperty("consumer.group", cfg.getProperty("databricks.profile", "subscriber"));
        try (var consumer = new KafkaConsumer<String, String>(EventHubs.consumer(required(cfg, "eventhubs.namespace"), group))) {
            consumer.subscribe(List.of(topic));
            System.out.printf("listening   on %s as consumer group %s (Ctrl-C to stop)%n", topic, group);
            new Listener(current, group, System.out, Instant.now()).run(consumer,
                    runFor == null ? Instant.MAX : Instant.now().plus(runFor), () -> {
                        JsonNode now = api.status(0).body();
                        if ("HUB_STALE".equals(now.path("state").asText())) {
                            System.out.printf("WARNING     %s: is the subscription_hub pipeline running?%n",
                                    now.path("problems"));
                        }
                    }, HEALTH_CHECK_EVERY);
        }
        exit(0, "ran for " + runFor);
    }

    static void exit(int code, String message) {
        System.out.println((code == 0 ? "done        " : "ERROR       ") + message);
        System.exit(code);
    }

    static String firstLine(String s) {
        return s.strip().lines().findFirst().orElse("");
    }

    // ---------------------------------------------------------------------------------------------------------------
    /**
     * With a profile: OAuth M2M from that profile only (DATABRICKS_* environment variables would otherwise take
     * precedence). Without one: the ambient identity, e.g. a workspace job's run-as service principal. Either way it
     * must be a service principal, whose user name is its application ID.
     */
    static WorkspaceClient signIn(Properties cfg) {
        String profile = cfg.getProperty("databricks.profile", "").trim();
        var c = profile.isEmpty() ? new DatabricksConfig() : new DatabricksConfig().setProfile(profile).setAuthType("oauth-m2m");
        var workspace = new WorkspaceClient(c);
        var me = workspace.currentUser().me(new MeRequest());
        if (!isServicePrincipal(me.getUserName())) {
            exit(5, "signed in as " + me.getUserName() + ", not a service principal" + (profile.isEmpty() ? "" : ": check profile " + profile));
        }
        System.out.printf("signed in   as %s (%s)%n", me.getDisplayName(), me.getUserName());
        return workspace;
    }

    static boolean isServicePrincipal(String userName) {
        return userName != null && userName.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    /** The SP's Databricks OAuth token (the SDK refreshes it). */
    static String bearer(WorkspaceClient workspace) {
        return workspace.config().authenticate().get("Authorization").replaceFirst("^Bearer ", "");
    }

    // ---------------------------------------------------------------------------------------------------------------
    /** The settings file plus key=value arguments (which win). Refuses secrets: they belong in the CLI profile. */
    static Properties settings(List<String> argv) throws IOException {
        Path named = argv.stream().filter(a -> a.endsWith(".properties")).map(Path::of).findFirst().orElse(null);
        Path file = settingsFile(named, appHome() == null ? List.of(Path.of("")) : List.of(Path.of(""), appHome()));
        var p = new Properties();
        if (file != null) {
            if (!Files.exists(file)) throw new IllegalArgumentException(file + " not found");
            try (InputStream in = Files.newInputStream(file)) {
                p.load(in);
            }
        }
        for (String a : argv) {
            int i = a.indexOf('=');
            if (i > 0 && !a.startsWith("--")) p.setProperty(a.substring(0, i).trim(), a.substring(i + 1).trim());
        }
        for (String key : p.stringPropertyNames()) {
            if (key.toLowerCase().contains("secret") || key.toLowerCase().contains("password")) {
                throw new IllegalArgumentException(key + ": keep secrets in the Databricks CLI profile, not here");
            }
        }
        return p;
    }

    /** The named file, else the first directory with subscriber.properties or exactly one file in apps/; else null. */
    static Path settingsFile(Path named, List<Path> dirs) throws IOException {
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

    static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("missing " + key + " (make registry writes it to apps/)");
        return v.trim();
    }

    /** The directory above the jar's target/ (src/subscriber), or null when it isn't a local build (e.g. a job). */
    private static Path appHome() {
        try {
            Path jar = Path.of(App.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            Path target = jar.getParent();
            return target != null && "target".equals(String.valueOf(target.getFileName())) ? target.getParent() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
