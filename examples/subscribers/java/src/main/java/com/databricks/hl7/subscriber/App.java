package com.databricks.hl7.subscriber;

import com.databricks.hl7.subscriber.api.Api;
import com.databricks.hl7.subscriber.api.SubscriptionsApi;
import com.databricks.hl7.subscriber.api.client.ApiException;
import com.databricks.hl7.subscriber.api.model.Accepted;
import com.databricks.hl7.subscriber.api.model.Cancelled;
import com.databricks.hl7.subscriber.api.model.Connection;
import com.databricks.hl7.subscriber.api.model.DeclareResult;
import com.databricks.hl7.subscriber.api.model.Refusal;
import com.databricks.hl7.subscriber.api.model.Status;
import com.databricks.hl7.subscriber.api.model.Status.StateEnum;
import com.databricks.hl7.subscriber.api.model.Subscription;
import com.databricks.hl7.subscriber.e2e.E2E;
import com.databricks.hl7.subscriber.notifications.Listener;
import com.databricks.hl7.subscriber.platform.EventHubs;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.iam.MeRequest;
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
 * An example subscriber app. Signs in as its own Databricks service principal, declares {@link MySubscriptions} to the
 * Subscription API (through the client generated from its contract, docs/api/openapi.json), waits until the hub runs exactly those revisions, then prints the notifications that arrive on its own Event Hub.
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
 * Settings: only databricks.profile (the service principal) and api.app (the Subscription API's Databricks App name;
 * its URL is looked up, so a rebuilt workspace needs no change; api.url overrides it). Its topic, Kafka bootstrap
 * servers, and Unity Catalog credential come from the API (getConnection). They're read from the file (default subscriber.properties,
 * else the one file in apps/, which `make registry` writes; looked up in the current directory, then next to the jar)
 * plus key=value arguments. --e2e runs the end-to-end checks instead.
 */
public final class App {
    static final int WAIT_SECONDS = 120;
    static final int POLL_SECONDS = 25;
    private static final Duration HEALTH_CHECK_EVERY = Duration.ofSeconds(30);

    public static void main(String[] args) throws Exception {
        List<String> argv = List.of(args);
        Properties cfg = settings(argv);
        WorkspaceClient workspace = signIn(cfg);
        SubscriptionsApi api = Api.connect(apiUrl(workspace, cfg), () -> bearer(workspace));

        // 0. Where this app's notifications arrive and how to read them: the API knows, so the app configures neither.
        Connection connection;
        try {
            connection = api.getConnection();
        } catch (ApiException e) {
            exit(e.getCode() == 403 || e.getCode() == 503 ? 6 : 3, Api.problem(e).getCode() + ": "
                    + Api.problem(e).getMessage() + (e.getCode() == 403 ? " (make registry onboards apps)" : ""));
            return;
        }
        if (argv.contains("--e2e")) System.exit(E2E.run(workspace, api, connection, cfg));
        Duration runFor = argv.stream().filter(a -> a.startsWith("--duration=")).findFirst()
                .map(a -> Duration.parse(a.substring("--duration=".length()))).orElse(null);

        // 1. Declare what the code says. Databricks compiles each rule in this call: ACCEPTED, or refused with its error.
        DeclareResult declared;
        try {
            declared = api.declare(Api.declared(MySubscriptions.all()));
        } catch (ApiException e) {
            if (e.getCode() == 403) exit(6, Api.problem(e).getMessage() + " (make registry onboards apps)");
            if (e.getCode() != 422) exit(3, "the Subscription API answered " + e.getCode() + ": " + Api.problem(e).getMessage());
            List<Refusal> refused = Api.refusals(e);
            for (Refusal r : refused) {
                System.out.printf("refused     %-22s %s: %s%n", r.getName() == null ? "" : r.getName(), r.getCode(),
                        firstLine(r.getMessage()));
            }
            exit(2, refused.size() + " subscription(s) refused; fix MySubscriptions.java");
            return;
        }
        Map<String, Integer> current = new HashMap<>();  // subscriptionId -> revision
        for (Accepted s : declared.getAccepted()) {
            current.put(s.getSubscriptionId(), s.getRevision());
            System.out.printf("subscribed  %-22s revision %d ACCEPTED%s%n            %s%n", s.getName(), s.getRevision(),
                    Boolean.TRUE.equals(s.getUnchanged()) ? " (unchanged)" : "", s.getQuery());
        }
        for (Cancelled s : declared.getCancelled()) {
            System.out.printf("cancelled   %-22s (no longer in MySubscriptions)%n", s.getName());
        }
        if (current.isEmpty()) exit(0, "no subscriptions declared in MySubscriptions.java");

        // 2. Wait until the hub runs exactly these revisions: READY, FAILED, or not within WAIT_SECONDS. The API holds
        //    each call for up to POLL_SECONDS (short, so no proxy in between cuts it off).
        Instant deadline = Instant.now().plusSeconds(WAIT_SECONDS);
        Status status;
        while (true) {
            long left = Math.max(1, Duration.between(Instant.now(), deadline).getSeconds());
            try {
                status = api.getStatus((int) Math.min(POLL_SECONDS, left));
            } catch (ApiException e) {
                exit(4, "the Subscription API answered " + e.getCode() + ": " + Api.problem(e).getMessage());
                return;
            }
            if (status.getState() == StateEnum.READY || status.getState() == StateEnum.FAILED
                    || !Instant.now().isBefore(deadline)) break;
        }
        for (Subscription s : status.getSubscriptions()) {
            if (Boolean.TRUE.equals(s.getEnabled()) && !"PENDING".equals(s.getState())) {
                System.out.printf("hub         %-22s revision %d %s%n", s.getName(), s.getRevision(), s.getState());
            }
        }
        switch (status.getState()) {
            case READY -> { }
            case FAILED -> exit(3, "the hub can't run " + status.getProblems());
            default -> exit(4, status.getState() + ": " + status.getProblems()
                    + " (is the subscription_hub pipeline running? make demo dev)");
        }

        // 3. Consume this app's topic with a token Unity Catalog mints from its service credential (no Azure secret here).
        EventHubs.useCredential(workspace, connection.getCredential(), connection.getTokenScope());
        String group = cfg.getProperty("consumer.group", cfg.getProperty("databricks.profile", "subscriber"));
        try (var consumer = new KafkaConsumer<String, String>(EventHubs.consumer(connection.getBootstrapServers(), group))) {
            consumer.subscribe(List.of(connection.getTopic()));
            System.out.printf("listening   on %s as consumer group %s (Ctrl-C to stop)%n", connection.getTopic(), group);
            new Listener(current, group, System.out, Instant.now()).run(consumer,
                    runFor == null ? Instant.MAX : Instant.now().plus(runFor), () -> healthCheck(api), HEALTH_CHECK_EVERY);
        }
        exit(0, "ran for " + runFor);
    }

    /** While listening: warn (don't stop) if the hub has stopped, or the API can't be reached for a moment. */
    static void healthCheck(SubscriptionsApi api) {
        try {
            Status now = api.getStatus(0);
            if (now.getState() == StateEnum.HUB_STALE) {
                System.out.printf("WARNING     %s: is the subscription_hub pipeline running?%n", now.getProblems());
            }
        } catch (ApiException | RuntimeException e) {
            System.out.printf("WARNING     health check failed (%s); still listening%n", e.getMessage());
        }
    }

    static void exit(int code, String message) {
        System.out.println((code == 0 ? "done        " : "ERROR       ") + message);
        System.exit(code);
    }

    public static String firstLine(String s) {
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

    public static boolean isServicePrincipal(String userName) {
        return userName != null && userName.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    /** The SP's Databricks OAuth token (the SDK refreshes it). */
    public static String bearer(WorkspaceClient workspace) {
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

    /** The Subscription API's URL: api.url if set, else the URL of the Databricks App named api.app (CAN_USE can read it). */
    static String apiUrl(WorkspaceClient workspace, Properties cfg) {
        String url = cfg.getProperty("api.url", "").trim();
        if (!url.isEmpty()) return url;
        String name = required(cfg, "api.app");
        try {
            url = workspace.apps().get(name).getUrl();
        } catch (DatabricksError e) {
            exit(6, "can't find the Subscription API app " + name + ": " + firstLine(e.getMessage())
                    + " (make registry grants CAN_USE)");
        }
        if (url == null || url.isBlank()) exit(4, "the Subscription API app " + name + " has no URL yet (not deployed?)");
        return url;
    }

    public static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("missing " + key + " (make registry writes it to apps/)");
        return v.trim();
    }

    /** The directory above the jar's target/ (examples/subscribers/java), or null when it isn't a local build (e.g. a job). */
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
