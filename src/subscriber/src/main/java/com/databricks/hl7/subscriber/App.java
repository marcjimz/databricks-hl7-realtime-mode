package com.databricks.hl7.subscriber;

import com.databricks.hl7.subscriber.e2e.E2E;
import com.databricks.hl7.subscriber.hub.Activation;
import com.databricks.hl7.subscriber.hub.Listener;
import com.databricks.hl7.subscriber.hub.RegistryClient;
import com.databricks.hl7.subscriber.platform.Databricks;
import com.databricks.hl7.subscriber.platform.EventHubsTokens;
import com.databricks.hl7.subscriber.platform.KafkaSettings;
import com.databricks.hl7.subscriber.platform.SubscriberConfig;
import com.databricks.hl7.subscriber.platform.UcOAuthBearerHandler;
import com.databricks.hl7.subscriber.subscriptions.Subscription;
import com.databricks.hl7.subscriber.subscriptions.Sync;
import com.databricks.sdk.WorkspaceClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Subscriber app. Signs in as its Databricks service principal, registers {@link MySubscriptions} with the hub, waits
 * until the hub runs exactly those revisions, then prints the notifications that arrive on its own Event Hub.
 *
 * <p>Nothing is silent: it exits non-zero if the registry refuses a subscription (exit 2), the hub fails one (3), the
 * hub doesn't confirm them in time or isn't processing their sources (4), it isn't signed in as a service principal (5),
 * or the registry refuses its identity (6). While listening it warns when the hub stops.
 *
 * <pre>
 *   subscriptions/  what the app asks for (Predicate, Subscription, Sync)
 *   hub/            the round trip with the platform: register, wait for the hub, receive (RegistryClient, Listener)
 *   platform/       how it connects: settings, identity, Event Hubs tokens and Kafka settings
 *   e2e/            the end-to-end checks
 * </pre>
 */
public final class App {
    private static final Duration ACTIVATION_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration HUB_STALE_AFTER = Duration.ofMinutes(2);
    private static final Duration HEALTH_CHECK_EVERY = Duration.ofSeconds(30);

    /**
     * java -jar hl7-subscriber.jar [file.properties] [key=value ...] [--duration=PT5M] [--e2e]
     *
     * <p>Settings come from the file (default: subscriber.properties, else the one file in apps/, which `make registry`
     * writes) plus key=value arguments. --e2e runs the end-to-end checks instead of the app.
     */
    public static void main(String[] args) throws Exception {
        List<String> argv = List.of(args);
        SubscriberConfig cfg = SubscriberConfig.from(SubscriberConfig.merge(load(argv), argv));
        var me = Databricks.signIn(cfg);
        if (!me.isServicePrincipal()) {
            exit(5, "signed in as " + me.userName() + ", not a service principal"
                    + (cfg.profile() == null ? "" : ": check profile " + cfg.profile()));
        }
        System.out.printf("signed in   as %s (%s)%n", me.displayName(), me.userName());
        WorkspaceClient workspace = me.workspace();
        if (argv.contains("--e2e")) System.exit(E2E.run(workspace, cfg));
        Duration runFor = argv.stream().filter(a -> a.startsWith("--duration=")).findFirst()
                .map(a -> Duration.parse(a.substring("--duration=".length()))).orElse(null);

        // 1. Register what the code declares. The Data API runs each call as this SP's own Postgres role, and the
        //    registry validates each rule in the same call: ACCEPTED, or refused with a reason.
        var registry = new RegistryClient(URI.create(cfg.registryUrl()), () -> Databricks.bearer(workspace),
                RegistryClient.jdkHttp());
        RegistryClient.Listing registered;
        try {
            registered = registry.list();
        } catch (RegistryClient.RegistryException e) {
            exit(6, "the registry refused this identity (" + e.getMessage() + "): is the app onboarded (make registry)?");
            return;
        }
        var plan = Sync.plan(MySubscriptions.all(), registered.subscriptions());
        Map<String, Integer> accepted = new LinkedHashMap<>();  // name -> revision
        Map<String, Integer> current = new LinkedHashMap<>();   // subscriptionId -> revision
        String topic = null;
        int refused = 0;
        Map<String, Integer> before = registered.subscriptions().stream().filter(RegistryClient.Status::enabled)
                .collect(Collectors.toMap(RegistryClient.Status::name, RegistryClient.Status::revision));
        for (Subscription s : plan.subscribe()) {
            try {
                var r = registry.subscribe(s);
                accepted.put(r.name(), r.revision());
                current.put(r.subscriptionId(), r.revision());
                topic = r.destination();
                boolean unchanged = Integer.valueOf(r.revision()).equals(before.get(r.name()));
                System.out.printf("subscribed  %-22s revision %d %s%s%n            %s%n", r.name(), r.revision(), r.state(),
                        unchanged ? " (unchanged)" : "", r.predicateSql());
            } catch (RegistryClient.RegistryException e) {
                refused++;
                System.out.printf("refused     %-22s %s%n", s.name(), e.getMessage());
            }
        }
        for (String name : plan.unsubscribe()) {
            registry.unsubscribe(name);
            System.out.printf("cancelled   %-22s (no longer in MySubscriptions)%n", name);
        }
        if (refused > 0) exit(2, refused + " subscription(s) refused; fix MySubscriptions.java");
        if (accepted.isEmpty()) exit(0, "no subscriptions declared in MySubscriptions.java");
        Set<String> sources = plan.subscribe().stream().map(Subscription::source).collect(Collectors.toSet());

        // 2. Wait until the hub runs exactly these revisions.
        var waited = Activation.await(registry, accepted, sources, ACTIVATION_TIMEOUT, HUB_STALE_AFTER, System.out);
        if (waited.outcome() == Activation.Outcome.FAILED) exit(3, waited.message());
        if (waited.outcome() == Activation.Outcome.TIMED_OUT) exit(4, waited.message());

        // 3. Consume this app's hub with a token UC mints from its service credential (no Azure secret here).
        UcOAuthBearerHandler.use(EventHubsTokens.forCredential(workspace, cfg.credential(), cfg.namespace()));
        try (var consumer = new KafkaConsumer<String, String>(KafkaSettings.consumer(cfg.namespace(), cfg.groupId()))) {
            consumer.subscribe(List.of(topic));
            System.out.printf("listening   on %s as consumer group %s (Ctrl-C to stop)%n", topic, cfg.groupId());
            var listener = new Listener(current, cfg.groupId(), System.out, Instant.now());
            listener.run(consumer, runFor == null ? Instant.MAX : Instant.now().plus(runFor), () -> {
                for (String s : Activation.staleSources(registry.list().hub(), sources, Instant.now(), HUB_STALE_AFTER)) {
                    System.out.printf("WARNING     the hub hasn't processed %s for over %d minutes: is the "
                            + "subscription_hub pipeline running?%n", s, HUB_STALE_AFTER.toMinutes());
                }
            }, HEALTH_CHECK_EVERY);
        }
        exit(0, "ran for " + runFor);
    }

    static void exit(int code, String message) {
        System.out.println((code == 0 ? "done        " : "ERROR       ") + message);
        System.exit(code);
    }

    /**
     * The settings file named on the command line, else subscriber.properties, else the one file in apps/: in the
     * current directory, then in the app's own directory (src/subscriber, above target/), so it runs from the repo root.
     */
    private static Properties load(List<String> argv) throws IOException {
        Path named = argv.stream().filter(a -> a.endsWith(".properties")).map(Path::of).findFirst().orElse(null);
        List<Path> dirs = appHome() == null ? List.of(Path.of("")) : List.of(Path.of(""), appHome());
        Path file = SubscriberConfig.settingsFile(named, dirs);
        var p = new Properties();
        if (file == null) return p;  // all settings as key=value arguments (workspace job)
        if (!Files.exists(file)) throw new IllegalArgumentException(file + " not found");
        try (InputStream in = Files.newInputStream(file)) {
            p.load(in);
        }
        return p;
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
