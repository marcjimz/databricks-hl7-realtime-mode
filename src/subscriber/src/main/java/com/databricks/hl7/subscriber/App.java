package com.databricks.hl7.subscriber;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.GenerateTemporaryServiceCredentialAzureOptions;
import com.databricks.sdk.service.catalog.GenerateTemporaryServiceCredentialRequest;
import com.databricks.sdk.service.catalog.TemporaryCredentials;
import com.databricks.sdk.service.iam.MeRequest;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Subscriber app. Signs in as its Databricks service principal, registers {@link MySubscriptions} with the hub, waits
 * until the hub runs exactly those revisions, then prints the notifications that arrive on its own Event Hub.
 *
 * <p>Nothing is silent: it exits non-zero if the registry refuses a subscription (exit 2), the hub fails one (3), or the
 * hub doesn't confirm them in time or isn't processing their sources (4); while listening it warns when the hub stops.
 */
public final class App {
    private static final Duration ACTIVATION_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration HUB_STALE_AFTER = Duration.ofMinutes(2);
    private static final Duration HEALTH_CHECK_EVERY = Duration.ofSeconds(30);
    private static final Duration BACKLOG_AFTER = Duration.ofMinutes(2);

    /**
     * java -jar hl7-subscriber.jar [file.properties] [key=value ...] [--duration=PT5M] [--e2e]
     *
     * <p>Settings come from the file (default: subscriber.properties, else the one file in apps/, which `make registry`
     * writes) plus key=value arguments. --e2e runs the end-to-end checks instead of the app.
     */
    public static void main(String[] args) throws Exception {
        List<String> argv = List.of(args);
        SubscriberConfig cfg = SubscriberConfig.from(SubscriberConfig.merge(load(argv), argv));
        WorkspaceClient workspace = signIn(cfg);
        if (argv.contains("--e2e")) System.exit(E2E.run(workspace, cfg));
        Duration runFor = argv.stream().filter(a -> a.startsWith("--duration=")).findFirst()
                .map(a -> Duration.parse(a.substring("--duration=".length()))).orElse(null);

        // 1. Register what the code declares. The Data API runs each call as this SP's own Postgres role, and the
        //    registry validates each rule in the same call: ACCEPTED, or refused with a reason.
        var registry = new RegistryClient(URI.create(cfg.registryUrl()), () -> bearer(workspace), RegistryClient.jdkHttp());
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
        awaitHub(registry, accepted, sources);

        // 3. Consume this app's hub with a token UC mints from its service credential (no Azure secret here).
        UcOAuthBearerHandler.use(new EventHubsTokens(() -> eventHubsToken(workspace, cfg), Clock.systemUTC()));
        try (var consumer = new KafkaConsumer<String, String>(KafkaSettings.consumer(cfg.namespace(), cfg.groupId()))) {
            consumer.subscribe(List.of(topic));
            System.out.printf("listening   on %s as consumer group %s (Ctrl-C to stop)%n", topic, cfg.groupId());
            Set<String> seen = new HashSet<>();
            Instant stopAt = runFor == null ? Instant.MAX : Instant.now().plus(runFor);
            Instant nextCheck = Instant.now().plus(HEALTH_CHECK_EVERY);
            Instant nextSkipReport = Instant.now().plusSeconds(10);
            long older = 0;
            // The consumer group resumes where it last committed (it never skips), so after time away the app first
            // works through the backlog: counted, with progress, rather than printed line by line.
            boolean live = false;
            long replayed = 0;
            String replayFrom = null;
            String replayAt = null;
            while (Instant.now().isBefore(stopAt)) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofSeconds(1))) {
                    Notification n = Notification.parse(rec.value());
                    if (!n.isCurrent(current)) {
                        older++;
                        continue;
                    }
                    if (!seen.add(n.notificationId())) continue; // at-least-once transport: deduplicate on the stable ID
                    if (seen.size() > 100_000) seen.clear();
                    if (!live && n.isBacklog(Instant.now(), BACKLOG_AFTER)) {
                        if (replayFrom == null) {
                            replayFrom = n.goldTs();
                            System.out.printf("catching up from %s, where consumer group %s last stopped%n", replayFrom,
                                    cfg.groupId());
                        }
                        replayed++;
                        replayAt = n.goldTs();
                        continue;
                    }
                    if (!live && replayFrom != null) {
                        System.out.printf("caught up   %d backlog notifications (%s to %s); live from here%n", replayed,
                                replayFrom, replayAt);
                    }
                    live = true;
                    System.out.printf("%s  %-22s rev %d  %s%n", n.goldTs(), n.subscription(), n.revision(), n.payload());
                }
                if (Instant.now().isAfter(nextSkipReport)) {
                    if (!live && replayFrom != null) {
                        System.out.printf("catching up %d so far, at %s%n", replayed, replayAt);
                    }
                    if (older > 0) {
                        System.out.printf("skipped     %d notifications for other subscriptions or earlier revisions%n",
                                older);
                        older = 0;
                    }
                    nextSkipReport = Instant.now().plusSeconds(10);
                }
                consumer.commitSync(); // after handling: a crash replays, never skips
                if (Instant.now().isAfter(nextCheck)) {
                    for (String s : Activation.staleSources(registry.list().hub(), sources, Instant.now(), HUB_STALE_AFTER)) {
                        System.out.printf("WARNING     the hub hasn't processed %s for over %d minutes: is the "
                                + "subscription_hub pipeline running?%n", s, HUB_STALE_AFTER.toMinutes());
                    }
                    nextCheck = Instant.now().plus(HEALTH_CHECK_EVERY);
                }
            }
        }
        exit(0, "ran for " + runFor);
    }

    /**
     * With a profile: OAuth M2M from that profile only (DATABRICKS_* environment variables would otherwise take
     * precedence). Without one: the ambient identity, e.g. a workspace job's run-as service principal. Either way it
     * must be a service principal.
     */
    static WorkspaceClient signIn(SubscriberConfig cfg) {
        DatabricksConfig c = cfg.profile() == null ? new DatabricksConfig()
                : new DatabricksConfig().setProfile(cfg.profile()).setAuthType("oauth-m2m");
        WorkspaceClient workspace = new WorkspaceClient(c);
        var me = workspace.currentUser().me(new MeRequest());
        if (!SubscriberConfig.isServicePrincipal(me.getUserName())) {
            exit(5, "signed in as " + me.getUserName() + ", not a service principal"
                    + (cfg.profile() == null ? "" : ": check profile " + cfg.profile()));
        }
        System.out.printf("signed in   as %s (%s)%n", me.getDisplayName(), me.getUserName());
        return workspace;
    }

    private static void awaitHub(RegistryClient registry, Map<String, Integer> accepted, Set<String> sources)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(ACTIVATION_TIMEOUT);
        Set<String> reported = new HashSet<>();
        boolean waitingForHub = false;
        while (true) {
            var listing = registry.list();
            for (var s : listing.subscriptions()) {
                if (Integer.valueOf(s.revision()).equals(accepted.get(s.name())) && !"PENDING".equals(s.state())
                        && reported.add(s.name())) {
                    System.out.printf("hub         %-22s revision %d %s%s%n", s.name(), s.revision(), s.state(),
                            s.error() == null ? "" : ": " + s.error());
                }
            }
            var result = Activation.check(accepted, listing.subscriptions());
            if (result.state() == Activation.State.READY) {
                // ACTIVE can be left over from an earlier run: also require the hub to be processing right now. A hub
                // that just restarted reports ACTIVE before its first heartbeat, so wait (up to the deadline) for one.
                var stale = Activation.staleSources(listing.hub(), sources, Instant.now(), HUB_STALE_AFTER);
                if (stale.isEmpty()) return;
                if (!waitingForHub) {
                    System.out.printf("waiting     the hub hasn't finished a batch of %s in %d minutes; waiting up to "
                            + "%d minutes%n", stale, HUB_STALE_AFTER.toMinutes(), ACTIVATION_TIMEOUT.toMinutes());
                    waitingForHub = true;
                }
            }
            if (result.state() == Activation.State.FAILED) exit(3, "the hub could not run: " + result.problems());
            if (Instant.now().isAfter(deadline)) {
                var stale = Activation.staleSources(listing.hub(), sources, Instant.now(), HUB_STALE_AFTER);
                exit(4, stale.isEmpty()
                        ? "the hub hasn't confirmed " + result.problems() + " within " + ACTIVATION_TIMEOUT.toMinutes() + " min"
                        : "the hub isn't processing " + stale + ": start the subscription_hub pipeline (make demo dev)");
            }
            Thread.sleep(1000);
        }
    }

    static void exit(int code, String message) {
        System.out.println((code == 0 ? "done        " : "ERROR       ") + message);
        System.exit(code);
    }

    /** The SP's Databricks OAuth token (the SDK refreshes it), which the Data API maps to the SP's Postgres role. */
    static String bearer(WorkspaceClient workspace) {
        Map<String, String> headers = workspace.config().authenticate();
        return headers.get("Authorization").replaceFirst("^Bearer ", "");
    }

    static EventHubsTokens.Token eventHubsToken(WorkspaceClient workspace, SubscriberConfig cfg) {
        return eventHubsToken(workspace, cfg.credential(), cfg.namespace());
    }

    static EventHubsTokens.Token eventHubsToken(WorkspaceClient workspace, String credential, String namespace) {
        TemporaryCredentials c = workspace.credentials().generateTemporaryServiceCredential(
                new GenerateTemporaryServiceCredentialRequest()
                        .setCredentialName(credential)
                        .setAzureOptions(new GenerateTemporaryServiceCredentialAzureOptions()
                                .setResources(List.of(EventHubsTokens.scope(namespace)))));
        return new EventHubsTokens.Token(c.getAzureAad().getAadToken(), Instant.ofEpochMilli(c.getExpirationTime()));
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
