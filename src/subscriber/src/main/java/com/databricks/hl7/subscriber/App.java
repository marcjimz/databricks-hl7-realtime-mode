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

    public static void main(String[] args) throws Exception {
        SubscriberConfig cfg = SubscriberConfig.from(load(Path.of(args.length > 0 ? args[0] : "subscriber.properties")));
        // OAuth M2M from the profile only: DATABRICKS_* environment variables would otherwise take precedence.
        WorkspaceClient workspace = new WorkspaceClient(
                new DatabricksConfig().setProfile(cfg.profile()).setAuthType("oauth-m2m"));
        var me = workspace.currentUser().me(new MeRequest());
        if (!SubscriberConfig.isServicePrincipal(me.getUserName())) {
            exit(5, "signed in as " + me.getUserName() + ", not a service principal: check profile " + cfg.profile());
        }
        System.out.printf("signed in   as %s (%s)%n", me.getDisplayName(), me.getUserName());

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
        for (Subscription s : plan.subscribe()) {
            try {
                var r = registry.subscribe(s);
                accepted.put(r.name(), r.revision());
                current.put(r.subscriptionId(), r.revision());
                topic = r.destination();
                System.out.printf("subscribed  %-22s revision %d %s%n            %s%n", r.name(), r.revision(), r.state(),
                        r.predicateSql());
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
            Instant nextCheck = Instant.now().plus(HEALTH_CHECK_EVERY);
            Instant nextSkipReport = Instant.now().plusSeconds(10);
            long older = 0;
            while (true) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofSeconds(1))) {
                    Notification n = Notification.parse(rec.value());
                    if (!n.isCurrent(current)) {
                        older++;
                    } else if (seen.add(n.notificationId())) { // at-least-once transport: deduplicate on the stable ID
                        System.out.printf("%s  %-22s rev %d  %s%n", n.goldTs(), n.subscription(), n.revision(), n.payload());
                    }
                    if (seen.size() > 100_000) seen.clear();
                }
                if (older > 0 && Instant.now().isAfter(nextSkipReport)) {
                    System.out.printf("skipped     %d notifications for earlier revisions (backlog on the hub)%n", older);
                    older = 0;
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
    }

    private static void awaitHub(RegistryClient registry, Map<String, Integer> accepted, Set<String> sources)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(ACTIVATION_TIMEOUT);
        Set<String> reported = new HashSet<>();
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
                // ACTIVE can be left over from an earlier run: also require the hub to be processing right now.
                var stale = Activation.staleSources(listing.hub(), sources, Instant.now(), HUB_STALE_AFTER);
                if (stale.isEmpty()) return;
                exit(4, "the hub isn't processing " + stale + ": start the subscription_hub pipeline (make demo dev)");
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

    private static void exit(int code, String message) {
        System.out.println((code == 0 ? "done        " : "ERROR       ") + message);
        System.exit(code);
    }

    /** The SP's Databricks OAuth token (the SDK refreshes it), which the Data API maps to the SP's Postgres role. */
    private static String bearer(WorkspaceClient workspace) {
        Map<String, String> headers = workspace.config().authenticate();
        return headers.get("Authorization").replaceFirst("^Bearer ", "");
    }

    private static EventHubsTokens.Token eventHubsToken(WorkspaceClient workspace, SubscriberConfig cfg) {
        TemporaryCredentials c = workspace.credentials().generateTemporaryServiceCredential(
                new GenerateTemporaryServiceCredentialRequest()
                        .setCredentialName(cfg.credential())
                        .setAzureOptions(new GenerateTemporaryServiceCredentialAzureOptions()
                                .setResources(List.of(EventHubsTokens.scope(cfg.namespace())))));
        return new EventHubsTokens.Token(c.getAzureAad().getAadToken(), Instant.ofEpochMilli(c.getExpirationTime()));
    }

    private static Properties load(Path file) throws IOException {
        if (!Files.exists(file)) {
            throw new IllegalArgumentException(file + " not found: cp subscriber.properties.example subscriber.properties");
        }
        var p = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            p.load(in);
        }
        return p;
    }
}
