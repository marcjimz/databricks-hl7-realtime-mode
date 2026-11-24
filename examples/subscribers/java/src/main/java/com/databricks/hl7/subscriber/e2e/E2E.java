package com.databricks.hl7.subscriber.e2e;

import com.databricks.hl7.subscriber.App;
import com.databricks.hl7.subscriber.api.Api;
import com.databricks.hl7.subscriber.api.SubscriptionsApi;
import com.databricks.hl7.subscriber.api.client.ApiException;
import com.databricks.hl7.subscriber.api.model.Accepted;
import com.databricks.hl7.subscriber.api.model.Connection;
import com.databricks.hl7.subscriber.api.model.Heartbeat;
import com.databricks.hl7.subscriber.api.model.Notification;
import com.databricks.hl7.subscriber.api.model.One;
import com.databricks.hl7.subscriber.api.model.Refusal;
import com.databricks.hl7.subscriber.api.model.Subscription;
import com.databricks.hl7.subscriber.notifications.Listener;
import com.databricks.hl7.subscriber.platform.EventHubs;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.iam.MeRequest;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;

/**
 * End-to-end checks against the live Subscription API, hub, and Event Hubs, as this app's service principal: the same
 * 26 checks as the Python example's e2e.py. Every check has an input, an expected result, and the actual result; the run
 * prints them as a table and exits 1 if any fails (a skipped check doesn't fail it).
 *
 * <pre>
 *   java -jar examples/subscribers/java/target/hl7-subscriber.jar --e2e                  (local, with the SP's profile)
 *   DATABRICKS_BUNDLE_ROOT=examples/subscribers/java databricks bundle run e2e   (workspace job, run as the SP)
 * </pre>
 *
 * It only uses subscriptions named e2e-* (one at a time; every one still enabled is cancelled at the end) and its own
 * consumer group, so it leaves the app's own subscriptions alone. It runs the same whichever hub serves the app
 * (edp_shared, or a team's own): the API names it, and the heartbeat check reads that hub's. Settings (written by
 * `make registry`): e2e.forbidden_credential, e2e.forbidden_topic, e2e.own_topic, e2e.base_schema, e2e.data_api_url.
 */
public final class E2E {
    private static final Duration WAIT = Duration.ofMinutes(3);
    static final List<String> SOURCES = List.of("silver_encounters", "silver_observations", "silver_orders",
            "silver_diagnoses", "silver_documents");
    /** Every subscription name the checks register. */
    static final List<String> NAMES = List.of("e2e-potassium", "e2e-ok", "e2e-refused");

    /** The rule the checks register, and its change. */
    record Rules(String good, String changed) { }

    /**
     * Potassium by the test's name if the app may read that column, else by its LOINC code (e.g. under the
     * ed-respiratory-monitoring template).
     */
    static Rules potassium(List<String> fields) {
        String test = fields == null || fields.isEmpty() || fields.contains("observation_cd_desc")
                ? "observation_cd_desc = 'Potassium'" : "alt_observation_cd = '2823-3'";
        return new Rules("SELECT patient_id, value_num FROM silver_observations WHERE " + test + " AND value_num >= 6.5",
                "SELECT patient_id, value_num, units FROM silver_observations WHERE " + test + " AND value_num >= 7.0");
    }

    /** A silver source this app isn't granted (the first), or null if it's granted all of them. */
    static String ungrantedSource(List<String> sources) {
        return SOURCES.stream().filter(s -> sources == null || !sources.contains(s)).findFirst().orElse(null);
    }

    /** The e2e-* subscriptions still enabled. */
    static List<String> leftovers(List<Subscription> subscriptions) {
        return subscriptions.stream().filter(s -> Boolean.TRUE.equals(s.getEnabled()) && s.getName().startsWith("e2e-"))
                .map(Subscription::getName).sorted().toList();
    }

    /** When `hub` (this app's) last ran a realtime batch of `source`, or null. */
    static String heartbeat(List<Heartbeat> heartbeats, String hub, String source) {
        if (heartbeats == null) return null;
        for (Heartbeat h : heartbeats) {
            if (hub != null && hub.equals(h.getHub()) && "realtime".equals(h.getTier()) && source.equals(h.getSource())) {
                return String.valueOf(h.getLastBatchAt());
            }
        }
        return null;
    }

    /** One row of the table; result is PASS, FAIL, or SKIP. */
    record Check(String name, String input, String expected, String actual, String result) { }

    private final List<Check> checks = new ArrayList<>();
    private final WorkspaceClient workspace;
    private final SubscriptionsApi api;
    private final Connection connection;
    private final Properties cfg;

    E2E(WorkspaceClient workspace, SubscriptionsApi api, Connection connection, Properties cfg) {
        this.workspace = workspace;
        this.api = api;
        this.connection = connection;
        this.cfg = cfg;
    }

    public static int run(WorkspaceClient workspace, SubscriptionsApi api, Connection connection, Properties cfg) {
        var e2e = new E2E(workspace, api, connection, cfg);
        try {
            e2e.all();
        } catch (Exception e) {
            e2e.record("run", "-", "completes", "aborted: " + e, false);
        } finally {
            cleanup(api);
        }
        e2e.print();
        return e2e.ok() ? 0 : 1;
    }

    /**
     * Cancel every e2e-* subscription still enabled (an accepted refusal check leaves e2e-refused); if the API can't
     * list them, every name the checks use.
     */
    static void cleanup(SubscriptionsApi api) {
        List<String> names;
        try {
            names = leftovers(api.getStatus(0).getSubscriptions());
        } catch (ApiException | RuntimeException e) {
            names = NAMES;
        }
        for (String name : names) {
            try {
                api.cancel(name);
            } catch (ApiException alreadyGone) {
                // 404: nothing to clean up
            }
        }
    }

    private String setting(String key) {
        String v = cfg.getProperty(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    private void all() throws Exception {
        String me = workspace.currentUser().me(new MeRequest()).getUserName();
        record("Sign in", "profile " + cfg.getProperty("databricks.profile", "(ambient identity)"), "a service principal", me,
                App.isServicePrincipal(me));

        // Isolation on Event Hubs and the registry: only this app's own credential and hub, read-only; no way around the API.
        String own = setting("e2e.own_topic");
        record("Connection from the API", "GET /api/v1/connection", "this app's own topic and credential",
                connection.getTopic() + " via " + connection.getCredential() + " @ " + connection.getBootstrapServers(),
                (own == null || own.equals(connection.getTopic())) && connection.getCredential() != null);
        String hub = connection.getHub();
        record("This app's hub", "GET /api/v1/connection", "the hub that runs its realtime rules",
                hub == null || hub.isBlank() ? "none" : hub, hub != null && !hub.isBlank());
        String bootstrap = connection.getBootstrapServers(), scope = connection.getTokenScope();
        record("Own Event Hubs credential", connection.getCredential(), "token issued", attempt(() ->
                "token, expires " + EventHubs.mint(workspace, connection.getCredential(), scope).expiresAt()),
                s -> s.startsWith("token"));
        String forbiddenCredential = setting("e2e.forbidden_credential");
        if (forbiddenCredential != null) {
            record("Another credential", forbiddenCredential, "denied (no ACCESS)", attempt(() -> {
                EventHubs.mint(workspace, forbiddenCredential, scope);
                return "token issued";
            }), s -> s.contains("ACCESS") || s.contains("PERMISSION_DENIED") || s.contains("does not have"));
        }
        EventHubs.useCredential(workspace, connection.getCredential(), scope);
        String forbiddenTopic = setting("e2e.forbidden_topic");
        if (forbiddenTopic != null) {
            record("Read a hub that isn't this app's", forbiddenTopic, "not authorized", readOnce(forbiddenTopic, bootstrap),
                    s -> s.contains("not authorized"));
        }
        record("Write to its own hub (forge a notification)", connection.getTopic(), "not authorized (Receiver only)",
                writeOnce(connection.getTopic(), bootstrap), s -> s.contains("not authorized"));
        String dataApi = setting("e2e.data_api_url");
        if (dataApi != null) {
            record("Skip the API (the registry's Data API)", "list_subscriptions via " + dataApi, "not available",
                    attempt(() -> {
                        var http = java.net.http.HttpClient.newHttpClient();
                        var r = http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(
                                        dataApi + "/subscription_api/rpc/list_subscriptions"))
                                .header("Authorization", "Bearer " + App.bearer(workspace)).GET().build(),
                                java.net.http.HttpResponse.BodyHandlers.ofString());
                        return r.statusCode() / 100 == 2 ? "answered " + r.statusCode() : "refused " + r.statusCode();
                    }), s -> !s.startsWith("answered"));
        }

        // Refused in the declare call itself: Databricks' own errors, or a shape the hub won't run.
        refused("Column not granted", "SELECT patient_id FROM silver_observations WHERE msg_sending_facility = 'NORTH'",
                "UNRESOLVED_COLUMN");
        String source = ungrantedSource(connection.getSources());
        if (source != null) {
            refused("Source not granted", "SELECT patient_id FROM " + source, "TABLE_OR_VIEW_NOT_FOUND");
        } else {
            skip("Source not granted", "a source this app isn't granted", "TABLE_OR_VIEW_NOT_FOUND",
                    "this app is granted every source");
        }
        refused("Misspelled column", "SELECT patient_id FROM silver_observations WHERE value_numm > 6", "UNRESOLVED_COLUMN");
        refused("Syntax error", "SELECT patient_id FROM silver_observations WHER value_num > 6", "PARSE_SYNTAX_ERROR");
        refused("Subquery", "SELECT patient_id FROM silver_observations WHERE value_num > "
                + "(SELECT max(value_num) FROM silver_observations)", "SQL_NOT_ALLOWED");
        refused("Join", "SELECT o.patient_id FROM silver_observations o JOIN silver_encounters e ON o.patient_id = e.patient_id",
                "SQL_NOT_ALLOWED");
        refused("Function that reaches outside the record",
                "SELECT patient_id FROM silver_observations WHERE http_request('GET', 'https://example.com') IS NOT NULL",
                "SQL_NOT_ALLOWED");
        String base = setting("e2e.base_schema");
        if (base != null) {
            refused("Table outside this app's views", "SELECT patient_id FROM " + base + ".silver_observations",
                    "SQL_NOT_ALLOWED");
        }
        Rules rules = potassium(connection.getFields());
        String good = rules.good(), changed = rules.changed();
        var mixed = new LinkedHashMap<String, String>();
        mixed.put("e2e-ok", good);
        mixed.put("e2e-bad", "SELECT patient_id FROM silver_observations WHERE value_numm > 6");
        int atomic;
        try {
            api.declare(Api.declared(mixed));
            atomic = 200;
        } catch (ApiException e) {
            atomic = e.getCode();
        }
        record("One refusal applies nothing", "declare e2e-ok (valid) + e2e-bad", "422, e2e-ok not registered",
                atomic + ", e2e-ok " + (registered("e2e-ok") ? "registered" : "not registered"),
                atomic == 422 && !registered("e2e-ok"));
        boolean valid = Boolean.TRUE.equals(api.validate(Api.declared(Map.of("e2e-ok", good))).getValid());
        record("Validate applies nothing", "validate e2e-ok", "valid, not registered",
                (valid ? "valid" : "invalid") + ", " + (registered("e2e-ok") ? "registered" : "not registered"),
                valid && !registered("e2e-ok"));

        // Register, receive, change, receive the change, cancel.
        String group = cfg.getProperty("databricks.profile", "subscriber") + "-e2e-" + UUID.randomUUID();
        try (var consumer = new KafkaConsumer<String, String>(EventHubs.consumer(bootstrap, group))) {
            Accepted r1 = api.putSubscription("e2e-potassium", new One().sql(good));
            record("Register", good, "ACCEPTED, canonical SQL", r1.getState() + " revision " + r1.getRevision() + ": "
                    + r1.getQuery(), r1.getState() == Accepted.StateEnum.ACCEPTED);
            record("Hub runs it", "e2e-potassium revision " + r1.getRevision(), "ACTIVE",
                    awaitState(r1.getSubscriptionId(), r1.getRevision(), "ACTIVE"), s -> s.equals("ACTIVE"));
            consumer.subscribe(List.of(r1.getDestination()));
            var n1 = awaitNotification(consumer, r1);
            record("Notification on this app's hub", r1.getDestination(),
                    "payload {patient_id, value_num}, value_num >= 6.5", describe(n1),
                    n1 != null && keys(n1).equals(Set.of("patient_id", "value_num")) && number(n1, "value_num") >= 6.5);

            Accepted again = api.putSubscription("e2e-potassium", new One().sql(good));
            record("Same SQL again", good, "same revision, unchanged", "revision " + again.getRevision()
                    + (Boolean.TRUE.equals(again.getUnchanged()) ? ", unchanged" : ", changed"),
                    again.getRevision().equals(r1.getRevision()) && Boolean.TRUE.equals(again.getUnchanged()));

            Accepted r2 = api.putSubscription("e2e-potassium", new One().sql(changed));
            record("Change the rule", changed, "next revision, then ACTIVE", "revision " + r2.getRevision()
                    + ", then " + awaitState(r2.getSubscriptionId(), r2.getRevision(), "ACTIVE"),
                    r2.getRevision() == r1.getRevision() + 1);
            var n2 = awaitNotification(consumer, r2);
            record("Notification reflects the change", r2.getDestination(),
                    "revision " + r2.getRevision() + ", payload {patient_id, value_num, units}, value_num >= 7.0",
                    describe(n2), n2 != null && keys(n2).equals(Set.of("patient_id", "value_num", "units"))
                            && number(n2, "value_num") >= 7.0);

            var cancel = api.cancel("e2e-potassium");
            record("Cancel", "DELETE e2e-potassium", "CANCELLED by the hub",
                    awaitState(cancel.getSubscriptionId(), cancel.getRevision(), "CANCELLED"), s -> s.equals("CANCELLED"));
        }
        String last = heartbeat(api.getStatus(0).getHeartbeats(), hub, "silver_observations");
        record("Hub heartbeat", hub + ": silver_observations", "a batch within 2 minutes", last == null ? "never" : last,
                s -> !s.equals("never") && OffsetDateTime.parse(s).toInstant().isAfter(Instant.now().minusSeconds(120)));
    }

    private void refused(String name, String sql, String code) {
        String actual;
        try {
            api.putSubscription("e2e-refused", new One().sql(sql));
            actual = "ACCEPTED";
        } catch (ApiException e) {
            List<Refusal> refusals = Api.refusals(e);
            Refusal first = refusals.isEmpty() ? Api.problem(e) : refusals.get(0);
            actual = first.getCode() + ": " + App.firstLine(first.getMessage());
        }
        record(name, sql, code, actual, actual.startsWith(code));
    }

    private boolean registered(String name) throws ApiException {
        for (Subscription s : api.getStatus(0).getSubscriptions()) {
            if (s.getName().equals(name) && Boolean.TRUE.equals(s.getEnabled())) return true;
        }
        return false;
    }

    private String awaitState(String subscriptionId, int revision, String wanted) throws Exception {
        Instant deadline = Instant.now().plus(WAIT);
        String last = "PENDING";
        while (Instant.now().isBefore(deadline)) {
            for (Subscription s : api.getStatus(0).getSubscriptions()) {
                if (s.getSubscriptionId().equals(subscriptionId) && s.getRevision() == revision) last = s.getState();
            }
            if (last.equals(wanted) || last.equals("FAILED")) return last;
            Thread.sleep(1000);
        }
        return last + " after " + WAIT.toMinutes() + " min";
    }

    private static Notification awaitNotification(KafkaConsumer<String, String> consumer, Accepted receipt) {
        Instant deadline = Instant.now().plus(WAIT);
        var current = Map.of(receipt.getSubscriptionId(), receipt.getRevision());
        while (Instant.now().isBefore(deadline)) {
            for (var rec : consumer.poll(Duration.ofSeconds(1))) {
                Notification n = Listener.parse(rec.value());
                if (Listener.isCurrent(n, current)) return n;
            }
        }
        return null;
    }

    private String readOnce(String topic, String bootstrap) {
        try (var consumer = new KafkaConsumer<String, String>(EventHubs.consumer(bootstrap,
                "e2e-forbidden-" + UUID.randomUUID()))) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plusSeconds(60);
            while (Instant.now().isBefore(deadline)) {
                if (!consumer.poll(Duration.ofSeconds(2)).isEmpty()) return "read records";
            }
            return "no error and no records in 60 s";
        } catch (AuthorizationException e) {
            return "not authorized: " + e.getClass().getSimpleName();
        } catch (Exception e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private String writeOnce(String topic, String bootstrap) {
        try (var producer = new KafkaProducer<String, String>(EventHubs.producer(bootstrap))) {
            producer.send(new ProducerRecord<>(topic, "e2e", "{\"forged\":true}")).get(45, java.util.concurrent.TimeUnit.SECONDS);
            return "written";
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable c = e.getCause();
            return c instanceof AuthorizationException ? "not authorized: " + c.getClass().getSimpleName()
                    : c.getClass().getSimpleName() + ": " + c.getMessage();
        } catch (AuthorizationException e) {
            return "not authorized: " + e.getClass().getSimpleName();
        } catch (Exception e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private interface Attempt {
        String call() throws Exception;
    }

    private static String attempt(Attempt a) {
        try {
            return a.call();
        } catch (Exception e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private void record(String name, String input, String expected, String actual, Predicate<String> pass) {
        record(name, input, expected, actual, pass.test(actual));
    }

    void record(String name, String input, String expected, String actual, boolean pass) {
        add(new Check(name, input, expected, actual, pass ? "PASS" : "FAIL"));
    }

    /** A check that doesn't apply to this app (e.g. it's granted every source): shown, never a failure. */
    void skip(String name, String input, String expected, String reason) {
        add(new Check(name, input, expected, reason, "SKIP"));
    }

    private void add(Check c) {
        checks.add(c);
        System.out.printf("%s  %s%n", c.result(), c.name());
    }

    boolean ok() {
        return checks.stream().noneMatch(c -> c.result().equals("FAIL"));
    }

    private static Set<String> keys(Notification n) {
        return new TreeSet<>(n.getPayload().keySet());
    }

    private static double number(Notification n, String field) {
        return n.getPayload().get(field) instanceof Number v ? v.doubleValue() : Double.NaN;
    }

    private static String describe(Notification n) {
        return n == null ? "none within " + WAIT.toMinutes() + " min" : "revision " + n.getRevision() + " " + Listener.payloadJson(n);
    }

    void print() {
        System.out.println();
        System.out.println("| # | Check | Input | Expected | Actual | Result |");
        System.out.println("|---|---|---|---|---|---|");
        int i = 1;
        for (Check c : checks) {
            System.out.printf("| %d | %s | %s | %s | %s | %s |%n", i++, c.name(), cell(c.input()), cell(c.expected()),
                    cell(c.actual()), c.result().equals("FAIL") ? "**FAIL**" : c.result());
        }
        long failed = checks.stream().filter(c -> c.result().equals("FAIL")).count();
        long skipped = checks.stream().filter(c -> c.result().equals("SKIP")).count();
        System.out.printf("%n%d checks, %d failed%s%n", checks.size(), failed, skipped > 0 ? ", " + skipped + " skipped" : "");
    }

    private static String cell(String s) {
        String t = s == null ? "" : s.replace("|", "\\|").replace("\n", " ");
        return t.length() > 160 ? t.substring(0, 157) + "..." : t;
    }
}
