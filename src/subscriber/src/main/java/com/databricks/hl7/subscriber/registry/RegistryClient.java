package com.databricks.hl7.subscriber.registry;

import com.databricks.hl7.subscriber.rules.Subscription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The hub's subscription registry: PostgreSQL functions in schema subscription_api, called over the Lakebase Data API as
 * this app's own identity. The database derives the owner from the token; no request names an owner, topic, or credential.
 */
public final class RegistryClient {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    // jsonb timestamptz: 2026-11-03T12:00:00.123456+00:00
    private static final DateTimeFormatter POSTGRES_TIMESTAMP = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME).appendOffset("+HH:MM", "Z").toFormatter();

    public record Response(int status, String body) { }

    /** HTTP transport, replaceable in tests. */
    public interface Http {
        Response send(HttpRequest request) throws IOException, InterruptedException;
    }

    /** The registry's answer to subscribe: ACCEPTED (validated) with the rule rendered as SQL for people to read. */
    public record Receipt(String subscriptionId, String name, int revision, String destination, String state,
            String predicateSql) { }

    /** state: PENDING (the hub hasn't run this revision yet), ACTIVE, CANCELLED, or FAILED (with error). */
    public record Status(String subscriptionId, String name, int revision, boolean enabled, String source,
            String predicateSql, String state, String error) { }

    /** This app's subscriptions, and when the hub last finished a batch per source. */
    public record Listing(List<Status> subscriptions, Map<String, Instant> hub) { }

    public static final class RegistryException extends RuntimeException {
        private final int status;
        private final String code;

        RegistryException(int status, String code, String message) {
            super(message);
            this.status = status;
            this.code = code;
        }

        public int status() { return status; }

        /** Domain code raised by the registry (e.g. FIELDS_DENIED), else the SQLSTATE. */
        public String code() { return code; }
    }

    private final URI api;
    private final Supplier<String> bearer;
    private final Http http;

    public RegistryClient(URI dataApiBase, Supplier<String> bearer, Http http) {
        if (!"https".equalsIgnoreCase(dataApiBase.getScheme()) || dataApiBase.getHost() == null) {
            throw new IllegalArgumentException("Use the HTTPS Data API URL");
        }
        this.api = URI.create(dataApiBase.toString().replaceAll("/+$", "") + "/subscription_api/rpc/");
        this.bearer = Objects.requireNonNull(bearer);
        this.http = Objects.requireNonNull(http);
    }

    /** The JDK HTTP client, no redirects. */
    public static Http jdkHttp() {
        HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).followRedirects(HttpClient.Redirect.NEVER).build();
        return request -> {
            HttpResponse<String> r = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return new Response(r.statusCode(), r.body());
        };
    }

    public Receipt subscribe(Subscription s) {
        return subscribe(s.name(), s.source(), s.where().toJson(), s.fields());
    }

    /** Sends any predicate JSON as-is (the E2E checks use it to send what the builder can't, such as SQL text). */
    public Receipt subscribe(String name, String source, JsonNode predicate, List<String> fields) {
        ObjectNode body = JSON.createObjectNode().put("p_name", name).put("p_source", source);
        body.set("p_predicate", predicate);
        body.putArray("p_fields").addAll(fields.stream().map(JSON.getNodeFactory()::textNode).toList());
        JsonNode r = call("subscribe", body);
        return new Receipt(r.path("subscriptionId").asText(), r.path("name").asText(), r.path("revision").asInt(),
                r.path("destination").asText(), r.path("state").asText(), r.path("predicateSql").asText());
    }

    public void unsubscribe(String name) {
        call("unsubscribe", JSON.createObjectNode().put("p_name", name));
    }

    public Listing list() {
        JsonNode r = call("list_subscriptions", null);
        List<Status> subs = new ArrayList<>();
        for (JsonNode n : r.path("subscriptions")) {
            subs.add(new Status(n.path("subscriptionId").asText(), n.path("name").asText(), n.path("revision").asInt(),
                    n.path("enabled").asBoolean(), n.path("source").asText(), n.path("predicateSql").asText(),
                    n.path("state").asText(), n.path("error").isNull() ? null : n.path("error").asText(null)));
        }
        Map<String, Instant> hub = new HashMap<>();
        r.path("hub").fields().forEachRemaining(e -> hub.put(e.getKey(),
                OffsetDateTime.parse(e.getValue().asText(), POSTGRES_TIMESTAMP).toInstant()));
        return new Listing(subs, hub);
    }

    private JsonNode call(String rpc, ObjectNode body) {
        HttpRequest.Builder b = HttpRequest.newBuilder(api.resolve(rpc)).timeout(TIMEOUT)
                .header("Authorization", "Bearer " + bearer.get())
                .header("Accept", "application/json");
        b = body == null ? b.GET()
                : b.header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
        Response r;
        try {
            r = http.send(b.build());
        } catch (IOException e) {
            throw new RegistryException(0, "TRANSPORT", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RegistryException(0, "INTERRUPTED", "interrupted");
        }
        JsonNode json = parse(r.body());
        if (r.status() / 100 != 2) {
            String message = json.path("message").asText(r.body());
            String code = message.matches("^[A-Z_]+(:.*)?$") ? message.split(":", 2)[0] : json.path("code").asText("HTTP");
            throw new RegistryException(r.status(), code, message);
        }
        return json;
    }

    private static JsonNode parse(String body) {
        try {
            return body == null || body.isBlank() ? JSON.createObjectNode() : JSON.readTree(body);
        } catch (IOException e) {
            return JSON.createObjectNode().put("message", body);
        }
    }
}
