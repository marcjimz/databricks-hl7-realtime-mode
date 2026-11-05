package com.databricks.hl7.subscriber.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The Subscription API (a Databricks App): the one way to register subscriptions. Called with this app's own service
 * principal token; the API knows the caller from that, never from the request. Every answer is JSON, including
 * Databricks' own error for a rule it can't compile.
 */
public final class SubscriptionApi {
    private static final ObjectMapper JSON = new ObjectMapper();

    public record Response(int status, JsonNode body) {
        public boolean ok() { return status / 100 == 2; }
    }

    private final URI base;
    private final Supplier<String> bearer;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NEVER).build();

    public SubscriptionApi(String url, Supplier<String> bearer) {
        URI u = URI.create(url.replaceAll("/+$", ""));
        if (!"https".equalsIgnoreCase(u.getScheme())) throw new IllegalArgumentException("use the app's HTTPS URL");
        this.base = u;
        this.bearer = bearer;
    }

    /** The full set this app wants (name -> SQL): atomic, and cancels what isn't declared any more. */
    public Response declare(Map<String, String> subscriptions) {
        return send("PUT", "/api/v1/subscriptions", items(subscriptions));
    }

    public Response validate(Map<String, String> subscriptions) {
        return send("POST", "/api/v1/validate", items(subscriptions));
    }

    public Response put(String name, String sql) {
        return send("PUT", "/api/v1/subscriptions/" + enc(name), JSON.createObjectNode().put("sql", sql));
    }

    public Response cancel(String name) {
        return send("DELETE", "/api/v1/subscriptions/" + enc(name), null);
    }

    /** This app's subscriptions and the hub's state; with waitSeconds > 0, until READY or FAILED (or the wait ends). */
    public Response status(int waitSeconds) {
        return send("GET", "/api/v1/subscriptions?wait=" + waitSeconds, null);
    }

    private static ObjectNode items(Map<String, String> subscriptions) {
        ObjectNode body = JSON.createObjectNode();
        var list = body.putArray("subscriptions");
        subscriptions.forEach((name, sql) -> list.addObject().put("name", name).put("sql", sql));
        return body;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private Response send(String method, String path, JsonNode body) {
        var request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofMinutes(6))
                .header("Authorization", "Bearer " + bearer.get()).header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8))
                .build();
        try {
            HttpResponse<String> r = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            JsonNode json;
            try {
                json = r.body().isBlank() ? JSON.createObjectNode() : JSON.readTree(r.body());
            } catch (IOException e) {
                json = JSON.createObjectNode().put("code", "HTTP_" + r.statusCode()).put("message", r.body());
            }
            return new Response(r.statusCode(), json);
        } catch (IOException e) {
            throw new IllegalStateException("Subscription API unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }
}
