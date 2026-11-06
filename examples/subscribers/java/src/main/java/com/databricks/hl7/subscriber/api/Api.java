package com.databricks.hl7.subscriber.api;

import com.databricks.hl7.subscriber.api.client.ApiClient;
import com.databricks.hl7.subscriber.api.client.ApiException;
import com.databricks.hl7.subscriber.api.client.JSON;
import com.databricks.hl7.subscriber.api.model.Declared;
import com.databricks.hl7.subscriber.api.model.Error;
import com.databricks.hl7.subscriber.api.model.Item;
import com.databricks.hl7.subscriber.api.model.Refusal;
import com.databricks.hl7.subscriber.api.model.RefusedResult;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The Subscription API is the platform's contract (docs/api/openapi.json). Maven generates its client from that file at
 * build time: {@link SubscriptionsApi}, with one method per operation (declare, getStatus, putSubscription, cancel,
 * validate), and api.model, with one class per request and response. This class only wires the client up: the API's
 * URL, the service principal's token on every call, and reading the error body of a refused call.
 */
public final class Api {
    private Api() { }

    /** A client for the API at {@code url}, sending a fresh {@code bearer} token (the SP's OAuth token) on every call. */
    public static SubscriptionsApi connect(String url, Supplier<String> bearer) {
        if (!"https".equals(URI.create(url).getScheme())) {
            throw new IllegalArgumentException("api.url must be https: " + url);
        }
        var client = new ApiClient();
        client.updateBaseUri(url.replaceAll("/+$", ""));
        client.setRequestInterceptor(request -> request.header("Authorization", "Bearer " + bearer.get()));
        client.setReadTimeout(Duration.ofSeconds(90)); // a status call waits at most 60 s on the server
        return new SubscriptionsApi(client);
    }

    /** The request body for a declare or validate call: name -> SQL, in order. */
    public static Declared declared(Map<String, String> subscriptions) {
        var d = new Declared();
        subscriptions.forEach((name, sql) -> d.addSubscriptionsItem(new Item().name(name).sql(sql)));
        return d;
    }

    /** A 422's refusals: each rule's Databricks error (or ours). Empty for any other failure. */
    public static List<Refusal> refusals(ApiException e) {
        if (e.getCode() != 422) return List.of();
        RefusedResult r = read(e, RefusedResult.class);
        return r == null || r.getRefused() == null ? List.of(problem(e)) : r.getRefused();
    }

    /** A failed call as the contract's {code, message}; HTTP_<status> when the body isn't one. */
    public static Refusal problem(ApiException e) {
        Error err = read(e, Error.class);
        return err != null && err.getCode() != null ? new Refusal().code(err.getCode()).message(err.getMessage())
                : new Refusal().code("HTTP_" + e.getCode()).message(String.valueOf(e.getResponseBody()));
    }

    private static <T> T read(ApiException e, Class<T> type) {
        try {
            return e.getResponseBody() == null ? null : JSON.getDefault().getMapper().readValue(e.getResponseBody(), type);
        } catch (IOException notThatShape) {
            return null;
        }
    }
}
