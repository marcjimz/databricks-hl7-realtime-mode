package com.databricks.hl7.subscriber.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.hl7.subscriber.rules.Predicate;
import com.databricks.hl7.subscriber.rules.Subscription;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class RegistryClientTest {

    static final URI BASE = URI.create("https://ep-1.database.eastus2.azuredatabricks.net/api/2.0/workspace/1/rest/db");
    static final ObjectMapper JSON = new ObjectMapper();

    /** Records requests and replies with a canned status and body. */
    static final class FakeHttp implements RegistryClient.Http {
        final List<HttpRequest> requests = new ArrayList<>();
        int status = 200;
        String body = "{}";

        @Override
        public RegistryClient.Response send(HttpRequest request) {
            requests.add(request);
            return new RegistryClient.Response(status, body);
        }
    }

    static String bodyOf(HttpRequest request) {
        var out = new StringBuilder();
        request.bodyPublisher().orElseThrow().subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
            @Override public void onNext(ByteBuffer b) { out.append(java.nio.charset.StandardCharsets.UTF_8.decode(b)); }
            @Override public void onError(Throwable t) { throw new RuntimeException(t); }
            @Override public void onComplete() { }
        });
        return out.toString();
    }

    @Test
    void subscribeCallsTheRpcWithTheCallersTokenAndNoOwner() throws Exception {
        var http = new FakeHttp();
        http.body = "{\"subscriptionId\":\"11111111-1111-4111-8111-111111111111\",\"name\":\"k\",\"revision\":2,"
                + "\"destination\":\"cat.subscribers.app-a\",\"state\":\"ACCEPTED\",\"predicateSql\":\"value_num > 6\"}";
        var client = new RegistryClient(BASE, () -> "db-token", http);

        var receipt = client.subscribe(
                new Subscription("k", "gold_observations", Predicate.field("value_num").gt(6), List.of("patient_id")));

        HttpRequest r = http.requests.get(0);
        assertEquals("POST", r.method());
        assertEquals(BASE + "/subscription_api/rpc/subscribe", r.uri().toString());
        assertEquals("Bearer db-token", r.headers().firstValue("Authorization").orElseThrow());
        var sent = JSON.readTree(bodyOf(r));
        assertEquals("k", sent.get("p_name").asText());
        assertEquals("gold_observations", sent.get("p_source").asText());
        assertEquals("{\"field\":\"value_num\",\"op\":\"gt\",\"value\":6}", sent.get("p_predicate").toString(),
                "the predicate is structured JSON, not SQL text");
        assertEquals("patient_id", sent.get("p_fields").get(0).asText());
        assertTrue(sent.findValue("owner") == null, "the server derives the owner from the token");
        assertEquals(2, receipt.revision());
        assertEquals("ACCEPTED", receipt.state());
        assertEquals("value_num > 6", receipt.predicateSql());
        assertEquals("cat.subscribers.app-a", receipt.destination());
    }

    @Test
    void listParsesStatusIncludingRejections() throws Exception {
        var http = new FakeHttp();
        http.body = "{\"subscriptions\":[{\"subscriptionId\":\"11111111-1111-4111-8111-111111111111\",\"name\":\"bad\","
                + "\"revision\":1,\"enabled\":true,\"source\":\"gold_observations\",\"predicateSql\":\"x\","
                + "\"state\":\"FAILED\",\"error\":\"FIELD_NOT_IN_SOURCE: x\"}],"
                + "\"hub\":{\"gold_observations\":\"2026-11-03T12:00:00.123456+00:00\"}}";
        var client = new RegistryClient(BASE, () -> "db-token", http);

        var listing = client.list();
        var list = listing.subscriptions();

        assertEquals("GET", http.requests.get(0).method(), "a STABLE read-only RPC");
        assertEquals(BASE + "/subscription_api/rpc/list_subscriptions", http.requests.get(0).uri().toString());
        assertEquals(1, list.size());
        assertEquals("FAILED", list.get(0).state());
        assertEquals("FIELD_NOT_IN_SOURCE: x", list.get(0).error());
        assertEquals(java.time.Instant.parse("2026-11-03T12:00:00.123456Z"), listing.hub().get("gold_observations"));
    }

    @Test
    void errorsCarryTheDatabaseDomainCode() {
        var http = new FakeHttp();
        http.status = 403;
        http.body = "{\"code\":\"42501\",\"message\":\"FIELDS_DENIED: {ssn}\"}";
        var client = new RegistryClient(BASE, () -> "db-token", http);

        var e = assertThrows(RegistryClient.RegistryException.class,
                () -> client.subscribe(new Subscription("k", "gold_observations", Predicate.field("ssn").isNotNull(),
                        List.of("ssn"))));
        assertEquals("FIELDS_DENIED", e.code());
        assertEquals(403, e.status());
    }

    @Test
    void refusesAnythingButHttps() {
        assertThrows(IllegalArgumentException.class,
                () -> new RegistryClient(URI.create("http://example.com/rest/db"), () -> "t", new FakeHttp()));
    }
}
