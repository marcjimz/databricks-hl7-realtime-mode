package com.databricks.hl7.subscriber.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class UcOAuthBearerHandlerTest {

    @AfterEach
    void reset() {
        UcOAuthBearerHandler.use(null);
    }

    @Test
    void answersKafkaWithTheCurrentUcToken() throws Exception {
        Instant expiry = Instant.parse("2026-11-03T13:00:00Z");
        Clock clock = Clock.fixed(Instant.parse("2026-11-03T12:00:00Z"), ZoneOffset.UTC);
        UcOAuthBearerHandler.use(new EventHubsTokens(() -> new EventHubsTokens.Token("aad-token", expiry), clock));

        var handler = new UcOAuthBearerHandler();
        handler.configure(Map.of(), "OAUTHBEARER", List.of());
        var callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {callback});

        assertNotNull(callback.token());
        assertEquals("aad-token", callback.token().value());
        assertEquals(expiry.toEpochMilli(), callback.token().lifetimeMs());
    }
}
