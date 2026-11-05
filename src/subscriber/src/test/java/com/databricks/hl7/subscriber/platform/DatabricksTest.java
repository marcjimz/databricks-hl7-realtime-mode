package com.databricks.hl7.subscriber.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class DatabricksTest {

    @Test
    void onlyAServicePrincipalMayRunTheApp() {
        assertEquals(true, Databricks.isServicePrincipal("38fafc55-d28b-4f48-8d17-299f8d2698ee"));
        assertEquals(false, Databricks.isServicePrincipal("someone@example.com"));
        assertEquals(false, Databricks.isServicePrincipal(null));
    }
}
