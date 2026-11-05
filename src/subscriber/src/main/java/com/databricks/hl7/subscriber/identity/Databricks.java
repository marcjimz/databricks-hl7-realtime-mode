package com.databricks.hl7.subscriber.identity;

import com.databricks.hl7.subscriber.config.SubscriberConfig;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.iam.MeRequest;
import java.util.Map;

/**
 * Who the app is: its Databricks service principal. Every call the app makes (the registry's Data API, Unity Catalog's
 * Event Hubs token) carries this identity, and the platform decides what it may do from that alone.
 */
public final class Databricks {
    private Databricks() { }

    /** The signed-in workspace client and who it is. */
    public record SignedIn(WorkspaceClient workspace, String userName, String displayName) {
        public boolean isServicePrincipal() {
            return Databricks.isServicePrincipal(userName);
        }
    }

    /**
     * With a profile: OAuth M2M from that profile only (DATABRICKS_* environment variables would otherwise take
     * precedence). Without one: the ambient identity, e.g. a workspace job's run-as service principal. The caller checks
     * {@link SignedIn#isServicePrincipal()}.
     */
    public static SignedIn signIn(SubscriberConfig cfg) {
        DatabricksConfig c = cfg.profile() == null ? new DatabricksConfig()
                : new DatabricksConfig().setProfile(cfg.profile()).setAuthType("oauth-m2m");
        WorkspaceClient workspace = new WorkspaceClient(c);
        var me = workspace.currentUser().me(new MeRequest());
        return new SignedIn(workspace, me.getUserName(), me.getDisplayName());
    }

    /** The SP's Databricks OAuth token (the SDK refreshes it), which the Data API maps to the SP's Postgres role. */
    public static String bearer(WorkspaceClient workspace) {
        Map<String, String> headers = workspace.config().authenticate();
        return headers.get("Authorization").replaceFirst("^Bearer ", "");
    }

    /** A service principal's Databricks user name is its application ID (a UUID); a person's is an email. */
    public static boolean isServicePrincipal(String userName) {
        return userName != null && userName.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }
}
