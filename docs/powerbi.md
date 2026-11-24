# Live Power BI dashboard

A Power BI report that shows the demo live: per-hop latency, clinical activity, and subscriptions. It re-queries
Databricks every 5 seconds while it's open. The data stays in Unity Catalog: Power BI reads it with **DirectQuery**
through the SQL warehouse and imports nothing.

```
Databricks workspace (its own tenant)                     Power BI / Fabric (any tenant)
Delta mirror tables + registry ─► <catalog>.powerbi views ◄── DirectQuery, every 5 s ── semantic model ◄─ report
                                   (SQL warehouse)            (Azure Databricks connector)  workspace hl7-realtime
                                                                                            on our own F2 capacity
```

Everything is in the repo, and nothing is built by hand in Power BI Desktop:

| Part | Where |
|---|---|
| Views: small, pre-aggregated, a short window each | `src/dashboards/powerbi/views.sql` |
| Model (one DirectQuery table per view, measures) and report (pages, visuals, refresh) | `src/dashboards/powerbi/definition.py`, rendered to TMDL and PBIR |
| Fabric capacity | `infra/fabric` (Terraform; its own tenant and subscription) |
| Deploy: views, the read-only SP's grants, workspace, model, report | `infra/powerbi.py` (`make powerbi`) |

## Set up (once)

1. **Capacity:** `cp infra/fabric/terraform.tfvars.example infra/fabric/terraform.tfvars`, fill in the Fabric tenant,
   a subscription in it, and yourself as admin. Then `az login --tenant <Fabric tenant>` and `make fabric-up` (plan →
   you confirm → apply). F2 bills while it's running.
2. **Read-only service principal**, in the Databricks workspace (needs workspace admin):

   ```bash
   databricks service-principals create --display-name sp-hl7-powerbi
   databricks service-principal-secrets-proxy create <its id>   # keep the secret in ~/.databrickscfg, never in git
   ```

3. **`make powerbi`.** It creates the views, gives the SP the `databricks-sql-access` entitlement, USE CATALOG, USE SCHEMA and
   SELECT on `<catalog>.powerbi` only, and CAN_USE on the warehouse, then creates or updates the workspace, model, and
   report. It prints their URLs. Rerun it after any change to the definitions: items are updated in place.
4. **Credentials:** open the model's settings → **Data source credentials** → Edit, and sign in. To use the SP, choose
   **Databricks Client Credentials** with its client ID and secret. This works across tenants: Entra SSO doesn't.
5. **Allow a 5 s refresh:** Admin portal → Capacity settings → Fabric capacity → your capacity → **Power BI workloads**
   → **Automatic page refresh**: set the minimum refresh interval to 1 second. A new capacity starts at **5 minutes**,
   and the report is capped at that minimum. No public API sets it (not Power BI REST workloads, not ARM), but it
   stays with the capacity through pause and resume.

`make fabric-pause` stops Fabric billing, `make fabric-resume` starts it again, and `make fabric-down` destroys the
capacity (Terraform asks first; a new one needs step 5 again).

## How "live" works

- **Automatic page refresh, fixed 5 s interval** (`definition.REFRESH_SECONDS`). Each page re-runs its visuals' queries
  while it's open. PBIR stores it as `refreshType 'APR'` and `duration 'PT5S'`, which we learned from a report Power BI
  saved itself: a hand-written change-detection setting passed the schema but was ignored.
- **Change detection isn't used.** The service can't set it up (Power BI Desktop only), and with live data the silver
  timestamp changes every second, so it would fire at every check anyway.
- **Freshness:** the views read the lakehouse mirror's streaming tables, a few seconds behind the hot path (the newest
  record was 13 s old when checked). The **Newest silver record** card shows it on the Latency page.
- **Proof it refreshes:** query history (`client_application = 'PowerBI'`) shows a round of 6 queries every 5 s on the
  Latency page, each 0.5–2 s.

## Cost

| | Measured |
|---|---|
| Fabric F2 | $0.36/hour while running, $0 while paused ($0.18 per CU-hour, East US 2) |
| Warehouse load | one open Latency page: 6 queries per refresh, ~70 a minute, 0.5–2 s each, and the warehouse never idles while it's open. Every open copy of the report adds its own. |

The four headline cards each query `headline` separately. One visual over that view would cut a refresh from 6 queries
to 3.

## Not verified

- **The service principal path in Power BI.** It's granted and checked from the CLI: it reads the `powerbi` views,
  including the two over the Lakebase registry, and is refused base tables and the registry catalog. But the demo model
  was signed in with a user's own credentials, so Power BI's queries ran as that user.
- **Warehouse DBUs** for a fixed window with the report open (`system.billing.usage`), and Fabric CU use (Capacity
  Metrics app).
- **Many viewers** at 5 s: queries could queue on the warehouse, or F2 could throttle.
