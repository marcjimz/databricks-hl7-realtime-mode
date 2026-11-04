# Subscriptions

Apps subscribe at runtime to the gold records they need. Each app's matches arrive on its own Event Hub. Adding,
changing, or cancelling a subscription is a single API call: no pipeline, hub, or deployment per subscription.

```
                                   Lakebase registry (Data API)
   Java app (as its own SP) ──subscribe──►  subscription_api.subscribe(...)  ── ACCEPTED or refused, same call
          ▲                                       │ rules + grants
          │                                       ▼
          │      gold_* topics ──► subscription hub pipeline (micro-batch) ──► <catalog>.subscribers.<app>
          └──────────────────────────────── consume with a UC-minted Event Hubs token ◄──────┘
```

- **Registry** (`src/registry/registry.sql`, Lakebase Postgres): the apps' subscriptions and the platform's grants.
  Apps reach it only through the functions in schema `subscription_api`, via the Lakebase Data API (REST).
- **Hub** (`resources/pipelines/subscription_hub.yml`, `src/pipelines/subscription_hub`, `src/hl7rt/subscriptions.py`):
  one serverless pipeline with one flow per gold source. Each micro-batch it reloads the rules if they changed (one
  indexed query), applies every rule, and writes matches to each app's hub.
- **App** (`src/subscriber`, Java 17): declares its subscriptions in code, registers them on start, and consumes its hub.

The hub runs micro-batches (seconds, not milliseconds): it needs `foreachBatch` to apply a rule set that changes at
runtime, and Real-Time Mode doesn't support `foreachBatch`. `foreach_batch_sink` is Public Preview, so the hub runs
on the PREVIEW channel. The RTM hot path is unchanged.

## Security model

| Layer | What enforces it |
|---|---|
| Who is calling | The app signs in as its own Databricks service principal (OAuth M2M). The Data API runs every call as that SP's Postgres role, so the registry knows the caller without trusting the request. |
| Whose subscription | Row-level security plus a trigger: the owner is always `current_user`. An app can't read, change, or cancel another app's subscriptions, even with direct SQL that skips the API. |
| What it may read | Platform grants per app (`subscriber_apps` in Terraform): sources and columns. Filtering on a column counts as reading it, so predicates may only use granted columns. Output fields are capped the same way. |
| What a rule may do | Rules are a structured predicate (JSON), never SQL text. The registry validates every field, operator, and value type against the source's registered columns when the rule is written. The hub builds its Spark filter from `F.col` and literals only, so an app can't inject SQL, use a subquery, or reach another table. |
| Where results go | The registry assigns the destination (the app's own hub); a request can't name one. |
| Who can read results | Each app's hub is readable only by that app's own access connector (Data Receiver on that one hub). Its UC service credential `sub_<app>_eventhubs` grants ACCESS only to the app's SP. The app holds no Azure secret: Unity Catalog mints a short-lived Event Hubs token. |
| Secrets | The SP's OAuth secret lives in the Databricks CLI profile (`~/.databrickscfg`). `subscriber.properties` holds no secrets, and the app refuses to start if it finds one there. |

**Not covered yet:**
- **Hub identity.** The hub runs as whoever deploys the bundle, which is the registry owner and bypasses RLS. In
  production, give it a dedicated service principal with read access to the registry and write access to `status` and
  `hub_heartbeat` only.
- **Cancellation is a soft stop.** Re-subscribing a cancelled name re-enables it under a new revision. The spec treats
  cancellation as terminal.

## No silent failures

The app stops with a reason instead of listening quietly:

| Exit | When |
|---|---|
| 2 | The registry refused a subscription (unknown or ungranted field, wrong type, unsupported operator, ...) |
| 3 | The hub couldn't run an accepted revision (FAILED, with the reason) |
| 4 | The hub didn't confirm the revisions within 2 minutes, or isn't processing their source (stopped pipeline) |
| 5 | It signed in as something other than a service principal |
| 6 | The registry refused the identity (app not onboarded) |

While listening, it prints only notifications for the exact subscription revisions it just registered. Earlier
revisions' matches still on the hub are counted as "skipped". Every 30 seconds it checks the hub's heartbeat and
warns if a source hasn't been processed for 2 minutes. Notifications carry a stable `notificationId`, so redeliveries
(the Kafka sink is at-least-once) are deduplicated.

## Rules

```java
field("observation_cd_desc").eq("Hemoglobin").and(field("value_num").lt(8))
```

| Column type | Operators |
|---|---|
| string | `eq` `ne` `in` `notIn` `isNull` `isNotNull` `contains` `startsWith` `endsWith` (literal text, no wildcards) |
| int / long / double | `eq` `ne` `lt` `le` `gt` `ge` `in` `notIn` `isNull` `isNotNull` |
| timestamp | `eq` `ne` `lt` `le` `gt` `ge` `isNull` `isNotNull` (an `Instant`) |

- Combine with `and`, `or`, `not`. Limits: 8 levels deep, 100 nodes, 1000 values per `in`.
- Comparisons follow SQL: a NULL never matches, so use `isNull()` to select missing values.
- The registry stores a SQL rendering of each rule for people to read (`predicateSql`). It is never executed.

**Anything that needs a join, subquery, aggregate, or memory across events is out of scope for an app's rule.** The
platform builds it once, as a new governed source (a gold topic/table), and grants it to the apps that need it:

| Need | Pattern |
|---|---|
| Labs for patients currently in the ICU | A platform source that adds the patient's location to each lab (stream-static or stateful join at gold) |
| A large cohort list | A platform-registered lookup source; a small list fits in `in(...)` |
| "Second abnormal result within 24 h" | Temporal correlation: a separate, stateful capability |
| Current census by unit changed | Query-result changes: a materialized view plus a diff, platform-built |

## Set up (bring your own service principal)

Each subscriber app is an onboarding step, done once per app. For the FE sandbox it's scripted, so it survives the
Sunday wipe.

1. **Create the app's service principal** in the workspace: Settings → Identity and access → Service principals →
   Add, then Secrets → Generate. Keep the secret; you'll paste it into step 3.
2. **Declare the app** in `infra/azure/terraform.tfvars`:
   ```hcl
   subscriber_apps = {
     "my-app" = {
       application_id = "<the SP's application ID>"
       sources        = ["gold_observations"]
       fields         = ["patient_id", "observation_cd_desc", "value_num", "units", "abnormal_flag"]
     }
   }
   ```
   Then `make up` creates or updates everything below. It's idempotent:
   - **Terraform:** the app's hub `<catalog>.subscribers.my-app`, its access connector, and Data Receiver on that hub.
   - **`make uc`:** the UC service credential `sub_my_app_eventhubs`, with ACCESS granted to the SP.
   - **`make registry`:** the Lakebase project and registry if missing, the SP's Postgres role, and its binding
     (hub, sources, fields). It prints `registry.url` and `registry.endpoint` for step 4.
3. **Add the SP's profile** to `~/.databrickscfg`, then `chmod 600 ~/.databrickscfg`:
   ```ini
   [my-app]
   host          = https://<workspace host>
   client_id     = <the SP's application ID>
   client_secret = <the secret from step 1>
   ```
4. **Configure the app:**
   ```bash
   cd src/subscriber
   cp subscriber.properties.example subscriber.properties   # gitignored; fill in the values `make registry` printed
   ```

To revoke an app, remove it from `subscriber_apps` and run `make up`. `make registry` deletes its binding, so it
can't subscribe, and the hub drops its rules at the next batch. Terraform deletes its hub and connector.

The service credential is used directly. A UC **Kafka connection** wrapping it (Catalog → Create → Connection → Kafka →
Service Credential) is a governance object for Lakeflow Connect ingestion. It doesn't give an outside app a token.
