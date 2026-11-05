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

**Measured (2026-11-05, one app, 1 rule, demo load, steady state):** the notification's broker time on the app's hub
minus the gold record's broker time was p50 2.7 s, p99 4.7 s, max 5.2 s over 8,803 notifications in 15 minutes. From
the raw HL7 topic to the app's hub it was p50 2.8 s and p99 4.8 s: the hot path adds well under 100 ms of that. Not
included: the app's own poll after the record lands. Scale is untested: see [SCALABILITY.md](SCALABILITY.md).

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

While listening, it prints only notifications for the exact subscription revisions it just registered. Matches for
earlier revisions or other subscriptions still on the hub are counted as "skipped". Its consumer group resumes where it
last committed, so after time away it first works through the backlog. It reports that backlog as a count ("catching
up", then "caught up") rather than line by line, then prints live matches. Every 30 seconds it checks the hub's heartbeat and
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
3. **Add the SP's profile** to `~/.databrickscfg`, named after the app, then `chmod 600 ~/.databrickscfg`:
   ```ini
   [my-app]
   host          = https://<workspace host>
   client_id     = <the SP's application ID>
   client_secret = <the secret from step 1>
   ```
4. **Check it end to end:**
   ```bash
   cd src/subscriber && mvn -q package
   java -jar target/hl7-subscriber.jar --e2e      # 20 checks, PASS/FAIL table, exit 1 on any failure
   ```
   `make registry` wrote the app's settings to `src/subscriber/apps/<app>.properties` (gitignored, no secrets). It
   rewrites them on every run, because the registry URL changes when the workspace is rebuilt. With a different
   profile name, set `databricks.profile` in a `subscriber.properties` of your own (see the `.example`).

## End-to-end checks

`--e2e` runs every check below against the live system, as the app's service principal. It uses its own `e2e-*`
subscriptions and consumer group, cleans up after itself, and exits 1 if any check fails. The same jar runs it in
the workspace as a serverless job, as the same service principal, with no secret:
`databricks bundle deploy && databricks bundle run e2e` (from `src/subscriber`). The deployer needs the **Service Principal: User** role on that SP to
bind the job's `run_as`.

| # | Check | Input | Expected |
|---|---|---|---|
| 1 | Sign in | the profile (or the job's identity) | a service principal |
| 2 | Own Event Hubs credential | `sub_<app>_eventhubs` | token issued |
| 3 | Another credential | `<catalog>_eventhubs` (the hub's) | denied (no ACCESS) |
| 4 | Read a hub that isn't this app's | a gold topic | TopicAuthorizationException |
| 5 | Write to its own hub (forge a notification) | `<catalog>.subscribers.<app>` | TopicAuthorizationException |
| 6 | Output field not granted | fields include `msg_sending_facility` | FIELDS_DENIED |
| 7 | Filter on a field not granted | `msg_sending_facility = 'NORTH'` | FIELD_DENIED |
| 8 | Source not granted | `gold_encounters` | SOURCE_DENIED |
| 9 | Unknown field | `value_numm > 6` | FIELD_NOT_IN_SOURCE |
| 10 | Wrong value type | `value_num = "high"` | TYPE_MISMATCH |
| 11 | SQL text instead of a predicate | `value_num > (SELECT max(...) FROM <gold topic>)` | PREDICATE_INVALID |
| 12 | Cancel a subscription it doesn't have | `e2e-does-not-exist` | NOT_FOUND_OR_INACCESSIBLE |
| 13 | Register | potassium ≥ 6.5, fields `patient_id, value_num` | ACCEPTED, with the SQL rendering |
| 14 | Hub runs it | that revision | ACTIVE |
| 15 | Notification on its own hub | | exactly those fields, value ≥ 6.5 |
| 16 | Same rule again | identical definition | same revision |
| 17 | Change the rule | ≥ 7.0, add `units` | next revision ACCEPTED, then ACTIVE |
| 18 | Notification reflects the change | | new revision, new fields, value ≥ 7.0 |
| 19 | Cancel | `unsubscribe` | CANCELLED by the hub |
| 20 | Hub heartbeat | `gold_observations` | a batch within 2 minutes |

Cross-app cases (another app reading, cancelling, or spoofing your rows) need a second service principal live. Today
they run against a real PostgreSQL in `tests/test_registry.py`; [SCALABILITY.md](SCALABILITY.md) plans the
multi-SP suite.

To revoke an app, remove it from `subscriber_apps` and run `make up`. `make registry` deletes its binding, so it
can't subscribe, and the hub drops its rules at the next batch. Terraform deletes its hub and connector.

The service credential is used directly. A UC **Kafka connection** wrapping it (Catalog → Create → Connection → Kafka →
Service Credential) is a governance object for Lakeflow Connect ingestion. It doesn't give an outside app a token.
