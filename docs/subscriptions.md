# Subscriptions

Apps subscribe at runtime to the gold records they need, with a rule written in **Databricks SQL**. Each app's matches
arrive on its own Event Hub. Adding, changing, or cancelling a subscription is a single API call: no pipeline, hub, or
deployment per subscription.

```
Java app (as its own SP) ──PUT [{name, sql}]──► Subscription API (Databricks App, 2 instances, stateless)
        │  GET ?wait=120 ─────────────────────►   caller = the SP the Apps proxy verified
        │                                          shape check, then Databricks compiles the SQL (DESCRIBE QUERY)
        │                                          against the app's own views: its errors pass through unchanged
        │                                          store it as the app's own registry role (Lakebase)
        │                                                       │ rules
        │      gold_* topics ──► subscription hub (micro-batch, all rules in one pass) ──► <catalog>.subscribers.<app>
        └──────────────────────── consume with a UC-minted Event Hubs token ◄────────────────────────────┘
```

- **Subscription API** (`src/subscription_api`, `resources/apps/subscription_api.yml`) is the only way to register
  subscriptions, and a thin facade:
  - it identifies the caller;
  - it checks the rule's shape (`rules.py`, the only rule logic of our own);
  - Databricks compiles the rule;
  - it stores the rule;
  - it waits for the hub.
- **Registry** (`src/registry/registry.sql`, Lakebase Postgres): the apps' subscriptions and the platform's grants. Only
  the API writes to it, and always as the calling app's own role.
- **Hub** (`resources/pipelines/subscription_hub.yml`, `src/pipelines/subscription_hub`, `src/hl7rt/subscriptions.py`):
  one serverless pipeline with one flow per gold source.
  - Each micro-batch it reloads the rules if they changed (one indexed query), re-checks them, and evaluates all of a
    source's rules in one projection.
  - It writes the matches to each app's hub.
  - A rule that fails on the data is reported FAILED on its own; the other rules keep running.
- **App** (`examples/subscribers/java`, Java 17): declares its subscriptions in code, sends them to the API, and consumes its hub.

The hub runs micro-batches (seconds, not milliseconds). It needs `foreachBatch` to apply a rule set that changes at
runtime, and Real-Time Mode doesn't support `foreachBatch`. `foreach_batch_sink` is Public Preview, so the hub runs on
the PREVIEW channel. The RTM hot path is unchanged.

**How the hub evaluates rules:**
- **Up to 1,000 enabled rules across all apps.** Past that, a new rule is refused with `RULE_LIMIT_REACHED`; cancel
  one first.
- **Compiled off the delivery path:** the rule set is compiled in the background when the registry changes, and
  batches keep using the last compiled set.
- **Simple rules run as data:** a column compared with a literal, joined by `AND`. Each record meets only the rules
  that name one of its values (an equality index), in a plan whose size doesn't depend on the rule count.
- **Other rules run as SQL expressions:** functions, `OR`, `LIKE`.
- **The path doesn't change the result:** both select the same records as the SQL editor and send identical
  notifications.

**Measured** (2026-11-05, demo load, rules matching 0.1% of patients each). Delivery is the app-hub broker time minus
the gold record's broker time:

| Rules on one source | Hub batch interval | Delivery p50 | p99 | One rule change → ACTIVE |
|---|---|---|---|---|
| 1 | 3.4–4 s | 2.7 s | 4.7 s | ~5 s |
| 1,000 (the cap) | ~4 s | 4.5 s | 7.1 s | 8 s |
| 1,000, of which 100 use functions | ~4 s | 4.5 s | 6.5 s | — |

From the raw HL7 topic to the app's hub, one rule measured p99 4.8 s, of which the hot path takes under 100 ms. These
numbers don't include the app's own poll. How it got here, and what's next past the cap: [SCALABILITY.md](SCALABILITY.md).

## Security model

| Layer | What enforces it |
|---|---|
| Who is calling | The app calls the Subscription API with its own service principal's OAuth token. The Databricks Apps proxy authenticates it, sets `X-Forwarded-Email` to the SP's application ID, and overwrites any forged copy (tested). Anonymous calls get 401. |
| No way around the API | App roles in the registry are NOLOGIN, granted only to the API's service principal. Apps have no login role and no Data API: the Lakebase Data API is off, and a direct connection as the SP is refused (tested). |
| Whose subscription | The API writes as the app's own role (`SET LOCAL ROLE`). Row-level security plus a trigger make the owner `current_user`, so an app can't read, change, or cancel another app's subscriptions, and the API can't write as itself. |
| What it may read | Unity Catalog views per app (`<catalog>.sub_<app>.<source>`) expose only its granted fields (`subscriber_apps` in Terraform). Databricks compiles each rule against them, so an ungranted column or source is Databricks' own `UNRESOLVED_COLUMN` or `TABLE_OR_VIEW_NOT_FOUND`. Filtering on a column counts as reading it. The registry trigger re-checks the source and fields. |
| What a rule may do | One SELECT of columns, from one source, with a WHERE filter: any Databricks built-in except functions that reach outside the record (network, secrets, JVM, AI, UDFs). No joins, subqueries, CTEs, set operations, aggregates, parameters, or multiple statements (`SQL_NOT_ALLOWED`). The hub re-checks each rule before evaluating it, granted columns included. |
| Where results go | The registry assigns the destination (the app's own hub); a request can't name one. |
| Who can read results | Each app's hub is readable only by that app's own access connector (Data Receiver on that one hub). Its UC service credential `sub_<app>_eventhubs` grants ACCESS only to the app's SP. The app holds no Azure secret: Unity Catalog mints a short-lived Event Hubs token. |
| Secrets | The SP's OAuth secret lives in the Databricks CLI profile (`~/.databrickscfg`). The settings file holds none, and the app refuses to start if it finds one there. |

**Not covered yet:**
- **Hub identity.** The hub runs as whoever deploys the bundle, which is the registry owner and bypasses RLS. In
  production, give it a dedicated service principal with read access to the registry and write access to `status` and
  `hub_heartbeat` only.
- **Cancellation is a soft stop.** Re-declaring a cancelled name re-enables it under a new revision.

## No silent failures

The app stops with a reason instead of listening quietly:

| Exit | When |
|---|---|
| 2 | A rule was refused: Databricks' own error, `SQL_NOT_ALLOWED`, `INVALID_NAME`, or `RULE_LIMIT_REACHED`. Nothing was applied. |
| 3 | The hub can't run an accepted revision (FAILED, with Spark's error) |
| 4 | The hub didn't confirm within 2 minutes, or isn't processing their sources (`HUB_STALE`: stopped pipeline) |
| 5 | It signed in as something other than a service principal |
| 6 | Its service principal isn't an onboarded subscriber app (`APP_NOT_ONBOARDED`) |

While listening, it prints only notifications for the subscription revisions it just registered.
- **Skipped:** matches for earlier revisions or other subscriptions still on the hub are counted as "skipped".
- **Catching up:** its consumer group resumes where it last committed, so after time away it first works through the
  backlog, reported as a count ("catching up", then "caught up").
- **Hub warnings:** every 30 seconds it checks that the hub is processing.
- **Dedupe:** notifications carry a stable `notificationId`, so redeliveries are deduplicated (the Kafka sink is
  at-least-once).

## The Subscription API

Every call carries the app's own SP token (`Authorization: Bearer …`). Every answer is JSON.

| Call | Does |
|---|---|
| `PUT /api/v1/subscriptions` `{"subscriptions": [{"name", "sql"}]}` | Declares the app's full set. Atomic: if any rule is refused, nothing is applied (422, with Databricks' error per rule). Cancels what's no longer declared. Returns each revision, `unchanged`, the canonical query, and the destination. |
| `PUT /api/v1/subscriptions/{name}` `{"sql"}` | One subscription |
| `DELETE /api/v1/subscriptions/{name}` | Cancel one (404 `NOT_FOUND_OR_INACCESSIBLE` if it isn't yours) |
| `GET /api/v1/subscriptions?wait=N` | This app's subscriptions and the hub's state. With `wait`, holds until `READY` (the hub runs exactly these revisions and is processing), `FAILED`, or the wait ends (`PENDING` / `HUB_STALE`). |
| `POST /api/v1/validate` | The same checks as `PUT`, applying nothing (for CI) |

Any language can be a client: an HTTP call, then a Kafka consumer that dedupes by `notificationId` and keeps only the
revisions it registered. The full contract:
- **OpenAPI:** [`docs/api/openapi.json`](api/openapi.json), and Swagger UI at `<api>/docs`.
- **The guide:** [docs/integration.md](integration.md), covering the handshake with curl, the Event Hubs token, Kafka
  settings, the notification format, and the errors.

The App runs 2 instances behind one URL. Databricks Apps calls this horizontal scaling: a fixed instance count, 1–5 per
app. Load-based autoscaling (min < max) isn't enabled for this workspace: the Apps team plans it as a private preview,
and it adds instances in 2–3 minutes. When it is enabled, set `app_min_instances` < `app_max_instances` in
`databricks.yml`. The API is stateless (the registry is the only state), so any instance serves any call.

## Rules

```sql
SELECT patient_id, value_num FROM gold_observations
WHERE observation_cd_desc = 'Hemoglobin' AND value_num < 8
```

- **Allowed:**
  - `SELECT` lists the fields to deliver: columns only.
  - `FROM` names one gold source: `gold_encounters`, `gold_observations`, or `gold_orders`. Unqualified, or the app's own
    view in full, as tested in the editor.
  - `WHERE` is any Databricks SQL filter over that record: comparisons, `IN`, `BETWEEN`, `LIKE`, `IS NULL`, `AND`/`OR`/
    `NOT`, and built-ins such as `lower`, `startswith`, `contains`, `abs`, `coalesce`.
- **Normalized:** identifiers are case-insensitive, as in Databricks, and stored in lowercase. Comments and a trailing
  `;` are dropped from the canonical query.
- **Refused:** randomness (`rand`, `uuid`, …): a rule must match the same record the same way every time. Also time
  travel (`VERSION AS OF`), a field listed twice, and SQL over 10,000 characters.
- **Also refused:** functions that reach outside the record (`http_request`, `secret`, `reflect`/`java_method`, `ai_*`,
  `read_*`, UC UDFs, `current_user`), and joins, subqueries, CTEs, set operations, aggregates, sorting, `LIMIT`,
  parameters, and multiple statements. All are `SQL_NOT_ALLOWED`.
- **The same semantics as the SQL editor:** the hub evaluates the stored WHERE with Spark, so the rule selects in the
  hub what it selects in the editor (`tests/test_subscriptions.py` checks this for each operator, NULLs included).
- **A rule can compile and still fail on data.** `value_num > 'high'` compiles, then fails when it meets a row (ANSI
  cast), in the editor as in the hub. The hub reports that rule FAILED with Spark's error (exit 3), and the other rules
  keep running.

**Anything that needs a join, subquery, aggregate, or memory across events is out of scope for an app's rule.** The
platform builds it once, as a new governed source (a gold topic/table), and grants it to the apps that need it:

| Need | Pattern |
|---|---|
| Labs for patients currently in the ICU | A platform source that adds the patient's location to each lab (stream-static or stateful join at gold) |
| A large cohort list | A platform-registered lookup source; a small list fits in `IN (...)` |
| "Second abnormal result within 24 h" | Temporal correlation: a separate, stateful capability |
| Current census by unit changed | Query-result changes: a materialized view plus a diff, platform-built |

## Set up (bring your own service principal)

Each subscriber app is an onboarding step, done once per app. For the FE sandbox it's scripted, so it survives the
Sunday wipe.

1. **Create the app's service principal and its secret.** In the UI: Settings → Identity and access → Service
   principals → Add, then Secrets → Generate. Or from the CLI: `databricks service-principals create`, then
   `databricks service-principal-secrets-proxy create <id>`, writing the secret straight into the profile in step 3.
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
   Then `make up`, and `make demo dev`. Both are idempotent:
   - **Terraform:** the app's hub `<catalog>.subscribers.my-app`, its access connector, and Data Receiver on that hub.
   - **`make uc`:** the UC service credential `sub_my_app_eventhubs`, with ACCESS granted to the SP.
   - **`make registry`** (also run by `make demo`):
     - the app's NOLOGIN registry role and its binding;
     - its views `<catalog>.sub_my_app.<source>`;
     - CAN_USE on the Subscription API, and the API's read on those views.
     - It also writes the app's settings to `examples/subscribers/apps/my-app.properties` (gitignored, no secrets),
       rewritten every run because URLs change when the workspace is rebuilt.
3. **Add the SP's profile** to `~/.databrickscfg`, named after the app, then `chmod 600 ~/.databrickscfg`:
   ```ini
   [my-app]
   host          = https://<workspace host>
   client_id     = <the SP's application ID>
   client_secret = <the secret from step 1>
   ```
4. **Check it end to end:**
   ```bash
   mvn -q -f examples/subscribers/java package
   java -jar examples/subscribers/java/target/hl7-subscriber.jar --e2e      # 25 checks, PASS/FAIL table, exit 1 on any failure
   ```

## End-to-end checks

`--e2e` runs every check below against the live system, as the app's service principal.
- **It leaves the app's own subscriptions alone.** It uses only `e2e-*` names, one at a time, and its own consumer
  group, and it cleans up after itself.
- **Exit code:** it exits 1 if any check fails.
- **In the workspace:** the same jar runs as a serverless job, as the same service principal, with no secret. Run
  `DATABRICKS_BUNDLE_ROOT=examples/subscribers/java databricks bundle deploy`, then the same with `bundle run e2e`. The deployer
  needs the **Service Principal: User** role on that SP to bind the job's `run_as`.

| # | Check | Input | Expected |
|---|---|---|---|
| 1 | Sign in | the profile (or the job's identity) | a service principal |
| 2 | Connection from the API | `GET /api/v1/connection` | this app's own topic and credential |
| 3 | Own Event Hubs credential | `sub_<app>_eventhubs` | token issued |
| 4 | Another credential | `<catalog>_eventhubs` (the hub's) | denied (no ACCESS) |
| 5 | Read a hub that isn't this app's | a gold topic | not authorized |
| 6 | Write to its own hub (forge a notification) | `<catalog>.subscribers.<app>` | not authorized (Receiver only) |
| 7 | Skip the API | the registry's old Data API URL | not available |
| 8 | Column not granted | `WHERE msg_sending_facility = 'NORTH'` | `UNRESOLVED_COLUMN` (Databricks) |
| 9 | Source not granted | `FROM gold_encounters` | `TABLE_OR_VIEW_NOT_FOUND` (Databricks) |
| 10 | Misspelled column | `WHERE value_numm > 6` | `UNRESOLVED_COLUMN` (Databricks) |
| 11 | Syntax error | `WHER value_num > 6` | `PARSE_SYNTAX_ERROR` (Databricks) |
| 12 | Subquery | `WHERE value_num > (SELECT max(...) ...)` | `SQL_NOT_ALLOWED` |
| 13 | Join | `... JOIN gold_encounters ...` | `SQL_NOT_ALLOWED` |
| 14 | Function that reaches outside the record | `http_request(...)` | `SQL_NOT_ALLOWED` |
| 15 | Table outside this app's views | `FROM <catalog>.<schema>.gold_observations` | `SQL_NOT_ALLOWED` |
| 16 | One refusal applies nothing | a valid and a misspelled rule together | 422, the valid one not registered |
| 17 | Validate applies nothing | `POST /validate` | valid, not registered |
| 18 | Register | potassium ≥ 6.5, fields `patient_id, value_num` | ACCEPTED, with the canonical SQL |
| 19 | Hub runs it | that revision | ACTIVE |
| 20 | Notification on its own hub | | exactly those fields, value ≥ 6.5 |
| 21 | Same SQL again | identical rule | same revision, unchanged |
| 22 | Change the rule | ≥ 7.0, add `units` | next revision, then ACTIVE |
| 23 | Notification reflects the change | | new revision, new fields, value ≥ 7.0 |
| 24 | Cancel | `DELETE` | CANCELLED by the hub |
| 25 | Hub heartbeat | `gold_observations` | a batch within 2 minutes |

Cross-app cases need a second service principal live:
- **Today:** they run against a real PostgreSQL in `tests/test_registry.py` (an app can't read, cancel, or claim another
  app's rows; the API can act only as its caller) and in `tests/test_subscription_api.py`.
- **Planned:** [SCALABILITY.md](SCALABILITY.md) plans the multi-SP suite.

## Monitoring

The **HL7 subscriptions** dashboard (`make demo dev` prints its URL) reads the registry live. Refresh it to update.
- **Counts:** active subscriptions, failed ones, and apps subscribed.
- **Hub heartbeat:** per gold source, seconds since the last batch and the rules running. A source is STALE after 2
  minutes.
- **Current subscriptions:** per app and service principal, the registered revision, its SQL, fields, the hub's state
  (PENDING, ACTIVE, FAILED with the error, or CANCELLING), and how long the hub took to confirm it.
- **Cancelled subscriptions**, below.

How it reads the registry:
- `make registry` registers the registry database in Unity Catalog as `<catalog>_registry`. The dashboard queries the
  views in its `subscription_monitor` schema from the SQL warehouse.
- Those views show every app's rules, so they're for the platform team only. Nothing in them is granted to apps or the
  API.
- Dashboard viewers query as themselves, so they need access to that catalog.

To revoke an app, remove it from `subscriber_apps` and run `make up`. `make registry` deletes its binding, so the API
refuses it (`APP_NOT_ONBOARDED`), and the hub drops its rules at the next batch. Terraform deletes its hub and connector.

The service credential is used directly. A UC **Kafka connection** wrapping it (Catalog → Create → Connection → Kafka →
Service Credential) is a governance object for Lakeflow Connect ingestion. It doesn't give an outside app a token.
