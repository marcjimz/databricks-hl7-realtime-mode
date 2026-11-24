# Subscriptions

Apps subscribe at runtime to the silver records they need, with a rule written in **Databricks SQL**. Each app's matches
arrive on its own Event Hub. Adding, changing, or cancelling a subscription is a single API call: no pipeline, hub, or
deployment per subscription.

```
App (as its own SP) ──PUT [{name, sql}]──► Subscription API (Databricks App, 2 instances, stateless)
        │  GET ?wait=25 ─────────────────────►   caller = the SP the Apps proxy verified
        │                                          shape check, then Databricks compiles the SQL (DESCRIBE QUERY)
        │                                          against the app's own views: its errors pass through unchanged
        │                                          store it as the app's own registry role (Lakebase)
        │                                                       │ rules
        │    silver_* topics ──► the app's subscription hub (edp_shared, or its team's own) ──► <catalog>.subscribers.<app>
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
- **Hub** (`hub/resources/subscription_hub.yml`, `src/pipelines/subscription_hub`, `src/hl7rt/engine.py`): one
  serverless pipeline per hub, with a flow per silver source, batching every `hub_trigger_interval_seconds` (default 1;
  SDP's default is 4 s). Each app's realtime rules run on its hub: `edp_shared` or its team's own (see "Two ways to
  subscribe"). Every app's deferred rules run on one more pipeline (see "Delivery tiers").
  - Each micro-batch it reads only the subscriptions that changed (one query when nothing did), re-checks them, reads
    the batch once, and matches every rule of that source.
  - It writes each app's matches with that app's own Event Hubs producer: a topic that refuses writes parks only that
    app's notifications (in the registry, delivered in order once the topic is back), never another app's.
  - A rule that fails on the data is reported FAILED on its own; the other rules keep running.
- **App** (`examples/subscribers/python` and `examples/subscribers/java`, the same client twice): declares its
  subscriptions in code, sends them to the API, and consumes its hub.

## Two ways to subscribe

The rules, the API, and the app are the same either way. The platform decides which hub runs an app's rules
(`subscriber_apps.<app>.subscription_hub` in Terraform, default `edp_shared`), and `GET /api/v1/connection` tells the
app.

| | `edp_shared` (the default) | A team's own hub |
|---|---|---|
| Deployed by | the platform, with the main bundle (`make demo dev`) | the team, from the [`hub/`](../hub/README.md) bundle, **before** its apps register rules (`make hub dev HUB=<name>`) |
| Runs as | the platform's deployer | the team's service principal |
| Runs | the realtime rules of every app assigned to it, and **every** app's deferred rules | only its apps' realtime rules |
| Event Hubs | `<catalog>_eventhubs` | `hub_<name>_eventhubs`: Data Receiver on the five silver hubs, Data Sender only on its apps' hubs, ACCESS for its service principal only |
| Registry | the owner (its queries filter to its hub) | its own Lakebase role: row-level security lets it read only its apps' rules and write only its own rows |
| Rule cap | 2,000 enabled rules per tier | 2,000 (`subscriber_hubs.<name>.max_enabled_rules`) |
| Cost | shared by everyone on it | one always-on serverless pipeline, ~$3.80/h estimated, whatever the apps or rules |

- **Why a team would run its own:** isolation of compute and a rule cap of its own, for a business unit rather than an
  app (a pipeline per app would cost ~$3.80/h each).
- **What it takes:** the platform adds the hub to `subscriber_hubs` and assigns apps to it, then `make up` (Terraform,
  `make uc`, `make registry`). The team deploys the bundle as its service principal. Details: [hub/README.md](../hub/README.md).
- **Trust:** a team hub's service principal reads every column of silver; the hub enforces each app's column grants in
  its own code. Running a hub means being trusted with silver.

The hub runs micro-batches (seconds, not milliseconds). It needs `foreachBatch` to apply a rule set that changes at
runtime, and Real-Time Mode doesn't support `foreachBatch`. `foreach_batch_sink` is Public Preview, so the hub runs on
the PREVIEW channel. The RTM hot path is unchanged.

**How the hub evaluates rules:**
- **Up to 2,000 enabled rules per hub and tier.** Past that, a new rule is refused with `RULE_LIMIT_REACHED`, naming
  the hub; cancel one first, or move apps to another hub.
- **A change touches only its own rule.** The hub reads only the subscriptions whose generation moved, re-checks them,
  and places each new revision in its matcher: the subscription's state is then `PLACED`, and `ACTIVE` once a batch
  has decided records with it.
- **Simple rules are matched on the driver:** comparisons of a column with a literal, combined with `AND` and `OR`
  (flattened to at most 64 AND groups). Each group is indexed in memory on its most selective `=` or `IN` (patient or
  visit ID first, then codes, then points of care, then anything else), so a record meets only the groups that name
  one of its values. Spark renders each selected field of the batch once, exactly as `to_json` would; the notification
  joins those, so it's byte for byte what the SQL path writes.
- **Other rules run as SQL expressions** (functions, `LIKE`, `NOT` around a group), in the same read of the batch.
- **The path doesn't change the result:** both select the same records as the SQL editor and send identical
  notifications (`tests/test_subscriptions.py`, `tests/test_matching.py`).
- **A retried batch decides as it did the first time.** Before it publishes, the hub records which revisions decided
  the batch (`bindings`); a retry of that batch (after a crash or restart) decides from those revisions, even if a rule
  changed in between.
- **One app can't hold up another.** Each app has its own producer. If its topic refuses writes for longer than it
  usually takes to acknowledge (3 × its recent p99, 2–10 s), its notifications are parked in the registry (`pending`)
  and delivered in order when the topic is back; the other apps' notifications go on.

**Measured, this engine, on dev** (2026-11-24, demo load, 30 minutes): both patterns at once. app-a on `edp_shared`
and app-b on its own hub `bu_demo` (run as app-b's service principal), each with the same 7 realtime rules across all
five sources: one expression rule (`startswith`), and a 200-patient list edited every 20 s. Delivery is the app-topic
broker time minus the source record's broker time:

| Hub | Notifications | Delivery p50 | p90 | p99 | max | Duplicates | Edit → PLACED p50 / p99 | Edit → ACTIVE p50 / p99 |
|---|---|---|---|---|---|---|---|---|
| `edp_shared` (app-a) | 78,571 | 1.31 s | 1.86 s | 2.48 s | 14.0 s | 0 | 1.15 / 2.79 s | 2.0 / 4.33 s |
| `bu_demo` (app-b) | 78,533 | 1.28 s | 1.83 s | 2.47 s | 13.7 s | 0 | 1.04 / 3.19 s | 1.82 / 3.94 s |

- **Both hubs decided exactly the same records** for every rule (e.g. 24,266 and 24,266 for one rule, 7,379 and 7,379
  for the expression rule), each on its own pipeline, checkpoint, and identity.
- **The expression rule's tail is the simple rules' tail** (p99 2.44 / 2.52 s): it runs in the same read.
- **Batch phases** (p99): the silver read and collect 1.1 s, matching ≤ 5 ms, binding ~40 ms, delivery ~0.5 s. Nothing
  was parked.
- **The max:** three minutes had a p99 over 7 s. Two of them (03:12 and 03:21 UTC) hit both hubs at once, so the cause
  is upstream of the hubs; one (03:32) hit only `bu_demo` (12.2 s against 3.5 s). Not traced.
- **Edits:** 105 per app, all ACTIVE; times include the API call (the PUT) and the status poll (every 0.25 s).

**Measured, this engine at scale** (spike S7, 2026-11-20, dev, demo load: the same engine over a synthetic registry,
every app with its own topic, rules matching ~0.1% of a source's records each):

| Apps (rules) | Topology | Delivery p99 steady | p99 under churn (30 membership edits/s) | Cost (estimated) |
|---|---|---|---|---|
| 2 (80) → 50 (2,000) | one pipeline, a flow per source | 2.7–2.9 s | — | ~$3.80/h |
| 50 (2,000) | one pipeline, a flow per source (the default) | 2.9 s | 6.5 s | ~$3.80/h |
| 50 (2,000) | one pipeline per source | 3.0 s | 2.8 s | ~$14.20/h |

Matching 2,000 rules took p99 under 0.1 s per batch. Every slow batch was spent reading the batch from silver, not
matching or delivering. The ~$3.80/h is S6's billed 8.2–8.8 DBU/h for a five-flow pipeline, flat in apps and rules.

**Measured, the previous engine** (Spark matching, one pipeline per source; 2026-11-05, demo load):

| Rules on one source | Hub batch interval | Delivery p50 | p99 | One rule change → ACTIVE |
|---|---|---|---|---|
| 1 | 3.4–4 s | 2.7 s | 4.7 s | ~5 s |
| 1,000 (its cap) | ~4 s | 4.5 s | 7.1 s | 8 s |
| 1,000, of which 100 use functions | ~4 s | 4.5 s | 6.5 s | — |

From the raw HL7 topic to the app's hub, one rule measured p99 4.8 s, of which the hot path takes under 100 ms. These
numbers don't include the app's own poll. How it got here: [SCALABILITY.md](SCALABILITY.md).

## Dynamic patient rules

A workflow often follows patients, not codes: a base rule watches emergency-department registrations, and each
registration adds that patient to the app's patient rules (updates, admit diagnoses, labs and vitals, radiology, notes)
until discharge, transfer, or the app decides the patient doesn't qualify. The patient list is SQL, in the rule:

```sql
SELECT patient_id, value_num, units FROM silver_observations
WHERE patient_id IN ('MRN050001', 'MRN050002', ...)
  AND ((alt_observation_cd_system = 'LN' AND alt_observation_cd IN ('2823-3', '718-7'))
    OR (observation_cd_system = 'FLOWSHEET' AND observation_cd IN ('572 PULSE', '569 SPO2')))
```

- **Adding or removing a patient is an edit of that rule** (`PUT /api/v1/subscriptions/{name}`). It's a new revision
  of the same subscription: it doesn't count against the rule cap, and it doesn't touch any other rule, of this app or
  another. The hub's matcher indexes the rule on `patient_id`, so a long list costs a map entry per patient, not a scan
  (measured to the cap with 2,000-patient lists: docs/SCALABILITY.md).
- **Lists of thousands fit.** A rule may be up to 200,000 characters (~10,000 patient IDs); a 10,000-patient rule
  parses and flattens in well under a second.
- **Live only.** A rule delivers records from the batch its revision goes live (ACTIVE) on. Records of a newly added
  patient that arrived between the registration notification and that batch aren't delivered; read them from the app's
  own views if the workflow needs them.
- **The example:** `examples/subscribers/python/monitor.py` runs the whole workflow (logic in `patient_rules.py`) and
  times each census edit until ACTIVE. The synthetic feed plays coherent ED encounters for it to follow
  (`generator_journeys_per_minute`).
- **Measured with the previous engine** (2026-11-16, dev, demo load of 200 msg/s, 6 ED journeys a minute; five
  patient rules edited together per census change, from a separate app; 10–30 minute runs). In spike S7, this engine's
  rule edit → `PLACED` took p50 ~0.5 s / p99 ~1.1 s and edit → `ACTIVE` p99 3.0–4.0 s at 2,000 rules:

  | Hub | Census edit → ACTIVE p50 / p99 | Patient record → app p50 / p99 / max | n (edits, records) |
  |---|---|---|---|
  | One pipeline for all sources, SDP's default 4 s trigger | 6.1 / 8.4 s | 4.4 / 7.7 / 13.3 s | 244, 1,629 |
  | One pipeline, 1 s trigger | 4.3–4.5 / 7.0–9.3 s | 5.1–5.3 / 6.9–9.6 s | 140 + 86, 797 + 503 |
  | **One pipeline per source, 1 s trigger** | **3.4 / 8.4 s** | **3.6 / 5.8 / 6.6 s** | 94, 455 |

  No record was delivered for a patient after their removal went live, in any run. Edit → ACTIVE is the five PUTs
  (~1 s once Databricks has compiled the rule's shape: the API reuses that compile for edits that only change literals,
  `rules.shape`; a new shape compiles on the warehouse, ~1.4 s per rule), then the hub's next batch, which recompiles
  the edit in-batch (0.2–0.5 s).

  Where a batch's time goes (`hub_progress`, per source, p50 of the current setup): ~3 s per batch, of which ~1.1–1.4 s
  pass inside Spark's batch call before the hub's function starts (`age_ms`), ~1.1–1.4 s are the hub's collect, match,
  and Event Hubs write (`write_ms`), and ~0.6 s are Spark's offsets, commits, and source metrics (`spark_ms`). In one
  pipeline, the five sources' batches overlapped 5–8 at a time on one driver and Python process and took ~4–5 s each,
  so a shorter trigger alone made delivery slower. What's left is per-batch overhead of `foreachBatch` on serverless;
  going well under ~3 s needs a different engine (Real-Time Mode, the Phase 3 study).

## Data templates

A template is the platform's named grant for one workflow: the silver sources and columns it needs, and no more.
Templates are defined in Terraform (`subscriber_templates`) and granted to apps (`subscriber_apps.<app>.templates`); an
app's views and grant are its own columns plus its templates'. A subscription that names a template
(`"template": "ed-respiratory-monitoring"`) may read, filter on, and receive only that template's columns: the API
refuses anything else with `TEMPLATE_DENIED`, the registry checks the source and fields again, and the hub runs the rule
with the template's columns as its grant. Template changes apply to the hub at the subscription's next revision.

## Delivery tiers

Each subscription has a tier: `realtime` (the default, for clinical work: the app's hub, as above) or `deferred` (work
that can wait, such as billing). Every app's deferred rules, whatever its hub, run on one more pipeline of
`edp_shared`, `subscription_hub_deferred`: the same code on a longer trigger (`hub_deferred_interval_seconds`, default
60), so their load never shares a batch with realtime rules, and a team hub stays one pipeline. Both deliver to the
app's own Event Hub. Its heartbeat is listed with tier `deferred` (and as `deferred:<source>` in the `hub` map), and
`getStatus` reports `HUB_STALE` for a deferred subscription only after its trigger interval plus the usual 120 s.

## Security model

| Layer | What enforces it |
|---|---|
| Who is calling | The app calls the Subscription API with its own service principal's OAuth token. The Databricks Apps proxy authenticates it, sets `X-Forwarded-Email` to the SP's application ID, and overwrites any forged copy (tested). Anonymous calls get 401. |
| No way around the API | App roles in the registry are NOLOGIN, granted only to the API's service principal. Apps have no login role and no Data API: the Lakebase Data API is off, and a direct connection as the SP is refused (tested). |
| Whose subscription | The API writes as the app's own role (`SET LOCAL ROLE`). Row-level security plus a trigger make the owner `current_user`, so an app can't read, change, or cancel another app's subscriptions, and the API can't write as itself. |
| What it may read | Unity Catalog views per app (`<catalog>.sub_<app>.<source>`) expose only its granted fields (`subscriber_apps` in Terraform). Databricks compiles each rule against them, so an ungranted column or source is Databricks' own `UNRESOLVED_COLUMN` or `TABLE_OR_VIEW_NOT_FOUND`. Filtering on a column counts as reading it. The registry trigger re-checks the source and fields. |
| What a rule may do | One SELECT of columns, from one source, with a WHERE filter: any Databricks built-in except functions that reach outside the record (network, secrets, JVM, AI, UDFs). No joins, subqueries, CTEs, set operations, aggregates, parameters, or multiple statements (`SQL_NOT_ALLOWED`). The hub re-checks each rule before evaluating it, granted columns included. |
| Where results go | The registry assigns the destination (the app's own hub); a request can't name one. |
| Which hub runs them | The platform assigns each app's hub (`subscriber_apps.<app>.subscription_hub`); a request can't choose one. A status counts only when it comes from the hub that runs the rule. |
| What a team's hub may touch | Its Event Hubs identity reads only the silver hubs and writes only its apps' hubs (`hub_<name>_eventhubs`, ACCESS for its service principal only). Its registry role reads only its apps' rules and writes only rows of its own hub (row-level security, tested). |
| Who can read results | Each app's hub is readable only by that app's own access connector (Data Receiver on that one hub). Its UC service credential `sub_<app>_eventhubs` grants ACCESS only to the app's SP. The app holds no Azure secret: Unity Catalog mints a short-lived Event Hubs token. |
| Secrets | The SP's OAuth secret lives in the Databricks CLI profile (`~/.databrickscfg`). The settings file holds none, and the app refuses to start if it finds one there. |

**Not covered yet:**
- **`edp_shared`'s identity.** A team's hub runs as its own service principal with a registry role of its own.
  `edp_shared` still runs as whoever deploys the main bundle, the registry owner, which bypasses RLS (its queries filter
  to its hub). In production, give it a service principal and a `hub_edp_shared` member role as for a team hub.
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
SELECT patient_id, value_num FROM silver_observations
WHERE observation_cd_desc = 'Hemoglobin' AND value_num < 8
```

- **Allowed:**
  - `SELECT` lists the fields to deliver: columns only.
  - `FROM` names one silver source: `silver_encounters`, `silver_observations`, `silver_orders`, `silver_diagnoses`, or
    `silver_documents`. Unqualified, or the app's own view in full, as tested in the editor.
  - `WHERE` is any Databricks SQL filter over that record: comparisons, `IN`, `BETWEEN`, `LIKE`, `IS NULL`, `AND`/`OR`/
    `NOT`, and built-ins such as `lower`, `startswith`, `contains`, `abs`, `coalesce`.
- **Normalized:** identifiers are case-insensitive, as in Databricks, and stored in lowercase. Comments and a trailing
  `;` are dropped from the canonical query.
- **Refused:** randomness (`rand`, `uuid`, …): a rule must match the same record the same way every time. Also time
  travel (`VERSION AS OF`), a field listed twice, and SQL over 200,000 characters.
- **Also refused:** functions that reach outside the record (`http_request`, `secret`, `reflect`/`java_method`, `ai_*`,
  `read_*`, UC UDFs, `current_user`), and joins, subqueries, CTEs, set operations, aggregates, sorting, `LIMIT`,
  parameters, and multiple statements. All are `SQL_NOT_ALLOWED`.
- **The same semantics as the SQL editor:** the hub evaluates the stored WHERE with Spark, so the rule selects in the
  hub what it selects in the editor (`tests/test_subscriptions.py` checks this for each operator, NULLs included).
- **A rule can compile and still fail on data.** `value_num > 'high'` compiles, then fails when it meets a row (ANSI
  cast), in the editor as in the hub. The hub reports that rule FAILED with Spark's error (exit 3), and the other rules
  keep running.

**Anything that needs a join, subquery, aggregate, or memory across events is out of scope for an app's rule.** The
platform builds it once, as a new governed source (a topic/table), and grants it to the apps that need it:

| Need | Pattern |
|---|---|
| Labs for patients currently in the ICU | A platform source that adds the patient's location to each lab (stream-static or stateful join) |
| A large cohort list | A platform-registered lookup source; up to ~10,000 IDs fit in `IN (...)` (see "Dynamic patient rules") |
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
       templates      = []  # data templates from subscriber_templates, and/or its own sources and fields:
       sources        = ["silver_observations"]
       fields         = ["patient_id", "observation_cd_desc", "value_num", "units", "abnormal_flag"]
       # subscription_hub = "lab_team"  # optional: a team's own hub (subscriber_hubs); default edp_shared
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
   java -jar examples/subscribers/java/target/hl7-subscriber.jar --e2e      # 26 checks, PASS/FAIL/SKIP table, exit 1 on any failure
   ```
   Or the Python client: `uv run python examples/subscribers/python/subscriber.py --e2e`, the same 26 checks.

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
| 3 | This app's hub | `GET /api/v1/connection` | the hub that runs its realtime rules (`edp_shared` or its team's) |
| 4 | Own Event Hubs credential | `sub_<app>_eventhubs` | token issued |
| 5 | Another credential | `<catalog>_eventhubs` (the hub's) | denied (no ACCESS) |
| 6 | Read a hub that isn't this app's | a silver topic | not authorized |
| 7 | Write to its own hub (forge a notification) | `<catalog>.subscribers.<app>` | not authorized (Receiver only) |
| 8 | Skip the API | the registry's old Data API URL | not available |
| 9 | Column not granted | `WHERE msg_sending_facility = 'NORTH'` | `UNRESOLVED_COLUMN` (Databricks) |
| 10 | Source not granted | `FROM` the first silver source this app isn't granted (SKIP if it has all five) | `TABLE_OR_VIEW_NOT_FOUND` (Databricks) |
| 11 | Misspelled column | `WHERE value_numm > 6` | `UNRESOLVED_COLUMN` (Databricks) |
| 12 | Syntax error | `WHER value_num > 6` | `PARSE_SYNTAX_ERROR` (Databricks) |
| 13 | Subquery | `WHERE value_num > (SELECT max(...) ...)` | `SQL_NOT_ALLOWED` |
| 14 | Join | `... JOIN silver_encounters ...` | `SQL_NOT_ALLOWED` |
| 15 | Function that reaches outside the record | `http_request(...)` | `SQL_NOT_ALLOWED` |
| 16 | Table outside this app's views | `FROM <catalog>.<schema>.silver_observations` | `SQL_NOT_ALLOWED` |
| 17 | One refusal applies nothing | a valid and a misspelled rule together | 422, the valid one not registered |
| 18 | Validate applies nothing | `POST /validate` | valid, not registered |
| 19 | Register | potassium ≥ 6.5 (by name if `observation_cd_desc` is granted, else LOINC `2823-3`), fields `patient_id, value_num` | ACCEPTED, with the canonical SQL |
| 20 | Hub runs it | that revision | ACTIVE |
| 21 | Notification on its own hub | | exactly those fields, value ≥ 6.5 |
| 22 | Same SQL again | identical rule | same revision, unchanged |
| 23 | Change the rule | ≥ 7.0, add `units` | next revision, then ACTIVE |
| 24 | Notification reflects the change | | new revision, new fields, value ≥ 7.0 |
| 25 | Cancel | `DELETE` | CANCELLED by the hub |
| 26 | Hub heartbeat | this app's hub, `silver_observations` | a batch within 2 minutes |

Afterwards it cancels every `e2e-*` subscription the app still has enabled.

Cross-app cases need a second service principal live:
- **Today:** they run against a real PostgreSQL in `tests/test_registry.py` (an app can't read, cancel, or claim another
  app's rows; the API can act only as its caller) and in `tests/test_subscription_api.py`.
- **Planned:** [SCALABILITY.md](SCALABILITY.md) plans the multi-SP suite.

## Monitoring

The **HL7 subscriptions** dashboard (`make demo dev` prints its URL) reads the registry live. Refresh it to update.
- **Counts:** active subscriptions, failed ones, and apps subscribed.
- **Hub heartbeat:** per hub, tier, and silver source, seconds since the last batch and the rules running. A source
  is STALE after 2 minutes.
- **Current subscriptions:** per app and service principal, the registered revision, its SQL, fields, the hub's state
  (PENDING, PLACED, ACTIVE, FAILED with the error, or CANCELLING), the hub that runs it, and how long the hub took to
  confirm it.
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
