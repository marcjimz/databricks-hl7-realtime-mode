# AGENTS.md

Guidance for coding agents (and humans) working in this repo. **Keep this file current:** when a
decision, pattern, or hard-won lesson changes, update it in the same PR.

## What this repo is

A customer-facing reference for **millisecond HL7 v2 processing** on Azure Databricks with Spark
**Real-Time Mode (RTM)**, Lakeflow Spark Declarative Pipelines (SDP), and Azure Event Hubs, reached through a
Unity Catalog service credential. It has to **prove** low latency with measurements, so latency claims require data
(see "Measuring latency").

```
HOT PATH (RTM, Event Hubs topics between every layer)
  raw_hl7 ─► bronze ─► bronze_hl7 ─► silver ─► silver_{messages,encounters,observations,orders,diagnoses} | quarantine_hl7
LAKEHOUSE PATH (SDP micro-batch, off the hot path)
  every topic ─► the Delta streaming table of the same name (audit, replay, BI) + per-record latency telemetry
```

- Layers are **raw → bronze → silver**, each both an RTM Kafka topic and an SDP streaming table. Silver is one topic and
  table per entity. There's no gold layer on the hot path: nothing there needs a join, so silver writes the entity rows
  itself (one hop less). `gold` is kept for future business views built off the hot path.
- **Subscriptions** over silver: apps register Databricks SQL rules at runtime through the Subscription API (a Databricks
  App; the rules live in a Lakebase registry), a micro-batch hub pipeline
  delivers matches to each app's own hub, and a Java app (`examples/subscribers/java`) shows the client side. See
  "Subscription rules" and docs/subscriptions.md. The earlier SQL-file design is in `examples/subscriptions/`.
- Phases 0–5 were built on an AWS workspace (cross-cloud to Event Hubs); the repo now targets one Azure region end
  to end. New work: one branch + one PR per unit, verified on dev before merging.

## Non-negotiable principles

1. **Everything environment-specific is configurable.** That covers namespace, topic names, service
   credential, catalog/schema, offsets, rates, and sizing. Bundle variables (`databricks.yml`) flow into
   pipeline `configuration:` as `hl7.<key>` and are read with `hl7rt.config.conf(spark, "<key>")`.
   `tests/test_config_wiring.py` enforces this. Never hardcode names in `src/`.
2. **A topic is named after the Delta table it lands in: `<catalog>.<schema>.<table>`.** Bundle topic variables
   default to `${var.catalog}.${var.schema}.<table>`, Terraform (`infra/azure`) names hubs from `catalog`/`schema`/`tables`,
   and the mirror lands each topic in the table of that name. New layers follow the same rule.
   `tests/test_topic_names.py` enforces it. Renaming a hub recreates it (data is dropped), so pipelines then need
   a full refresh (new checkpoints).
3. **No keys or connection strings, anywhere.** Event Hubs auth is Entra ID through the Unity Catalog service credential
   `<catalog>_eventhubs` (the access connector's managed identity, with Event Hubs Data Sender + Receiver). The namespace has SAS
   keys disabled (`local_authentication_enabled = false`). Spark uses the Kafka option `databricks.serviceCredential` and must set
   no `kafka.sasl.*` / `kafka.security.*` options (they replace it). confluent-kafka generators use OAUTHBEARER with tokens from
   `dbutils.credentials.getServiceCredentialsProvider`. `tests/test_no_secrets.py` enforces it.
4. **Native Spark for parsing; no JARs or UDFs on the hot path.** HL7 v2 is delimited text, so use
   `split` / `try_element_at` / `try_to_timestamp`. A full parse costs less than the 16 ms measurement
   resolution.
5. **Delta is never an RTM source or sink.** RTM hops go Kafka → Kafka. Delta tables are written by
   separate non-RTM SDP pipelines that read the topics.
6. **Measure before claiming.** Performance "improvements" must be benchmarked against the original.
   Phase 1's first "improved" silver was 2.4× *slower* until measured and fixed.
7. **Honesty in reporting.** Report p99 *and* max, say what wasn't verified, and correct earlier
   conclusions in `docs/latency.md` when evidence changes them.

## Repo layout

```
databricks.yml        variables + targets; include: resources/**
resources/<type>/     bundle resources grouped by type: jobs/, pipelines/, schemas/, dashboards/
src/hl7rt/            shared library: pure DataFrame transforms, Event Hubs helpers, generators (unit-tested)
src/pipelines/<name>/ thin SDP sources that call into hl7rt (pipeline root_path = src/)
src/dashboards/       the AI/BI dashboard definitions (.lvdash.json); powerbi/: the live Power BI dashboard's views and
                      its model + report as data (definition.py)
infra/azure/          Terraform for everything Azure: workspace (+ metastore assignment), ADLS, access connector,
                      Event Hubs + hubs, RBAC. tfvars/state gitignored (local state).
infra/uc.sh           idempotent Unity Catalog bootstrap (`make uc`): credentials, external location, catalog
infra/registry.py     idempotent registry bootstrap (`make registry`): Lakebase project, schema, app roles, UC views, API grants
infra/fabric/         Terraform for the Power BI dashboard's Fabric capacity (its own tenant + subscription; tfvars gitignored)
infra/powerbi.py      idempotent Power BI deploy (`make powerbi`): views, read-only SP grants, Fabric workspace, model, report
src/registry/         the subscription registry: schema, owner trigger, RLS, API functions, grants (registry.sql)
src/subscription_api/ the Subscription API (Databricks App, FastAPI): the integration contract (OpenAPI), caller, shape
                      check, Databricks compiles, store, wait
examples/             clients and parked designs, not part of the platform:
                      subscribers/python/ and subscribers/java/ (the same example subscriber twice: `uv run python ...`
                      from the repo root, or Maven + `java -jar`; both API clients are generated from docs/api/openapi.json;
                      unit-tested by `make test`, not in the wheel or bundle sync); subscribers/apps/ (their settings,
                      written by `make registry`, gitignored);
                      subscriptions/ (earlier SQL-file design; not deployed, linted, or tested)
tests/                pytest with local Spark (.venv via uv); test_registry.py uses a throwaway local PostgreSQL
docs/                 integration.md + api/openapi.json (the contract), latency, subscriptions, SCALABILITY test plan,
                      Epic JSON, story.html (interactive deck),
                      SPIKE-sql-channels (full-SQL design spike, not built);
                      diagrams/ (.drawio + PNG + slides)
```

- Put logic in `src/hl7rt` as functions of DataFrames, so it's testable without a cluster. Keep
  `src/pipelines/*` to decorators + wiring.
- **Keep the repo to what's deployed or tested.** `src/` is the framework only. Clients of it (the Java subscriber) and
  parked designs go to `examples/` with a README. Throwaway experiments stay outside the repo folder.
- Generators are console scripts in the `hl7rt` wheel. The bundle `artifacts:` block builds it into
  `.build/` (gitignored), and it runs as serverless `python_wheel_task` jobs with
  `environment_version: "3"` (Python 3.12).
- **Terraform owns Azure; the Databricks CLI owns Unity Catalog.** UC objects live in the shared regional metastore and
  outlive the weekly sandbox wipe, so Terraform state for them would go stale. `infra/uc.sh` creates or repoints them
  and recreates a catalog built for an earlier workspace (catalog property `hl7.workspace_id`). It refuses to touch a catalog
  it didn't create. Azure names are deterministic (prefix-based) so a rebuild reuses the same storage URL.
- **Replaying a wipe (verified 2026-11-01, real Sunday wipe).** `make up` handled stale Terraform state by itself
  (resources seen as deleted, 18 recreated, metastore assignment replaced). Three things needed handling, and
  `tests/test_infra.py` guards each:
  - **The old managed resource group can survive the wipe or a workspace delete.** Azure's default name
    (`databricks-rg-<resource group>`) is fixed and ignores the region, so the westus2 → eastus2 move failed with
    "Resource group already exists in location 'westus2'". The name is `databricks-rg-<prefix>-<location>`. A leftover in
    the *same* region still blocks; deleting the failed workspace (`az databricks workspace delete`) removes its managed
    RG.
  - **An ISOLATED catalog stays bound to the deleted workspace.** The new workspace can't see it, but create says it
    exists. `uc.sh` adds a binding for the new workspace, then drops and recreates the catalog.
  - **A recreated access connector keeps its resource ID but gets a new managed identity.** Unity Catalog fails with
    "storage credential was not found ... access connector may have been deleted or recreated" until the credential is
    updated. `uc.sh` records the connector's principal ID (Terraform output `access_connector_principal_id`) in each
    credential's comment and updates on a mismatch.

## Real-Time Mode rules (GA on SDP; see docs/latency.md)

- **Use the shared wiring:** every RTM hop uses `hl7rt.rtm` (`read_topic`, `sink_options`, `flow_conf`). Don't hand-roll
  Kafka options per pipeline. `tests/test_rtm_resources.py` enforces continuous, serverless, no cluster spec, the GA
  (`CURRENT`) channel, `maxPartitions` set, and no development-mode targets. Event Hubs client settings follow Microsoft's guidance
  (`eventhubs._JAVA_CLIENT` / `_LIBRDKAFKA`: metadata and idle < 240 s), enforced by `tests/test_eventhubs.py`.
- **Route, don't fork:** a sink created without a `topic` routes each record by its `topic` column. Silver
  writes the silver and quarantine topics from **one** flow (one read, no extra slots), not two flows.
- **Enable it in SDP:** pipeline `continuous: true` + `spark.databricks.streaming.realTimeMode.enabled: "true"`
  in `configuration`, and `@dp.update_flow(target=<kafka sink>, spark_conf={"pipelines.trigger": "RealTime",
  "pipelines.trigger.interval": "5 minutes"})`. Sinks are `dp.create_sink(name, "kafka", {..., "topic": ...})`.
  The topic is static, so use one sink per output topic.
- **One RTM flow per pipeline, one pipeline per cluster.** Flows that share slots contend.
- **Compute: serverless, `CURRENT` channel (GA).** Databricks sizes compute. Flows reach RUNNING within about a minute,
  where classic clusters took ~9 min.
  - On this channel each source's reader task count is fixed, so set `maxPartitions` to the hub's partition count
    (`rtm_max_partitions`).
  - Several RTM flows per pipeline without slot contention, and adaptive source partitioning, exist only on the
    `PREVIEW` channel (Beta). Don't use them until they're GA.
- **Classic compute (phases 0–5; latency history linked from docs/latency.md)** needed fixed workers, Photon off, and
  slots ≥ tasks across all stages plus ~20% headroom. With exactly as many slots as tasks, the epoch handoff failed with
  `CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT`. It doesn't apply on serverless.
- **Never use bundle `mode: development`** for RTM targets: it turns `continuous` off.
- **A first non-RTM run leaves a v1 checkpoint** → `CHECKPOINT_FORMAT_V1_NOT_SUPPORTED`. Fix with a
  full refresh.
- **`asyncProgressTrackingEnabled` is rejected on SDP classic** (standard access mode). It isn't needed:
  async progress tracking and async state checkpointing are on by default in DBR 18.3.
- **Supported in RTM pipelines (GA docs, `ldp/real-time`):**
  - Stateless ops, `explode`, tumbling/sliding windows, aggregations, and `dropDuplicates`.
  - `transformWithState` (row API, no event-time timers), broadcast (stream-static) joins, `union`, and `forEach` sinks.
  - Output mode is update only.
- **Not supported:** stream-to-stream joins, Delta source/sink, `foreachBatch`, session windows, `mapPartitions`,
  `flatMapGroupsWithState`, and `transformWithStateInPandas`.
  - The parked `examples/subscriptions` join ran on classic before GA. "Event A then event B" subscriptions need
    `transformWithState`, or micro-batch.
- **Epoch boundaries pause by design.** Every `pipelines.trigger.interval` the engine commits offsets, checkpoints, and
  plans the next epoch (driver: `Interbatch latency for batch N`, ~1–1.7 s). The next epoch then opens new Kafka
  consumers. There is no zero-pause mode.
  - On AWS a boundary hit ~0.1% of messages at 5 min.
  - On Azure it delays ~2 s of traffic (~0.7% per 5-minute epoch), enough to push a 12-minute p99 over 200 ms. So the
    interval is **15 minutes** (`rtm_trigger_interval`). Longer means fewer pauses but bigger replays after a failure.
  - We set `kafka.fetch.max.wait.ms=50` on RTM readers (runbook).
- **Hung Event Hubs connections (Azure).** A connection occasionally stops answering until `request.timeout.ms`
  expires; that took 18–77 s with Microsoft's 60 s setting. The RTM hop stalls for the whole time, and executor logs
  show `Cancelled in-flight PRODUCE request ... due to node N being disconnected`. `rtm.HUNG_REQUEST_GUARD` (5 s, RTM
  readers and sinks, and the subscription hub's writes) caps it at ~5 s. Other micro-batch pipelines keep 60 s.
  Without it the hub's write hung for the 2-minute delivery timeout and failed the flow (seen live, Nov 6–7).
- **Entra auth isn't the latency cost.** An A/B on the same path measured new-connection setup with the service
  credential versus SAS: write 285 vs 291 ms, read 1315 vs 1211 ms. The token comes from cache. The logged
  `UnsupportedCallbackException` (`retrieveCustomExtensions`) is harmless.
- **Event Hubs Premium is required.** Standard's multi-tenant tail missed 200 ms p99 on two hops.
  - Size it by namespace CPU (`az monitor metrics` NamespaceCpuUsage). In westus2, 1 PU ran at ~83% and every hop's
    tail grew. 4 PU (the default) also halves the namespace-wide ~300 ms stalls seen at 2 PU.
  - Capacity wasn't the issue for Phase 3's partition stall (client config was), and it doesn't fix hung connections.
- **Workspace, Event Hubs, and storage share one Azure region** (`location` + `metastore_id`, default eastus2 with
  `fevm-shared-sandbox-eastus2`). Phases 0–5 ran AWS us-east-1 → Azure eastus, crossing clouds on every hop (~8 ms
  produce round trip). The latency results were measured in westus2.
  - **A region's features depend on its control-plane shard, not just the region.** New westus2 workspaces land on the
    new `westus2-c3` shard (`dig <workspace host>`), where Lakebase Autoscaling was still off (Nov 2026: "Lakebase
    Autoscaling is not enabled in this region"), while older westus2 workspaces have it. We moved to eastus2. Check
    the features you need on a fresh workspace before committing to a region.

## Subscription rules (see docs/subscriptions.md)

- **Identifiers are case-insensitive, as in Databricks.** `rules.parse` normalizes them to lowercase, or the hub's
  case-sensitive grant check would fail a rule Databricks accepted.
- **Rules are Databricks SQL, and Databricks compiles them.** The Subscription API (`src/subscription_api`, a Databricks
  App) runs `DESCRIBE QUERY` against the caller's own views and passes Databricks' errors through unchanged. The only
  rule logic of our own is the shape check (`rules.py`): one SELECT of columns, from one silver source, with a WHERE. No
  joins, subqueries, CTEs, set operations, aggregates, parameters, or functions that reach outside the record.
  - **Why the shape check:** the hub evaluates the WHERE with its own permissions. In the spike, a `(SELECT ...)` inside
    app SQL ran with the hub's permissions.
  - **Keep it small.** Don't add custom validation that Databricks already does. Joins and cross-event logic become new
    platform-built sources, not app rules.
- **The hub evaluates exactly what the SQL editor would.** It runs the stored WHERE with `F.expr` over the record.
  `tests/test_subscriptions.py` compares the hub with `spark.sql` for each operator, NULLs included. The hub re-checks
  each rule (shape plus granted columns) before running it, and a rule that fails on data is FAILED alone.
- **Grants live in Unity Catalog.** Each app's views `<catalog>.sub_<app>.<source>` expose only its granted fields
  (`make registry`, from `subscriber_apps`). The API's SP may read them; apps don't need to. Filtering on a column is
  reading it, and an ungranted column is Databricks' own `UNRESOLVED_COLUMN`.
- **The API is the only way in.**
  - **Identity:** the caller is the `X-Forwarded-Email` the Apps proxy sets (the SP's application ID). The proxy
    overwrites a forged copy (verified live).
  - **Registry access:** app roles are NOLOGIN and granted only to `subscription_service` (the API's SP). The API writes
    with `SET LOCAL ROLE <app>`, so the owner trigger and RLS see the app.
  - **No other path:** the Lakebase Data API is off, and old SP login roles are dropped. E2E check 7 and
    `tests/test_registry.py` guard this.
- **Postgres 16+:** the role's creator doesn't get `SET` on it. Even the registry owner can't act as an app without an
  explicit `GRANT <app> TO <me> WITH SET TRUE`, which the perf seeding does and then revokes.
- **The API is stateless and horizontally scaled:** a fixed 2 instances (`app_min_instances` = `app_max_instances`).
  - **Why fixed:** load-based autoscaling (min < max) isn't enabled for this workspace ("Autoscaling is not enabled for
    this workspace"). The Apps team plans it as a private preview, with 2–3 minute scale-up.
  - **Updating the count:** instance counts change through the new update API (`databricks apps create-update`), not
    `PATCH`.
- **Each app reads only its own hub:** its own access connector with Data Receiver on that hub, behind its own UC
  service credential, ACCESS granted to its SP only (`subscriber_apps` in Terraform, then `make uc`).
- **No silent states in the app.** It exits non-zero on a refused or failed rule, an unconfirmed activation, a stopped
  hub (heartbeat), a non-SP identity, or an app that isn't onboarded. It prints only the revisions it just registered.
  - **It never skips a match.** Its consumer group resumes from its last commit, so after time away it works through the
    backlog and reports it as a count ("catching up" / "caught up") before printing live matches. Re-registering an
    identical rule keeps the revision and prints `(unchanged)`.
  - **Running the app as an SP rewrites that SP's subscriptions** to match the code it was built from. Use `--e2e`
    (only `e2e-*` names) or another app for checks while someone is using that SP. Don't rebuild a jar someone is
    running: it crashes with `NoClassDefFoundError`.
  - **Library logs are at WARN** (`simplelogger.properties`), so the demo output stays readable.
- **The platform's integration point is the API contract, not the Java app.** `docs/api/openapi.json` is generated from
  the API, and `tests/test_openapi.py` fails if they differ: regenerate it with `uv run python tests/test_openapi.py`
  after any API change. Every operation has an `operationId` (clients' method names: declare, getStatus,
  putSubscription, cancel, validate, health) and a typed response. The Kafka message is in it too (`Notification`), and
  `tests/test_subscriptions.py` checks the hub writes exactly that schema. Version 1 only adds fields: the schemas carry
  no `additionalProperties`, so generated clients stay plain classes and ignore unknown fields. The proxy-set identity
  header isn't a client parameter. docs/integration.md is the language-neutral guide: handshake, Event Hubs
  token, Kafka settings, notification envelope, and errors. Keep it in step with the API.
- **An app configures only its service principal and `api.app`** (the API's App name; it looks up the URL with
  `apps.get`, which CAN_USE allows, so a rebuilt workspace needs no change). `GET /api/v1/connection` returns the
  rest: its topic, the Kafka bootstrap servers (from the App's `HL7_EVENTHUBS_NAMESPACE`), its UC credential (`apps.eventhubs_credential`,
  written by `make registry`), and the token scope. Don't add client settings the API can answer.
- **The Java client is deliberately small and contract-driven**, with packages by role so a reader starts at the top:
  - `App` (entry point) and `MySubscriptions` (name → SQL, the file developers edit) at the root;
  - `api/`: `SubscriptionsApi` and `api.model` are **generated** at build time from docs/api/openapi.json
    (openapi-generator, `native` library). `api/Api.java` only wires it: URL, the SP's token per call, reading refusals.
    Never hand-write a request or response class;
  - `notifications/Listener` (the delivery contract, on the generated `Notification`);
  - `platform/EventHubs` (UC token, OAUTHBEARER, consumer settings);
  - `e2e/E2E`.

  Keep logic on the server. Add a package only for a new role, not per class.
- **The Python client mirrors the Java one, module for module** (`examples/subscribers/python`: subscriber, my_subscriptions,
  api, notifications, eventhubs, e2e), with the same output, exit codes, and 25 checks. `build.py` generates its client
  with openapi-python-client into `generated/` (gitignored) whenever the contract is newer; never hand-write a model there
  either. `tests/test_python_subscriber.py` runs the generated client against the real API in-process, so an API change
  that breaks clients fails `make test`. Keep both examples' `MySubscriptions` the same: they declare the SP's whole set.
  - **librdkafka reports a topic it may not read only in the topic's metadata** (subscribing alone stays silent), so
    the E2E read check asks for metadata first. Java's client raises it on subscribe.
- **Lakebase drops connections** (idle close, suspend after 5 idle minutes, compute restarts). A pooled API
  connection whose first statement hits a network error is replaced, and one that failed is never pooled (a dead one
  once made every API call 500 for days); the hub's client reopens and retries its idempotent statement once.
  `tests/test_subscription_api.py` and `tests/test_hub_registry.py` kill the connection to prove it.
- **The hub is micro-batch on the PREVIEW channel** (`foreach_batch_sink`; RTM has no foreachBatch). Its flows run
  batches concurrently, so the shared registry client serializes access (`tests/test_hub_registry.py`). Use pg8000:
  psycopg[binary] aborted the serverless Python process. Serverless has no `persist`/`cache`, so a failed batch is
  recomputed to find the failing rule.
- **The hub runs at most 1,000 enabled rules** (`subscription_registry.limits`; the trigger serializes the count with an
  advisory lock, and an upsert of an already-enabled name isn't a new rule). At the cap it delivers p99 ~7 s
  (docs/SCALABILITY.md has the measured steps). Keep it fast with these rules:
  - **Compile per registry change, never per batch.** The hub compiles a source's rules in a background thread, and
    batches use the last compiled set (`_RuleSet`). `hub_progress` records each batch's phases: read it before
    optimizing.
  - **Don't build expressions with PySpark `Column` calls per rule.** Each call is a round trip to the driver (about
    70 ms per rule). Build one `F.expr` string per chunk, from checked parts only: the WHERE regenerated from its
    parsed tree, quoted identifiers (`rules.identifier`), and escaped literals (`rules.literal`).
  - **Simple rules are data, joined through an equality index** (`clauses()` in rules.py, `_data_matches`). Without the
    index, a cross join of records and rules was 10× slower. Anything not simple keeps the expression path. Both paths
    must match `spark.sql` and send identical notifications (`tests/test_subscriptions.py`).
  - **Read each micro-batch once** (collect into a local DataFrame; batches are bounded by `maxOffsetsPerTrigger`).
    Each union branch over the source would read it again.
  - **Serverless doesn't allow `persist`/`cache` or Spark codegen settings.** Restructure the plan instead.
  - **A running continuous pipeline doesn't pick up changed source files on deploy** unless its definition changed.
    Restart it (stop + start-update; it resumes from its checkpoint).
- **The Apps proxy may cut long requests.** Clients poll `?wait` in short calls (≤25 s); the API caps each call at
  60 s.
- **Monitoring reads the registry through Unity Catalog.** `make registry` registers the database as `<catalog>_registry`
  (`w.postgres.create_catalog`). The subscriptions dashboard reads the `subscription_monitor` views (platform only;
  nothing granted to `subscriber`). Views there are dropped and recreated: `CREATE OR REPLACE VIEW` can't change a
  column's type. `tests/test_dashboard.py` keeps both dashboards portable.
- **The Databricks SDK lets `DATABRICKS_*` environment variables override a profile.** The app pins
  `auth_type=oauth-m2m` from its profile and refuses to run as anything but a service principal.
- **`--e2e` is the acceptance test** (25 live checks, local or as the subscriber bundle's `e2e` job). Add a check there for
  every new guarantee, and keep docs/subscriptions.md's table in step. Plans beyond one SP: docs/SCALABILITY.md.
- **Serverless JAR tasks need environment_version 4+** (`java_dependencies`). Binding a job's `run_as` to an SP needs
  the deployer to have **Service Principal: User** on it, so the app's job is its own bundle (`examples/subscribers/java`):
  in the platform bundle it made every `make demo` fail for deployers without that role.
- **Maven Central is blocked on the Databricks network.** Use the internal mirror in `~/.m2/settings.xml`
  (`maven-proxy.dev.databricks.com`). Keep it out of the repo: customers won't have it.

## Data contracts

- **Silver is one row per entity** (`hl7rt.silver`):
  - `silver_messages`: one per message;
  - `silver_encounters`: ADT;
  - `silver_observations`: one per OBX, with the LOINC/alternate code;
  - `silver_orders`: ORM;
  - `silver_diagnoses`: one per DG1, with code, system, and type.

  Every row starts its entity fields with `CONTEXT` (patient, visit, header) and ends with `LINEAGE` (`raw_ts`,
  `bronze_ts`, `raw_partition`, `raw_offset`). So a rule on one entity needs no join: real criteria ("ADT A04 at
  these points of care with this diagnosis") are one filter. Repeating segments carry `position` (1-based), the upsert
  key with the raw coordinates. HL7 set IDs are optional, positions always exist.
- **Column names are the Epic silver contract's** (the names real subscription criteria use, docs/examples/criteria.md).
  The flat "first OBX/OBR" columns are gone: rules run on entity rows. The Epic JSON pipeline (`hl7rt.epic_json`) keeps
  its own 34-column contract. The HL7 field → column mapping is documented at the top of `hl7rt/hl7v2.py`.
- Build HL7 test messages by field number (`hl7rt.generators.hl7v2.segment`), never by hand-counting pipes.
  One hand-written golden message checks the builder.
- **One silver flow writes every entity topic.** It parses each message once, builds an array of `(topic, value)`, and
  `explode`s it. The sink routes by `topic`. Entity JSON uses `to_json(..., {"ignoreNullFields": "false"})` so every
  record has the full shape subscribers code against. (Spark's default drops null fields.) After going live, change an
  entity schema only additively. The single flow's plan is about the size of the old silver flow's alone
  (`tests/test_silver.py` guards it).
- **Silver tables carry `_meta`** (record_id, source_ts, raw_ts), a stable record identity for downstream consumers.
- Each hot-path record carries the upstream broker timestamps (`raw_ts`, `bronze_ts`, ...). The lakehouse mirror
  (`hl7rt.lakehouse`) turns them into `hop_ms` / `e2e_ms` columns.

## Measuring latency

- Event Hubs stamps every record with **LogAppendTime** (`timestampType = 1`). Each layer carries the
  upstream topic's broker timestamp forward (`raw_ts`, `bronze_ts`, `silver_ts`, `source_ts`), so
  `hop_ms = this_topic.timestamp - upstream_ts` uses broker clocks only, with no client clock skew.
- **The producer → raw hop crosses clocks** (generator host vs broker), so it can read slightly negative.
  Every other hop compares broker timestamps only.
- **Resolution is ±16 ms** (Event Hubs timestamps tick in ~16 ms steps; broker clocks differ by a few
  ms). Readings of 0 ms are real "same tick" results, not bugs. For sub-16 ms precision use RTM's
  `processingLatencyMs`.
- **Telemetry is live:** the mirror's streaming tables carry `hop_ms` / `e2e_ms` per record. Query
  percentiles over them; don't build one-shot probe jobs.
- **Always check the pipeline event log for flow `WARN`/`ERROR` before reading latency.** A crash and
  restart looks exactly like a latency spike.
- **At-least-once, upserted.** The Kafka sink is at-least-once: a pipeline that restarts from its checkpoint
  re-writes records sent since the last commit (Phase 5: ~17k records). Topics may carry repeats; Delta doesn't.
  Every table is an AUTO CDC (SCD 1) upsert keyed by the record's raw-topic coordinates (`lakehouse.KEYS`), so a repeat
  updates its row. Don't key on `msg_control_id`: generators and sources can reuse it.
- **Exclude backlog replay:** after downtime, hops drain backlog at "minutes" of apparent latency.
  Filter on fresh records (`upstream_ts` within the window), or `make clear-data` for demos.
- **Break a bad tail down by Event Hubs partition before blaming Spark.** The mirror tables don't keep the output
  partition, so read the topic directly from SQL with `read_kafka(..., serviceCredential => '<catalog>_eventhubs')` and
  group `timestamp - upstream_ts` by `partition`. In Phase 3 one partition of two freshly recreated hubs had p99
  ~800 ms while the other partitions sat at ~45 ms, with no Azure throttling or errors (`az monitor metrics`:
  ThrottledRequests / ServerErrors). Signature: a slow record is followed, from the *same input partition*, by
  on-time records, so the stall is on the output partition, not the Spark task.
- **Follow Microsoft's Event Hubs Kafka client settings** (`eventhubs._JAVA_CLIENT` / `_LIBRDKAFKA`). With Kafka
  defaults (`metadata.max.age.ms` 300 s, `connections.max.idle.ms` 540 s, both above Azure's 240 s idle close), one
  output partition per hub stalled 0.5–1 s and pushed p99 to ~550–875 ms. With the settings, the same load gives
  35–47 ms. Our first theory (low per-partition rate) was wrong: check client config before blaming the service.
- **Uneven keys are expected and harmless.** Records are keyed by patient MRN (generator default 50,000 patients,
  `--patients`), so partitions differ slightly. A slow partition with *fewer* records isn't overloaded.
- **The target bar:** every hop under 200 ms at **p99**, over a soak that spans at least 2 epoch
  boundaries. Report max separately.

## Operating the demo

- **Targets:** `make up` (Azure + `make uc`), `make demo dev` (deploy + start all pipelines and both feeds),
  `make clear-data dev`, `make stop dev` (stops compute billing). Deploy starts new or changed continuous pipelines;
  `demo` starts the rest.
- **`make up`** builds or rebuilds Azure (Terraform plan → you confirm → apply) and then runs `make uc`. The FE sandbox is
  wiped every Sunday: rerun `make up`, then `make demo dev`. The Makefile reads the workspace URL, `catalog`, and
  `eh_namespace` from Terraform outputs (`DATABRICKS_HOST`, `BUNDLE_VAR_*`) and authenticates with the Azure CLI, so a new
  workspace needs no login or config edits. `uc.sh` clears the local bundle cache when it recreates the catalog.
- **`make clear-data <env>`** empties the Delta tables (full refresh of the lakehouse mirror) without touching RTM
  pipelines. Use it after downtime so backlog catch-up doesn't skew the dashboard.
- **Don't restart RTM pipelines between tests.** A restart replays from the checkpoint and skews latency. Change load,
  not pipelines.
- Spark's Kafka source **resumes from its checkpoint, not the consumer group**. `kafka_starting_offsets`
  only applies to a new checkpoint. A full refresh (`databricks bundle run --full-refresh-all`) starts at the live
  edge, for demos only. Production must resume from the checkpoint so no HL7 message is skipped.
- **Dashboard** (`resources/dashboards/hl7_latency.yml` → `src/dashboards/hl7_latency.lvdash.json`): queries use
  unqualified table names, and the bundle sets `dataset_catalog` / `dataset_schema`. The warehouse is a
  `lookup` by name per target. `tests/test_dashboard.py` enforces portability and that every table it reads is
  produced.
- **Machines that sleep stall background soaks.** Wrap long local waits in `caffeinate -i` (macOS).
- **One environment per `infra/azure/terraform.tfvars`** (gitignored). The repo carries only the `.example`. Your
  sandbox values and a customer's never meet in git.

## Spark patterns and pitfalls

- **Watch for `grows beyond 64 KB` in driver logs** (classic only; serverless has no cluster log delivery). If a projection's whole-stage code exceeds the JVM method
  limit, the failed compile is retried at every RTM epoch boundary (silver: +3 s per boundary, p99 407 → 125 ms
  once fixed). Set `rtm.NO_WHOLE_STAGE_CODEGEN` on that flow. Execution already falls back to non-fused code. This **doesn't
  reproduce on local open-source Spark** (silver compiles fine there), so check the driver logs of every new RTM flow on dev.
- **Diagnose from the topics and the event log.** Serverless pipelines don't deliver driver or executor logs. Read latency
  per minute and per partition with `read_kafka` (see "Measuring latency"), and check the pipeline event log for flow
  WARN/ERROR. The classic investigation used driver logs (latency history).
- **Keep plans small: split once, read many.** Build parsed data in staged projections (`hl7v2.with_segments`):
  split the message into segments once and each needed segment into fields once, then read fields from those
  columns. Spark won't collapse a projection if that would duplicate an expensive expression. Inlining the
  segment split inside every field produced a **379 KB plan with 290 `split()` calls per record**. The RTM
  driver sat in `STARTING` with 55% GC. `tests/test_hl7v2.py::test_silver_plan_splits_each_segment_once` guards it.

- **ANSI is on** (Spark 4 / DBR 18): use `try_to_timestamp`, `try_element_at`, `try_cast`. A single
  malformed value with `to_timestamp` fails the whole stream.
- **Parse JSON once.** The optimizer pushes filters below projections and re-inlines `from_json`
  (seen: 5 parses per row). Wrap the parse in `inline(array(struct(...)))` as a barrier (see
  `epic_json._with_fields`), and verify with the optimized plan.
- **Parse timestamps once.** Pick the format from the value's shape (`rlike`) rather than trying
  formats in sequence (~5× slower).
- `map_from_entries` + `spark.sql.mapKeyDedupPolicy=LAST_WIN` over a reversed array = first occurrence
  wins. That conf is set via the table's `spark_conf` (`epic_json.SPARK_CONF`).
- **Delta stats cover the first 32 columns only.** A `cluster_by` column past #32 fails with
  `DELTA_CLUSTERING_COLUMN_MISSING_STATS`. Set `delta.dataSkippingStatsColumns` (see the silver tables in `mirror.py`).
- Tests run with `TZ=UTC` and `spark.sql.session.timeZone=UTC` (collected timestamps convert to the
  process zone).

## Live Power BI dashboard (see docs/powerbi.md)

- **DirectQuery on small views, never imports.** `src/dashboards/powerbi/views.sql` holds pre-aggregated views over a short
  window in `<catalog>.powerbi`, and each visual runs one query per refresh. `tests/test_powerbi.py` keeps the views
  reading only produced tables and monitor views, and the model and report consistent with them. `make powerbi` checks the
  declared columns against Unity Catalog.
- **The model and report are generated, not built in Desktop.** `definition.py` renders TMDL (model) and PBIR (report),
  deployed through the Fabric REST API (create, then `updateDefinition` in place). Host, warehouse, catalog, and model ID
  are filled in at deploy time.
- **Learn PBIR encodings from what Power BI saves, not from the schema.** The schema accepted a hand-written
  change-detection `pageRefresh` that the service ignored. Power BI saves a fixed refresh as `refreshType 'APR'`,
  `duration 'PT5S'`. To find a setting's encoding, set it in the UI, `getDefinition`, and copy that.
- **Live refresh needs our own capacity, and its admin minimum lowered by hand.** A new capacity caps automatic page
  refresh at 5 minutes, and only the Admin portal sets it (no Power BI REST or ARM setting). Shared capacity's floor is 30 minutes.
- **Cross-tenant auth is Databricks OAuth M2M.** That's a Databricks-managed SP's client ID and secret, entered as
  "Databricks Client Credentials". Entra SSO doesn't cross tenants. The SP needs the `databricks-sql-access` entitlement,
  or every call fails before any grant is checked. It gets SELECT on `<catalog>.powerbi` only, and views over the
  Lakebase-registered catalog work through that grant alone.
- **Each open report polls on its own** (~70 queries a minute per viewer at 5 s), so the warehouse stays up while anyone
  watches. Measure before raising viewers or lowering the interval.

## Workflow

- **Branches:** one per phase/unit of work, PR'd with summary / rationale / testing / "not verified".
  Stack PRs when the base isn't merged yet. Never commit to `main` directly.
- **TDD for library code:** write specs first (RED), then implement (GREEN).
- **Before pushing:** `make test`, `make lint`, `databricks bundle validate -t dev`. For pipeline changes,
  deploy to dev and verify against live data (row counts, quarantine reasons, event-log errors, latency),
  then `make stop dev`.
- **Infra:** `make up` shows the Terraform plan and applies exactly that plan only after you confirm.
  Never auto-approve. Azure sandbox policy requires an `owner` tag (`var.owner`).
- **Confirm before** creating paid or outward-facing resources (namespaces, tiers, new clusters), destroying
  anything, or forcing destructive bundle deploys (e.g. schema recreation). Don't bypass those prompts.
- Commit messages end with `Co-authored-by: Isaac <no-reply@databricks.com>`.
