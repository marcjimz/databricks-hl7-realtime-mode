<div align="center">
  <img src="./docs/images/dbx-banner.jpeg" alt="Databricks" style="width:100%; height:auto;">
</div>

<br>

# HL7 v2 on Spark Real-Time Mode

A Databricks-native framework for **dynamic runtime filtering** of clinical streams. Applications declare the HL7 events
they want in Databricks SQL while everything runs, and receive matches on their own topic within seconds. No pipeline
redeploy, and no code of theirs runs on the platform.

Underneath, raw HL7 v2 is parsed and split into clinical entities in tens of milliseconds per layer, and every layer
also lands in Delta. Built on
[Real-Time Mode](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming),
serverless Lakeflow Spark Declarative Pipelines, and Azure Event Hubs, connected through a Unity Catalog service credential, with
no keys or connection strings.

```
REAL-TIME MODE (ms), an Event Hubs topic between every layer
  HL7 ─► raw_hl7 ─► bronze_hl7 ─► silver_hl7 ─► gold_encounters / gold_observations / gold_orders
                                     └─► quarantine_hl7
STREAMING TABLES (seconds)
  every topic ─► the Delta table of the same name: <catalog>.<schema>.<table>
SUBSCRIPTIONS (seconds)
  gold_* ─► subscription hub (SQL rules registered at runtime via the Subscription API) ─► each app's own topic
```

- **Silver** parses HL7 with plain Spark into the 34-column Epic silver contract. Bad messages go to quarantine.
- **Gold** splits each message into encounters, observations (one per lab result), and orders.
- **Epic JSON** is a drop-in pipeline for the existing pre-parsed Epic feed ([docs/epic-json.md](docs/epic-json.md)).
- **Subscriptions** let apps choose gold records at runtime, each delivered to the app's own hub
  ([docs/subscriptions.md](docs/subscriptions.md)).

## Subscribe from any application

The interface for external apps is the **Subscription API** (OpenAPI: [`docs/api/openapi.json`](docs/api/openapi.json),
Swagger at `<api>/docs`) plus **the app's own Event Hubs topic**. Any language works. An app configures only two things,
its own Databricks service principal and the API's URL; the API tells it everything else:

```
0. GET  <api>/api/v1/connection           → its topic, Kafka bootstrap servers, Unity Catalog credential, token scope
1. PUT  <api>/api/v1/subscriptions        {"subscriptions": [{"name": "critical-potassium",
                                            "sql": "SELECT patient_id, value_num FROM gold_observations
                                                    WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5"}]}
        → ACCEPTED (revision, destination topic), or Databricks' own error and nothing applied
2. GET  <api>/api/v1/subscriptions?wait=25  → repeat until READY (the hub runs exactly these revisions, ~5 s)
3. Read the topic with any Kafka client (OAUTHBEARER, token from Unity Catalog for that credential);
   deduplicate on notificationId
```

Change the SQL and declare again: the new filter is live within seconds. The full handshake, notification format, and
errors: [docs/integration.md](docs/integration.md). The Java app in `examples/subscribers/java` is one example client, not part
of the platform.

## Results

Measured at 200 msg/s at full demo load on serverless Real-Time Mode (GA), with workspace and Event Hubs in one region:

| Hop | p50 | p99 |
|---|---|---|
| raw → bronze | 7 ms | **86 ms** |
| bronze → silver (full HL7 parse) | 0 ms | **78 ms** |
| silver → gold | 0 ms | **78 ms** |

Every hop is under 200 ms at p99. The max is a few seconds, from checkpoints and occasional Event Hubs connection
hangs. See [docs/latency.md](docs/latency.md).

## Set up, and rebuild after a sandbox wipe

Prereqs: Azure CLI (`az login` to the target tenant and subscription), Databricks CLI ≥ 0.285, Terraform ≥ 1.5,
[uv](https://docs.astral.sh/uv/). For the subscriptions demo you also need Java 17+ and Maven 3.9+.

```bash
make install
cp infra/azure/terraform.tfvars.example infra/azure/terraform.tfvars   # once per environment: fill in your values
make up                                                            # plan -> confirm -> apply, then Unity Catalog
```

- `make up` creates:
  - The Databricks workspace, assigned to the region's Unity Catalog metastore.
  - ADLS storage and an access connector.
  - Event Hubs Premium, with one hub per table.
  - A Unity Catalog catalog plus the storage and Event Hubs service credentials.
  - The Lakebase subscription registry, and each subscriber app's hub, identity, and credential.
- **It's safe to rerun.** If the resource group was deleted (the FE sandbox is wiped every Sunday), run `make up` again.
  Terraform recreates what's missing, and the catalog is recreated for the new workspace.
- **Your values stay local:** `terraform.tfvars` holds your subscription, tenant, metastore, and name prefix. It's
  gitignored. For another environment, such as a customer's, use a fresh copy of the example.

## Run the demo

```bash
make demo dev          # deploy, start all pipelines and the HL7 feeds, print the dashboard URL (serverless: ~2 min)
make clear-data dev    # once every hop shows data: drop start-up catch-up from the dashboard
make stop dev          # stop pipelines and feeds (Event Hubs keeps billing until the sandbox is wiped)
make help              # everything else
```

**Demo flow:**
1. Show the source: raw HL7 v2 (ADT, ORU, ORM, MDM) at 200 msg/s.
2. Show the hot path: an Event Hubs topic between each layer. Silver has the Epic silver columns. In Catalog Explorer,
   every topic has a streaming table of the same name.
3. Show the dashboard: p99 per hop (all under 200 ms), p99 by minute, critical labs reaching gold, and quarantine.
   `make demo dev` prints its URL, the workspace URL, and the subscriptions dashboard's URL.
4. Show security: Catalog Explorer → External data → Credentials → `<catalog>_eventhubs`. The pipelines hold no secrets,
   and the Event Hubs namespace has SAS keys disabled.

## Subscriptions demo

An application subscribes at runtime, as its own service principal, to the gold records it needs. A rule is plain
**Databricks SQL**. The app sends it to the **Subscription API** (a Databricks App), Databricks compiles it on the spot,
and the hub applies it at its next micro-batch, with no redeploy. Test a rule in the SQL editor first: the same query
returns the records the hub delivers.

- **The integration contract,** for any language: [docs/integration.md](docs/integration.md), with Swagger at `<api>/docs`.
- **How it works and its security model:** [docs/subscriptions.md](docs/subscriptions.md).
- **The story as an interactive deck:** [docs/story.html](docs/story.html).

This walkthrough uses the **Java example** (`examples/subscribers/java`, prerequisites in its
[README](examples/subscribers/java/README.md#prerequisites)) to drive that contract.

### Before you start

1. **Tools:** Java 17+ and Maven 3.9+ (`java -version`, `mvn -v`). If your network blocks Maven Central, point Maven
   at a mirror in `~/.m2/settings.xml`.
2. **An onboarded app:** a service principal in `subscriber_apps` (`infra/azure/terraform.tfvars`), then `make up`.
   That creates its views, its hub, and its credential, and writes its settings (no secrets) to
   `examples/subscribers/java/apps/<app>.properties`. One-time setup:
   [docs/subscriptions.md](docs/subscriptions.md#set-up-bring-your-own-service-principal).
3. **Its profile:** `[<app>]` in `~/.databrickscfg` with the SP's `client_id` and `client_secret`.
4. **Everything running:** `make demo dev`, which includes the subscription hub and the Subscription API.

Run every command below from the repo root.

### 0. Check everything end to end

```
$ mvn -q -f examples/subscribers/java package && java -jar examples/subscribers/java/target/hl7-subscriber.jar --e2e
…
| 1 | Sign in | profile sp-hl7-realtime-demo | a service principal | 38fafc55-… | PASS |
| 2 | Connection from the API | GET /api/v1/connection | this app's own topic and credential | hl7_rtm_demo.subscribers.sp-hl7-realtime-demo via sub_sp_hl7_realtime_demo_eventhubs @ hl7rtmaz-ehns.servicebus.windows.net:9093 | PASS |
| 7 | Skip the API (the registry's Data API) | list_subscriptions via https://… | not available | refused 503 | PASS |
| 8 | Column not granted | SELECT patient_id FROM gold_observations WHERE msg_sending_facility = 'NORTH' | UNRESOLVED_COLUMN | UNRESOLVED_COLUMN.WITH_SUGGESTION: … | PASS |
| 12 | Subquery | SELECT patient_id FROM gold_observations WHERE value_num > (SELECT max(…) …) | SQL_NOT_ALLOWED | SQL_NOT_ALLOWED: a subquery isn't allowed … | PASS |
…
| 24 | Cancel | DELETE e2e-potassium | CANCELLED by the hub | CANCELLED | PASS |
| 25 | Hub heartbeat | gold_observations | a batch within 2 minutes | 2026-11-06T01:36:22… | PASS |

25 checks, 0 failed
```

The 25 checks cover:
- sign-in, and credential and topic isolation;
- no way around the API;
- Databricks' own compile errors, and the shapes the hub won't run;
- one refusal applying nothing;
- register → active → notified → changed → cancelled.

[The list](docs/subscriptions.md#end-to-end-checks). The run takes about 2 minutes and exits 1 if any check fails. Run
it first: if it passes, the steps below will work.

### 1. Run the app

It declares what `examples/subscribers/java/src/main/java/com/databricks/hl7/subscriber/MySubscriptions.java` holds, waits for the
hub to confirm, and prints matches as they arrive:

```java
rules.put("critical-potassium", """
        SELECT patient_id, value_num, units FROM gold_observations
        WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5""");
```

```
$ java -jar examples/subscribers/java/target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-d28b-4f48-8d17-299f8d2698ee)
subscribed  critical-potassium     revision 1 ACCEPTED
            SELECT patient_id, value_num, units FROM gold_observations WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5
hub         critical-potassium     revision 1 ACTIVE
listening   on hl7_rtm_demo.subscribers.sp-hl7-realtime-demo as consumer group sp-hl7-realtime-demo (Ctrl-C to stop)
2026-11-05T05:02:15.775Z  critical-potassium     rev 1  {"patient_id":"MRN021417","units":"mmol/L","value_num":6.7}
2026-11-05T05:02:15.838Z  critical-potassium     rev 1  {"patient_id":"MRN037740","units":"mmol/L","value_num":7.1}
…
```

- **`ACCEPTED`** means Databricks compiled the rule against this app's own views in that call. **`ACTIVE`** means the
  hub is running exactly that revision. The query shown is the canonical form that's stored. The timestamp is when the
  gold record was produced.
- **Test it in the SQL editor:** `SELECT patient_id, value_num, units FROM hl7_rtm_demo.sub_sp_hl7_realtime_demo.gold_observations
  WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5`. Those are the records the hub delivers (live, not
  historical).
- **Revision numbers count every change** to that subscription. Running an identical rule again keeps its revision and
  says `(unchanged)`.
- **After time away, the app catches up first.** Its consumer group resumes where it last stopped and never skips, so
  the backlog is reported as a count (`catching up` … `caught up`). Matches for other subscriptions or earlier revisions
  are counted as `skipped`.

### 2. Add a subscription (and change one)

Stop the app with Ctrl-C first: rebuilding replaces the jar a running app loads its classes from, and it would crash.
Raise the threshold, drop `units`, and add a second subscription:

```java
rules.put("critical-potassium", """
        SELECT patient_id, value_num FROM gold_observations
        WHERE observation_cd_desc = 'Potassium' AND value_num >= 7.0""");
rules.put("low-hemoglobin", """
        SELECT patient_id, value_num, abnormal_flag FROM gold_observations
        WHERE observation_cd_desc = 'Hemoglobin' AND value_num < 8""");
```

```
$ mvn -q -f examples/subscribers/java package && java -jar examples/subscribers/java/target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-d28b-4f48-8d17-299f8d2698ee)
subscribed  critical-potassium     revision 2 ACCEPTED
            SELECT patient_id, value_num FROM gold_observations WHERE observation_cd_desc = 'Potassium' AND value_num >= 7.0
subscribed  low-hemoglobin         revision 1 ACCEPTED
            SELECT patient_id, value_num, abnormal_flag FROM gold_observations WHERE observation_cd_desc = 'Hemoglobin' AND value_num < 8
hub         critical-potassium     revision 2 ACTIVE
hub         low-hemoglobin         revision 1 ACTIVE
listening   on hl7_rtm_demo.subscribers.sp-hl7-realtime-demo as consumer group sp-hl7-realtime-demo (Ctrl-C to stop)
2026-11-05T05:07:07.691Z  critical-potassium     rev 2  {"patient_id":"MRN037433","value_num":7.1}
2026-11-05T05:07:07.770Z  low-hemoglobin         rev 1  {"abnormal_flag":"LL","patient_id":"MRN018101","value_num":7.9}
…
```

Potassium now arrives without `units`, and only at 7.0 or above, and `low-hemoglobin` matches arrive too.

### 3. Remove a subscription

Delete `low-hemoglobin` from `MySubscriptions` (or `git checkout` the file, which also puts potassium back to 6.5), then
rebuild and run. The code is the source of truth, so the API cancels what it no longer declares:

```
$ mvn -q -f examples/subscribers/java package && java -jar examples/subscribers/java/target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-d28b-4f48-8d17-299f8d2698ee)
subscribed  critical-potassium     revision 3 ACCEPTED
            SELECT patient_id, value_num, units FROM gold_observations WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5
cancelled   low-hemoglobin         (no longer in MySubscriptions)
hub         critical-potassium     revision 3 ACTIVE
listening   on hl7_rtm_demo.subscribers.sp-hl7-realtime-demo as consumer group sp-hl7-realtime-demo (Ctrl-C to stop)
…
```

The hub stops `low-hemoglobin` at its next batch (CANCELLED on the dashboard), and any of its matches still on the hub
are counted as `skipped`, not printed.

### 4. Ask for something you aren't allowed

Add a misspelled column, a column this app isn't granted, and a subquery. Databricks' own errors come back in the same
call, nothing is applied, and the app exits with code 2:

```java
rules.put("typo", "SELECT patient_id FROM gold_observations WHERE value_numm > 6");
rules.put("by-facility", "SELECT patient_id FROM gold_observations WHERE msg_sending_facility = 'NORTH'");
rules.put("with-labs", "SELECT patient_id FROM gold_observations WHERE patient_id IN (SELECT patient_id FROM gold_encounters)");
```

```
$ mvn -q -f examples/subscribers/java package && java -jar examples/subscribers/java/target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-d28b-4f48-8d17-299f8d2698ee)
refused     typo                   UNRESOLVED_COLUMN.WITH_SUGGESTION: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `value_numm` cannot be resolved. Did you mean one of the following? [`value_num`, `value`, `units`, `visit_id`, `patient_id`]. …
refused     by-facility            UNRESOLVED_COLUMN.WITH_SUGGESTION: … with name `msg_sending_facility` cannot be resolved. …
refused     with-labs              SQL_NOT_ALLOWED: a subquery isn't allowed: a rule is SELECT <columns> FROM <source> WHERE <filter>
ERROR       3 subscription(s) refused; fix MySubscriptions.java
```

- **Databricks only offers granted columns.** Its suggestions list only what this app may see, because it compiled
  against the app's own view.
- **Why a subquery is refused:** the hub runs the rule with its own permissions, so a rule is one filter over one
  record. Joins, subqueries, and functions that reach outside the record are refused. Remove the three lines before the
  next step.
- **The hub's rule cap:** the hub runs at most 1,000 rules across all apps (`RULE_LIMIT_REACHED` past that; cancel one
  first). It's a platform limit, in `subscription_registry.limits`.

### 5. Stop the hub

The app doesn't listen silently to a stopped hub. Stop the hub pipeline:

```bash
(export DATABRICKS_HOST=$(terraform -chdir=infra/azure output -raw workspace_url) DATABRICKS_AUTH_TYPE=azure-cli
 databricks pipelines stop $(databricks pipelines list-pipelines --filter "name LIKE 'hl7rt-dev-subscription-hub'" \
   -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["pipeline_id"])'))
```

Wait two minutes, then run the app. The API waits up to two more minutes for the hub, then the app exits with code 4:

```
$ java -jar examples/subscribers/java/target/hl7-subscriber.jar
…
hub         critical-potassium     revision 2 ACTIVE
hub         low-hemoglobin         revision 1 ACTIVE
ERROR       HUB_STALE: ["the hub hasn't processed gold_observations for 120 s"] (is the subscription_hub pipeline running? make demo dev)
```

Start the hub again with `make demo dev`. It resumes from its checkpoint, so no gold record is skipped.

### Watch it on the dashboard

The **HL7 subscriptions** dashboard (`make demo dev` prints its URL) shows each app's subscriptions, their SQL, and the
hub's state, read live from the registry. Refresh it after each step above to see revisions, cancellations, and the
hub's heartbeat change ([details](docs/subscriptions.md#monitoring)).

### Clean up

- `git checkout examples/subscribers/java/src/main/java/com/databricks/hl7/subscriber/MySubscriptions.java` restores the
  original rule.
- `make stop dev` stops the pipelines, the feeds, and the Subscription API.

### In the workspace

The same jar runs the E2E checks as a serverless job, as the app's service principal, with no secret. Binding the job
to the SP needs the **Service Principal: User** role on it (Settings → Identity and access → Service principals →
Permissions). Then:

```bash
export DATABRICKS_HOST=$(terraform -chdir=infra/azure output -raw workspace_url) DATABRICKS_AUTH_TYPE=azure-cli
DATABRICKS_BUNDLE_ROOT=examples/subscribers/java databricks bundle deploy
DATABRICKS_BUNDLE_ROOT=examples/subscribers/java databricks bundle run e2e
```

## Configure

| What | Where |
|---|---|
| Subscription, tenant, region, metastore, name prefix, catalog, Event Hubs capacity | `infra/azure/terraform.tfvars` |
| Topic names, cluster size, feed rate, checkpoint interval, warehouse, registry project | `databricks.yml` |
| Subscriber apps (service principal, sources, columns) | `subscriber_apps` in `infra/azure/terraform.tfvars` |

## Repo

```
infra/azure/     Terraform: workspace, storage, access connector, Event Hubs
infra/uc.sh      Unity Catalog bootstrap (make uc)
infra/registry.py  Lakebase subscription registry bootstrap (make registry)
databricks.yml   bundle variables and targets; resources/ has the pipelines, jobs, and dashboard
src/hl7rt/       the logic: HL7 parsing, gold, Event Hubs wiring, generators
src/pipelines/   thin pipeline definitions (including the subscription hub)
src/registry/    the subscription registry's schema, owner trigger, RLS, and API (Lakebase Postgres)
src/subscription_api/  the Subscription API (Databricks App): the integration contract; Swagger at <api>/docs
tests/           unit tests (local Spark)
docs/            integration guide + api/openapi.json (the contract), latency, subscriptions, scalability plan,
                 Epic JSON notes, story.html (deck), SPIKE docs; diagrams/ (draw.io + slides)
examples/        clients and parked designs: subscribers/java/ (an example subscriber; its API client is generated
                 from the contract), the earlier SQL-file subscriptions, the earlier AWS cross-cloud setup
```

Contributors and coding agents: see [AGENTS.md](AGENTS.md).
