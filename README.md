<div align="center">
  <img src="./docs/images/dbx-banner.jpeg" alt="Databricks" style="width:100%; height:auto;">
</div>

<br>

# HL7 v2 on Spark Real-Time Mode

Millisecond HL7 processing on Azure Databricks. Raw HL7 v2 is parsed and split into clinical entities in tens of
milliseconds per layer, and every layer also lands in Delta. Built on
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
  gold_* ─► subscription hub (rules registered at runtime in Lakebase) ─► each app's own topic
```

- **Silver** parses HL7 with plain Spark into the 34-column Epic silver contract. Bad messages go to quarantine.
- **Gold** splits each message into encounters, observations (one per lab result), and orders.
- **Epic JSON** is a drop-in pipeline for the existing pre-parsed Epic feed ([docs/epic-json.md](docs/epic-json.md)).
- **Subscriptions** let apps choose gold records at runtime, each delivered to the app's own hub
  ([docs/subscriptions.md](docs/subscriptions.md)).

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
4. Show security: Catalog Explorer → External data → Credentials → `<catalog>_eventhubs`. The pipelines hold no secrets,
   and the Event Hubs namespace has SAS keys disabled.

## Subscriptions demo

A Java app subscribes at runtime, as its own service principal, to the gold records it needs. Change the code, run it
again, and the new rule takes effect at the hub's next micro-batch, with no redeploy. How it works and its security
model: [docs/subscriptions.md](docs/subscriptions.md). Diagrams and a three-slide overview: [docs/diagrams](docs/diagrams/).

### Before you start

1. **Tools:** Java 17+ and Maven 3.9+ (`java -version`, `mvn -v`). If your network blocks Maven Central, point Maven
   at a mirror in `~/.m2/settings.xml`.
2. **An onboarded app:** a service principal in `subscriber_apps` (`infra/azure/terraform.tfvars`), then `make up`.
   That writes the app's settings (no secrets) to `src/subscriber/apps/<app>.properties`. One-time setup:
   [docs/subscriptions.md](docs/subscriptions.md#set-up-bring-your-own-service-principal).
3. **Its profile:** `[<app>]` in `~/.databrickscfg` with the SP's `client_id` and `client_secret`.
4. **The pipelines running:** `make demo dev`, which includes the subscription hub.

Run every command below from the repo root.

### 0. Check everything end to end

```
$ mvn -q -f src/subscriber package && java -jar src/subscriber/target/hl7-subscriber.jar --e2e
…
| 1 | Sign in | profile sp-hl7-realtime-demo | a service principal | 38fafc55-… | PASS |
| 2 | Own Event Hubs credential | sub_sp_hl7_realtime_demo_eventhubs | token issued | token, expires … | PASS |
| 3 | Another credential | hl7_rtm_demo_eventhubs | denied (no ACCESS) | PermissionDenied: … | PASS |
…
| 19 | Cancel | unsubscribe e2e-potassium | CANCELLED by the hub | CANCELLED | PASS |
| 20 | Hub heartbeat | gold_observations | a batch within 2 minutes | last batch … | PASS |

20 checks, 0 failed
```

The 20 checks cover sign-in, credential and topic isolation, every refusal, and register → active → notified →
changed → cancelled ([the list](docs/subscriptions.md#end-to-end-checks)). The run takes about 2 minutes and exits 1
if any check fails. Run it first: if it passes, the steps below will work.

### 1. Run the app

It registers what `src/subscriber/src/main/java/com/databricks/hl7/subscriber/MySubscriptions.java` declares, waits
for the hub to confirm, and prints matches as they arrive:

```java
new Subscription("critical-potassium", "gold_observations",
        field("observation_cd_desc").eq("Potassium").and(field("value_num").ge(6.5)),
        List.of("patient_id", "value_num", "units"))
```

```
$ java -jar src/subscriber/target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-d28b-4f48-8d17-299f8d2698ee)
subscribed  critical-potassium     revision 1 ACCEPTED
            (observation_cd_desc = 'Potassium') AND (value_num >= 6.5)
hub         critical-potassium     revision 1 ACTIVE
listening   on hl7_rtm_demo.subscribers.sp-hl7-realtime-demo as consumer group sp-hl7-realtime-demo (Ctrl-C to stop)
2026-11-04T18:42:49.611Z  critical-potassium     rev 1  {"patient_id":"MRN029394","value_num":6.6,"units":"mmol/L"}
2026-11-04T18:42:50.115Z  critical-potassium     rev 1  {"patient_id":"MRN048702","value_num":6.8,"units":"mmol/L"}
…
```

- **`ACCEPTED`** means the registry validated the rule in that call. **`ACTIVE`** means the hub is running exactly
  that revision. The timestamp is when the gold record was produced.
- **Revision numbers count every change** to that subscription, so on a second walkthrough expect higher numbers.
  Running an identical rule again keeps its revision and says `(unchanged)`.
- **After time away, the app catches up first.** Its consumer group resumes where it last stopped and never skips, so
  the backlog is reported as a count and only live matches are printed:
  ```
  catching up from 2026-11-04T18:42:01.468Z, where consumer group sp-hl7-realtime-demo last stopped
  caught up   163 backlog notifications (2026-11-04T18:42:01.468Z to 2026-11-04T18:43:05.839Z); live from here
  ```

### 2. Change the code and run it again

Stop the app with Ctrl-C first: rebuilding replaces the jar a running app loads its classes from, and it would crash. Raise the threshold, drop `units`, and add a second subscription:

```java
new Subscription("critical-potassium", "gold_observations",
        field("observation_cd_desc").eq("Potassium").and(field("value_num").ge(7.0)),
        List.of("patient_id", "value_num")),
new Subscription("low-hemoglobin", "gold_observations",
        field("observation_cd_desc").eq("Hemoglobin").and(field("value_num").lt(8)),
        List.of("patient_id", "value_num", "abnormal_flag"))
```

```
$ mvn -q -f src/subscriber package && java -jar src/subscriber/target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-d28b-4f48-8d17-299f8d2698ee)
subscribed  critical-potassium     revision 2 ACCEPTED
            (observation_cd_desc = 'Potassium') AND (value_num >= 7.0)
subscribed  low-hemoglobin         revision 1 ACCEPTED
            (observation_cd_desc = 'Hemoglobin') AND (value_num < 8)
hub         critical-potassium     revision 2 ACTIVE
hub         low-hemoglobin         revision 1 ACTIVE
listening   on hl7_rtm_demo.subscribers.sp-hl7-realtime-demo …
2026-11-04T18:46:31.732Z  low-hemoglobin         rev 1  {"patient_id":"MRN009013","value_num":7.9,"abnormal_flag":"LL"}
2026-11-04T18:46:31.545Z  critical-potassium     rev 2  {"patient_id":"MRN044207","value_num":7.1}
…
skipped     330 notifications for other subscriptions or earlier revisions
```

- **The change shows up in the output:** potassium now arrives without `units` and only at 7.0 or above.
- **`skipped`** counts matches still on the hub for revision 1 (or for the E2E run's subscriptions). They aren't
  printed.
- **Deleting a subscription from the code cancels it** on the next run (`cancelled  low-hemoglobin`).

### 3. Ask for something you aren't allowed

Add a misspelled column and a column this app isn't granted. Both are refused in the same call, and the app exits
with code 2 without listening:

```java
new Subscription("typo", "gold_observations", field("value_numm").gt(6), List.of("patient_id")),
new Subscription("by-facility", "gold_observations", field("msg_sending_facility").eq("NORTH"), List.of("patient_id"))
```

```
$ mvn -q -f src/subscriber package && java -jar src/subscriber/target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-d28b-4f48-8d17-299f8d2698ee)
subscribed  critical-potassium     revision 2 ACCEPTED (unchanged)
            (observation_cd_desc = 'Potassium') AND (value_num >= 7.0)
subscribed  low-hemoglobin         revision 1 ACCEPTED (unchanged)
            (observation_cd_desc = 'Hemoglobin') AND (value_num < 8)
refused     typo                   FIELD_NOT_IN_SOURCE: value_numm is not a column of gold_observations
refused     by-facility            FIELD_DENIED: msg_sending_facility is not granted to this app
ERROR       2 subscription(s) refused; fix MySubscriptions.java
```

Remove the two lines again before the next step.

### 4. Stop the hub

The app doesn't listen silently to a stopped hub. From the repo root, stop the hub pipeline:

```bash
(export DATABRICKS_HOST=$(terraform -chdir=infra/azure output -raw workspace_url) DATABRICKS_AUTH_TYPE=azure-cli
 databricks pipelines stop $(databricks pipelines list-pipelines --filter "name LIKE 'hl7rt-dev-subscription-hub'" \
   -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["pipeline_id"])'))
```

Wait two minutes, then run the app. It waits up to two more minutes for the hub, then exits with code 4:

```
$ java -jar src/subscriber/target/hl7-subscriber.jar
…
hub         critical-potassium     revision 2 ACTIVE
hub         low-hemoglobin         revision 1 ACTIVE
waiting     the hub hasn't finished a batch of [gold_observations] in 2 minutes; waiting up to 2 minutes
ERROR       the hub isn't processing [gold_observations]: start the subscription_hub pipeline (make demo dev)
```

Start the hub again with `make demo dev`. It resumes from its checkpoint, so no gold record is skipped. Its first batch
covers the time it was stopped, so the app's next run starts with "catching up".

### Clean up

- `git checkout src/subscriber/src/main/java/com/databricks/hl7/subscriber/MySubscriptions.java` restores the
  original rule. The next run cancels `low-hemoglobin`.
- `make stop dev` stops the pipelines and feeds.

### In the workspace

The same jar runs the E2E checks as a serverless job, as the app's service principal, with no secret. Binding the job
to the SP needs the **Service Principal: User** role on it (Settings → Identity and access → Service principals →
Permissions). Then:

```bash
export DATABRICKS_HOST=$(terraform -chdir=infra/azure output -raw workspace_url) DATABRICKS_AUTH_TYPE=azure-cli
DATABRICKS_BUNDLE_ROOT=src/subscriber databricks bundle deploy
DATABRICKS_BUNDLE_ROOT=src/subscriber databricks bundle run e2e
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
src/registry/    the subscription registry's schema, validation, and API (Lakebase Postgres)
src/subscriber/  the Java subscriber app
tests/           unit tests (local Spark)
docs/            latency results, subscriptions, scalability plan, Epic JSON notes; diagrams/ (draw.io + slides)
examples/        not deployed: the earlier SQL-file subscriptions, the earlier AWS cross-cloud setup
```

Contributors and coding agents: see [AGENTS.md](AGENTS.md).
