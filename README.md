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
[uv](https://docs.astral.sh/uv/).

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
again, and the new rule takes effect at the hub's next micro-batch, with no redeploy. One-time setup (the app's
service principal, its profile, and `subscriber.properties`): [docs/subscriptions.md](docs/subscriptions.md#set-up-bring-your-own-service-principal).

Start the pipelines (`make demo dev`), then:

**1. Run the app.** It registers what `src/subscriber/.../MySubscriptions.java` declares, waits for the hub to confirm,
and prints matches as they arrive:

```java
new Subscription("critical-potassium", "gold_observations",
        field("observation_cd_desc").eq("Potassium").and(field("value_num").ge(6.5)),
        List.of("patient_id", "value_num", "units"))
```

```
$ cd src/subscriber && mvn -q package && java -jar target/hl7-subscriber.jar
signed in   as sp-hl7-realtime-demo (38fafc55-…)
subscribed  critical-potassium     revision 1 ACCEPTED
            (observation_cd_desc = 'Potassium') AND (value_num >= 6.5)
hub         critical-potassium     revision 1 ACTIVE
listening   on hl7_rtm_demo.subscribers.sp-hl7-realtime-demo as consumer group sp-hl7-realtime-demo (Ctrl-C to stop)
2026-11-04T03:45:26.819Z  critical-potassium     rev 1  {"patient_id":"MRN030074","value_num":7.1,"units":"mmol/L"}
2026-11-04T03:45:27.210Z  critical-potassium     rev 1  {"patient_id":"MRN015765","value_num":6.9,"units":"mmol/L"}
…
```

`ACCEPTED` means the registry validated the rule in that call. `ACTIVE` means the hub is running exactly that revision.

**2. Change the code and run it again** (Ctrl-C first). Raise the threshold, drop `units`, and add a second subscription:

```java
new Subscription("critical-potassium", "gold_observations",
        field("observation_cd_desc").eq("Potassium").and(field("value_num").ge(7.0)),
        List.of("patient_id", "value_num")),
new Subscription("low-hemoglobin", "gold_observations",
        field("observation_cd_desc").eq("Hemoglobin").and(field("value_num").lt(8)),
        List.of("patient_id", "value_num", "abnormal_flag"))
```

```
$ mvn -q package && java -jar target/hl7-subscriber.jar
subscribed  critical-potassium     revision 2 ACCEPTED
            (observation_cd_desc = 'Potassium') AND (value_num >= 7.0)
subscribed  low-hemoglobin         revision 1 ACCEPTED
            (observation_cd_desc = 'Hemoglobin') AND (value_num < 8)
hub         critical-potassium     revision 2 ACTIVE
hub         low-hemoglobin         revision 1 ACTIVE
listening   on hl7_rtm_demo.subscribers.sp-hl7-realtime-demo …
skipped     296 notifications for earlier revisions (backlog on the hub)
2026-11-04T03:46:58.981Z  critical-potassium     rev 2  {"patient_id":"MRN013953","value_num":7.1}
2026-11-04T03:46:59.434Z  low-hemoglobin         rev 1  {"patient_id":"MRN013934","value_num":7.3,"abnormal_flag":"LL"}
…
```

Deleting a subscription from the code cancels it on the next run (`cancelled  low-hemoglobin`). Matches for
revision 1 that were waiting on the hub while the app was stopped are counted as skipped, not printed.

**3. Ask for something you aren't allowed.** A misspelled column and a column this app isn't granted are refused in the
same call, and the app exits with code 2:

```java
new Subscription("typo", "gold_observations", field("value_numm").gt(6), List.of("patient_id")),
new Subscription("by-facility", "gold_observations", field("msg_sending_facility").eq("NORTH"), List.of("patient_id"))
```

```
refused     typo                   FIELD_NOT_IN_SOURCE: value_numm is not a column of gold_observations
refused     by-facility            FIELD_DENIED: msg_sending_facility is not granted to this app
ERROR       2 subscription(s) refused; fix MySubscriptions.java
```

**4. Stop the hub** (`databricks pipelines stop <subscription_hub id>`), wait two minutes, and run the app. It doesn't
listen silently; it exits with code 4:

```
hub         critical-potassium     revision 2 ACTIVE
ERROR       the hub isn't processing [gold_observations]: start the subscription_hub pipeline (make demo dev)
```

Rules are structured predicates, never SQL text, so there's nothing to inject. Each app reads only its own hub, with a
token Unity Catalog mints for its own credential. The full security model is in [docs/subscriptions.md](docs/subscriptions.md#security-model).

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
docs/            latency results, Epic JSON notes
examples/        not deployed: the earlier SQL-file subscriptions, the earlier AWS cross-cloud setup
```

Contributors and coding agents: see [AGENTS.md](AGENTS.md).
