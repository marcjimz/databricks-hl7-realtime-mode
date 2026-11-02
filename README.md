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
```

- **Silver** parses HL7 with plain Spark into the 34-column Epic silver contract. Bad messages go to quarantine.
- **Gold** splits each message into encounters, observations (one per lab result), and orders.
- **Epic JSON** is a drop-in pipeline for the existing pre-parsed Epic feed ([docs/epic-json.md](docs/epic-json.md)).

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

## Configure

| What | Where |
|---|---|
| Subscription, tenant, region, metastore, name prefix, catalog, Event Hubs capacity | `infra/azure/terraform.tfvars` |
| Topic names, cluster size, feed rate, checkpoint interval, warehouse | `databricks.yml` |

## Repo

```
infra/azure/     Terraform: workspace, storage, access connector, Event Hubs
infra/uc.sh      Unity Catalog bootstrap (make uc)
databricks.yml   bundle variables and targets; resources/ has the pipelines, jobs, and dashboard
src/hl7rt/       the logic: HL7 parsing, gold, Event Hubs wiring, generators
src/pipelines/   thin pipeline definitions
tests/           unit tests (local Spark)
docs/            latency results, Epic JSON notes
examples/        not deployed: self-service subscriptions (next phase), the earlier AWS cross-cloud setup
```

Contributors and coding agents: see [AGENTS.md](AGENTS.md).
