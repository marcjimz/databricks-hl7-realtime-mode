<div align="center">
  <img src="./docs/images/dbx-banner.jpeg" alt="Databricks" style="width:100%; height:auto;">
</div>

<br>

# HL7 v2 on Spark Real-Time Mode

Millisecond HL7 processing on Azure Databricks: raw HL7 v2 in, parsed and broken into clinical entities in tens of
milliseconds per layer, with every layer also landed in Delta. Built on
[Real-Time Mode](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming),
Lakeflow Spark Declarative Pipelines, and Azure Event Hubs, connected through a Unity Catalog service credential:
no connection strings, no keys.

## What it does

```
HOT PATH (Real-Time Mode, ms)
  HL7 ─► raw_hl7 ─► bronze ─► bronze_hl7 ─► silver ─► silver_hl7 ─► gold ─► gold_encounters / gold_observations / gold_orders
                                               └─► quarantine_hl7

NEAR-REAL-TIME PATH (seconds)
  every topic ─► the Delta table of the same name (upserted; audit, BI, latency telemetry)
```

- **Silver** parses HL7 with plain Spark into the existing 34-column Epic silver contract. Bad messages go to quarantine.
- **Gold** splits each message into encounters, observations (one per lab result), and orders.
- **Epic JSON:** the customer's existing pre-parsed feed, as a drop-in bronze/silver/quarantine pipeline.
- Each topic is named after the table it lands in: `<catalog>.<schema>.<table>`.
- **One Azure region** (workspace, Event Hubs Premium, ADLS). Pipelines and generators authenticate to Event Hubs as the
  workspace's access connector (managed identity) through the Unity Catalog service credential `<catalog>_eventhubs`.
- Self-service subscriptions over gold are a later phase, parked in [examples/subscriptions](examples/subscriptions).

## Results

Measured at 200 msg/s over 12 minutes (~144k messages per hop), on the earlier AWS workspace with Event Hubs in
Azure (every hop crossing clouds); Azure same-region numbers: [docs/latency.md](docs/latency.md).

| Hop | p50 | p99 |
|---|---|---|
| raw → bronze | 12 ms | **33 ms** |
| bronze → silver (full HL7 parse) | 15 ms | **32 ms** |
| silver → gold | 15 ms | **32 ms** |

On AWS every Real-Time Mode hop was under 200 ms at p99. **On Azure (this target), every hop is under 200 ms at p99
too**: raw → bronze 93 ms, bronze → silver 94 ms, and silver → gold 156 ms (30-minute soak, 4 PU, 15-minute
checkpoints, 5 s hung-request guard). The tail past p99 is still seconds: checkpoint pauses, and Event Hubs
connections that occasionally hang. Details: [docs/latency.md](docs/latency.md).

## Set up (and after every Sunday sandbox wipe)

Prereqs: Databricks CLI ≥ 0.285, Terraform ≥ 1.5, Azure CLI (`az login` to the sandbox tenant),
[uv](https://docs.astral.sh/uv/).

```bash
make install                                                       # local Python env
cp infra/azure/terraform.tfvars.example infra/azure/terraform.tfvars   # once: subscription, tenant, metastore, prefix
make up                                                            # Terraform plan -> you confirm -> apply, then make uc
```

`make up` builds the workspace (assigned to the region's Unity Catalog metastore), ADLS, the access connector, and
Event Hubs Premium with one hub per table. Then `make uc` creates the catalog, the storage credential, and the Event
Hubs service credential. Unity Catalog lives in the shared metastore and survives the wipe; `make uc` repoints it at
the rebuilt resources and recreates the catalog when the workspace is new. Both are safe to rerun. The Makefile reads
the workspace URL and names from Terraform outputs and signs in with the Azure CLI, so there is nothing else to
configure.

## Operate (from your machine)

```bash
make demo dev            # deploy, start everything, start the HL7 feed, print the dashboard URL
make stop dev            # stop all pipelines and the feed (stops Databricks billing)

make start dev           # start pipelines only
make generate dev        # start the HL7 feed       |  make generate-stop dev
make clear-data dev      # empty the Delta tables / dashboard and refill from live data (~2 min)
make reset dev           # restart from the live edge with fresh checkpoints (demo only)
make help                # everything else
```

Classic Real-Time Mode clusters take ~9 minutes to start. Event Hubs Premium bills until the sandbox is wiped or you
`terraform -chdir=infra/azure destroy`.

## Configure

| What | Where |
|---|---|
| Azure: subscription, tenant, region, metastore, name prefix, catalog, Event Hubs capacity | `infra/azure/terraform.tfvars` |
| Topic names, cluster sizes, feed rate, checkpoint interval, warehouse | `databricks.yml` → `variables` / `targets.dev` |

To add an environment, copy the `dev` target in `databricks.yml` and add its name to `ENVS` in the `Makefile`.

## Demo

1. **10 minutes before:** `make demo dev`, then open the dashboard URL it prints. Check every pipeline shows Running
   (Pipelines UI). Once all hops show data, run `make clear-data dev` so start-up catch-up doesn't skew the numbers,
   and use the dashboard's own refresh button (top right), not the browser's.
2. **Show the flow:** raw HL7 v2 at 200 msg/s → bronze → silver (the Epic silver contract) → gold, an Event Hubs topic
   between every layer and the same layers as streaming tables in Catalog Explorer.
3. **Show the numbers:** p99 per hop (all under 200 ms), p99 by minute, latest critical labs reaching gold, quarantine.
4. **Show the security story:** Catalog Explorer → External data → Credentials → `<catalog>_eventhubs`. The pipelines
   have no secrets; the Event Hubs namespace has SAS keys disabled.
5. **Stop:** `make stop dev`.

The [walkthrough](docs/walkthrough.md) is a 20-minute demo script.

## Repo

```
databricks.yml     variables and targets
resources/         pipelines, jobs, schemas, dashboards (by type)
src/hl7rt/         the logic: HL7 parsing, gold, Event Hubs helpers, generators
src/pipelines/     thin pipeline definitions
src/dashboards/    live latency dashboard
infra/azure/       Terraform: workspace, ADLS, access connector, Event Hubs (one region)
scripts/uc.sh      Unity Catalog bootstrap (make uc)
examples/          parked: self-service subscriptions
infra/eventhubs/   legacy eastus Event Hubs used by the earlier AWS workspace
tests/             unit tests (local Spark)
docs/              latency results, walkthrough, Epic JSON notes
```

Contributors and coding agents: see [AGENTS.md](AGENTS.md).
