# HL7 v2 on Spark Real-Time Mode

Millisecond HL7 processing on Databricks: raw HL7 v2 in, parsed and broken into clinical entities in tens of
milliseconds per layer, with every layer also landed in Delta.

Built on [Real-Time Mode](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming),
Lakeflow Spark Declarative Pipelines, and Azure Event Hubs (Kafka endpoint).

## What this does

```
HOT PATH (Real-Time Mode, ms)
  HL7 source ─► raw_hl7 ─► bronze ─► bronze_hl7 ─► silver ─► silver_hl7 ─► gold ─► gold_encounters
                                                     │                          gold_observations ─► subscriptions ─► sub_*
                                                     └─► quarantine_hl7         gold_orders

NEAR-REAL-TIME PATH (seconds)
  every topic ─► the Delta streaming table of the same name (audit, BI, replay, latency telemetry)
```

- **Bronze** keeps the raw message and adds a small header (type, control id, facility).
- **Silver** parses HL7 with plain Spark (no JARs or UDFs) into the customer's existing 34-column Epic silver
  contract plus every lab result. Bad messages go to quarantine with a reason.
- **Gold** splits each message into what teams consume: encounters (ADT), observations (one per lab result),
  and orders (ORM).
- **Naming:** each topic is named after the table it lands in, `<catalog>.<schema>.<table>`
  (e.g. `real_time_mode_demo_catalog.hl7_realtime.silver_hl7`).

## Subscribe (self-service)

A team describes what it wants in one YAML file, and the platform runs it. Two rule shapes:

- **One event:** "a critical lab result" (`src/subscriptions/critical_lab.yml`)
- **Event A, then event B for the same patient within N minutes:** "ED registration, then a critical lab within
  60 minutes" (`src/subscriptions/ed_critical_lab.yml`)

```bash
make sub-new NAME=my_alert MODE=rtm JOIN=1   # or MODE=microbatch; copies the templates
#   edit src/subscriptions/my_alert.yml (sources, filters, window, output columns)
make infra-plan && make infra-apply          # creates the Event Hubs topic sub_my_alert
make deploy dev                              # runs it; results in topic + table <catalog>.<schema>.sub_my_alert
```

`rtm` delivers in milliseconds on a classic cluster. `microbatch` delivers in seconds on serverless.
Every field is explained in [`src/subscriptions/_template.yml`](src/subscriptions/_template.yml).

## Results

Measured on dev (Phase 3, Event Hubs Premium, 200 msg/s, 10 minutes, ~106k messages per hop):

| Hop | p50 | p99 |
|---|---|---|
| raw → bronze | 9 ms | **37 ms** |
| bronze → silver (full HL7 parse) | 15 ms | **47 ms** |
| silver → gold (encounters, observations, orders) | 15 ms | **32 ms** |

Every hop is under 200 ms at p99. Each pipeline pauses ~1–2 s every 5 minutes to checkpoint; that's the max.

Each number is the time from one Event Hubs topic to the next, measured from Event Hubs' own timestamps.
Details, experiments, and open items: [docs/latency.md](docs/latency.md).

## Setup

Prereqs: Databricks CLI ≥ 0.285, Terraform ≥ 1.5, Azure CLI (logged in), [uv](https://docs.astral.sh/uv/).

```bash
make install                                   # local dev environment
cp infra/eventhubs/terraform.tfvars.example infra/eventhubs/terraform.tfvars   # subscription, owner, catalog
make infra-plan && make infra-apply            # Event Hubs namespace + one hub per table
make secrets dev                               # connection string -> Databricks secret scope
make deploy dev                                # deploy; pipelines start automatically
make generate dev                              # start synthetic HL7 traffic
make stop dev                                  # stop everything (stops compute billing)
```

All names and sizes are bundle variables in `databricks.yml`. `make help` lists every command.

## Repo

```
databricks.yml     variables and targets
resources/         pipelines, jobs, schemas (by type)
src/hl7rt/         the logic: HL7 parsing, gold, subscriptions, Event Hubs helpers, generators
src/subscriptions/ self-service rule files (one YAML per subscription)
src/pipelines/     thin pipeline definitions
infra/eventhubs/   Terraform for Event Hubs
tests/             unit tests (local Spark)
docs/              latency results, the Epic JSON feed improvements
```

## Status

| Phase | Scope | |
|---|---|---|
| 0 | Feasibility: Real-Time Mode latency, sizing, Event Hubs tier | ✅ |
| 1 | Scaffold + the existing Epic JSON feed, improved ([details](docs/epic-json.md)) | ✅ |
| 2 | Raw HL7 → bronze → silver + Delta streaming tables | ✅ |
| 3 | Gold: encounters, observations, orders | ✅ |
| 4 | Self-service subscriptions (YAML rules, RTM or micro-batch) | 🔄 in review |
| 5 | Dashboard and walkthrough | ⏳ |

Contributors and coding agents: see [AGENTS.md](AGENTS.md).
