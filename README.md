<div align="center">
  <img src="./docs/images/dbx-banner.jpeg" alt="Databricks" style="width:100%; height:auto;">
</div>

<br>

# HL7 v2 on Spark Real-Time Mode

Millisecond HL7 processing on Databricks: raw HL7 v2 in, parsed and broken into clinical entities in tens of
milliseconds per layer, with every layer also landed in Delta. Built on
[Real-Time Mode](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming),
Lakeflow Spark Declarative Pipelines, and Azure Event Hubs.

## What it does

```
HOT PATH (Real-Time Mode, ms)
  HL7 ─► raw_hl7 ─► bronze ─► bronze_hl7 ─► silver ─► silver_hl7 ─► gold ─► gold_* ─► subscriptions ─► sub_*
                                               └─► quarantine_hl7

NEAR-REAL-TIME PATH (seconds)
  every topic ─► the Delta table of the same name (upserted; audit, BI, latency telemetry)
```

- **Silver** parses HL7 with plain Spark into the existing 34-column Epic silver contract. Bad messages go to quarantine.
- **Gold** splits each message into encounters, observations (one per lab result), and orders.
- **Subscriptions** are YAML rules teams write themselves, in milliseconds (RTM) or seconds (micro-batch).
- Each topic is named after the table it lands in: `<catalog>.<schema>.<table>`.

## Results

Measured on dev at 200 msg/s over 12 minutes (~144k messages per hop):

| Hop | p50 | p99 |
|---|---|---|
| raw → bronze | 12 ms | **33 ms** |
| bronze → silver (full HL7 parse) | 15 ms | **32 ms** |
| silver → gold | 15 ms | **32 ms** |
| gold → subscription: critical lab | 15 ms | **31 ms** |
| gold → subscription: ED registration, then a critical lab within 60 min | 32 ms | **79 ms** |
| same subscription in micro-batch | 2.1 s | 5.4 s |

Every Real-Time Mode hop is under 200 ms at p99. Each pipeline pauses ~1–3 s every 5 minutes to checkpoint, which
sets the max. Details: [docs/latency.md](docs/latency.md).

## Setup (once)

Prereqs: Databricks CLI ≥ 0.285, Terraform ≥ 1.5, Azure CLI (logged in), [uv](https://docs.astral.sh/uv/).

```bash
make install                                             # local Python env
cp infra/eventhubs/terraform.tfvars.example infra/eventhubs/terraform.tfvars   # then edit it
make infra-plan && make infra-apply                      # Event Hubs namespace + topics
cp local.mk.example local.mk                             # your Databricks CLI profile (if several match)
make secrets dev                                         # connection string -> Databricks secret scope
```

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

Classic Real-Time Mode clusters take ~9 minutes to start. Event Hubs keeps billing until you `terraform destroy`.

## Configure

| What | Where |
|---|---|
| Azure: subscription, region, namespace, tier, capacity, catalog | `infra/eventhubs/terraform.tfvars` |
| Databricks: workspace, catalog, schema, Event Hubs namespace, warehouse | `databricks.yml` → `targets.dev.variables` |
| Topic names, cluster sizes, feed rate, checkpoint interval | `databricks.yml` → `variables` (defaults) |
| Subscriptions | `src/subscriptions/<name>.yml` |

To add an environment, copy the `dev` target in `databricks.yml` and add its name to `ENVS` in the `Makefile`.

## Demo: show a new subscription live

1. **10 minutes before:** `make demo dev`, then open the dashboard URL it prints. Wait until every hop shows data.
2. **Show what's running:** the p99 per hop (all under 200 ms), the two RTM examples (`critical_lab`,
   `ed_critical_lab`), and the same ED rule in micro-batch (`ed_critical_lab_nrt`).
3. **Create a subscription:**
   ```bash
   make sub-new NAME=hyperkalemia MODE=microbatch
   ```
   Replace the `when:` and `output:` lines in `src/subscriptions/hyperkalemia.yml` with:
   ```yaml
   when:
     source: gold_observations
     filter: "observation_cd = '2823-3' AND value_num > 6.0"
   output: [patient_id, visit_id, patient_facility, value_num, abnormal_flag]
   ```
4. **Run it:**
   ```bash
   make infra-plan && make infra-apply   # creates topic sub_hyperkalemia (needs `az login`)
   make deploy dev                       # starts it on serverless (~1 minute)
   ```
5. **Show the result:** table `sub_hyperkalemia` fills, and a `gold → hyperkalemia (micro-batch)` bar appears on the
   dashboard with no dashboard edits.
6. **Clean up:** `make stop dev`. To remove the example, delete `src/subscriptions/hyperkalemia.yml` and
   `resources/pipelines/sub_hyperkalemia.yml`, then `make deploy dev` (confirm the pipeline removal) and
   `make infra-plan && make infra-apply` (removes the topic).

Use `MODE=rtm` for millisecond delivery. Its classic cluster takes ~9 minutes to start, so in a live demo show the
pre-built RTM examples and create the new one in micro-batch.

## Subscribe (self-service)

```bash
make sub-new NAME=my_alert MODE=rtm      # or MODE=microbatch; JOIN=1 for "event A then event B"
# edit src/subscriptions/my_alert.yml: source, filter, optional window, output columns
make infra-plan && make infra-apply      # creates topic sub_my_alert
make deploy dev                          # runs it: topic + table <catalog>.<schema>.sub_my_alert, on the dashboard
```

Every field is explained in [`src/subscriptions/_template.yml`](src/subscriptions/_template.yml). The
[walkthrough](docs/walkthrough.md) is a 20-minute demo script.

## Repo

```
databricks.yml     variables and targets
resources/         pipelines, jobs, schemas, dashboards (by type)
src/hl7rt/         the logic: HL7 parsing, gold, subscriptions, Event Hubs helpers, generators
src/pipelines/     thin pipeline definitions
src/subscriptions/ self-service rule files
src/dashboards/    live latency dashboard
infra/eventhubs/   Terraform for Event Hubs
tests/             unit tests (local Spark)
docs/              latency results, walkthrough, Epic JSON notes
```

Contributors and coding agents: see [AGENTS.md](AGENTS.md).
