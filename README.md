# HL7 v2 on Spark Real-Time Mode

A working reference for **millisecond HL7 processing on Databricks** with
[Real-Time Mode](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming)
for Structured Streaming, on Lakeflow Spark Declarative Pipelines and Azure Event Hubs.

```
                     HOT PATH — Real-Time Mode, Event Hubs topics between layers
 HL7 source ─► hl7.raw ─► bronze ─► hl7.bronze ─► silver ─► hl7.silver ─► gold ─► hl7.gold.* ─► subscriptions ─► hl7.sub.*
                                                                                                   (self-service teams)
                     LAKEHOUSE PATH — SDP micro-batch, Unity Catalog
 every topic ─► Delta streaming tables (audit, replay, BI) + per-record latency telemetry
```

**Measured** (Phase 0, Event Hubs Premium, 15-minute soak): each hop is **~31–47 ms at p99**. See
[docs/latency.md](docs/latency.md) for every experiment, root cause, and open item.

## Status

| Phase | Scope | State |
|---|---|---|
| 0 | Feasibility: RTM latency per hop, sizing, Event Hubs tier | ✅ [docs/latency.md](docs/latency.md) |
| 1 | Scaffold + the existing Epic JSON feed (bronze/silver), improved | ✅ this repo |
| 2 | Raw HL7 → bronze → silver on RTM (native Spark HL7 parsing) + Delta mirror + live telemetry | ⏳ |
| 3 | Gold breakdown: encounters, observations (one row per OBX), orders | ⏳ |
| 4 | Self-service subscription template (filters, windows) → Event Hubs + Delta | ⏳ |
| 5 | Demo polish: dashboard, walkthrough | ⏳ |

## Quickstart

Prereqs: Databricks CLI ≥ 0.285, Terraform ≥ 1.5, Azure CLI (logged in), [uv](https://docs.astral.sh/uv/).

```bash
make install                                         # .venv with dev deps
cp infra/eventhubs/terraform.tfvars.example infra/eventhubs/terraform.tfvars   # fill in subscription/owner
make infra-plan && make infra-apply                  # Event Hubs namespace + hubs (reviewed plan)
make secrets dev                                     # connection string -> secret scope (never printed)
make deploy dev                                      # bundle validate + deploy
make generate dev                                    # continuous synthetic feed (until generate-stop)
make stop dev                                        # stop pipelines + generators when done
```

`make help` lists every target. If several CLI profiles match the workspace host, set
`DATABRICKS_CONFIG_PROFILE`.

## Layout

```
databricks.yml          bundle: variables + targets; includes resources/**
resources/              bundle resources, grouped by type
  jobs/                 generators (and later: subscription jobs)
  pipelines/            SDP pipelines
  schemas/              Unity Catalog schema
src/
  hl7rt/                shared library (pure transforms, Event Hubs helpers, generators) — unit-tested
  pipelines/            thin SDP pipeline sources that call into hl7rt
infra/<stack>/          Terraform, one stack per folder (today: eventhubs/), each with its own state
tests/                  pytest with local Spark
docs/                   latency results and design notes
AGENTS.md               principles, patterns, and lessons for contributors and coding agents
```

## Configuration

Everything environment-specific is a bundle variable (`databricks.yml`). Override per target or with
`--var name=value`.

| Variable | Default | Purpose |
|---|---|---|
| `catalog` | — (per target) | Existing UC catalog |
| `schema` | `hl7_realtime` | Schema created by the bundle |
| `eh_namespace` | — (per target) | Event Hubs namespace (`terraform output namespace`) |
| `eh_secret_scope` / `eh_secret_key` | `hl7-rtm` / `eh-connection-string` | Where the connection string lives |
| `epic_json_topic` | `epic.json` | Existing Epic JSON feed topic |
| `kafka_starting_offsets` | `latest` | Start point for a **new** checkpoint only |
| `kafka_max_offsets_per_trigger` | `50000` | Micro-batch cap (Epic JSON pipeline) |
| `generator_rate` | `50` | Synthetic events/second |

The connection string is only ever in the secret scope: `make secrets` pipes it from Terraform output, and
`tests/test_no_secrets.py` (also a pre-commit hook) fails if one is ever committed.

## The existing Epic JSON feed (Phase 1)

`src/pipelines/epic_json/` is a drop-in replacement for the customer's current bronze/silver pipeline.
Table names and the silver column contract (names, order, types) are unchanged. What changed:

| | Before | After |
|---|---|---|
| Field extraction | 34× `get(filter(columns, x -> x.name = '…'), 0)` per row | `columns` → map once, one lookup per field |
| Throughput (local, 300k rows) | ~46k rows/s | ~71–83k rows/s (**~1.7×**) |
| Malformed timestamp | `to_timestamp` raises under ANSI → **the stream fails** | `NULL` (HL7 DTM + ISO-8601 parsed by shape) |
| Bad payloads | All-null rows in silver | Quarantine table with `reason` + Kafka coordinates |
| Config / secrets | Hardcoded namespace/hub, key inline | Bundle variables + secret scope |
| Bronze layout | `pipelines.autoOptimize.zOrderCols` | Liquid clustering on `kafka_timestamp` |

> Note for an existing deployment: switching bronze from Z-order to `cluster_by` applies to the new table; an
> existing table keeps its layout until it's rebuilt.

Starting offsets: Spark's Kafka source resumes from its **checkpoint**, not the consumer group. `make reset <env>`
(full refresh) starts at the live edge, which is for demos only. In production, always resume from the checkpoint.
