# AGENTS.md

Guidance for coding agents (and humans) working in this repo. **Keep this file current:** when a
decision, pattern, or hard-won lesson changes, update it in the same PR.

## What this repo is

A customer-facing reference for **millisecond HL7 v2 processing** on Databricks with Spark
**Real-Time Mode (RTM)**, Lakeflow Spark Declarative Pipelines (SDP), and Azure Event Hubs. It has to
**prove** low latency with measurements, so latency claims require data (see "Measuring latency").

```
HOT PATH (RTM, Event Hubs topics between every layer)
  hl7.raw ─► bronze ─► hl7.bronze ─► silver ─► hl7.silver ─► gold ─► hl7.gold.* ─► subscriptions ─► hl7.sub.*
LAKEHOUSE PATH (SDP micro-batch, off the hot path)
  every topic ─► Delta streaming tables (audit, replay, BI) + per-record latency telemetry
```

- Layers are **raw → bronze → silver → gold → self-service subscriptions**. Call the last layer
  "subscriptions", not "alerts".
- Delivery is **phased**: one branch + one PR per phase, each verified on dev before the next.
  Phase status lives in README.md.

## Non-negotiable principles

1. **Everything environment-specific is configurable.** That covers namespace, topic names, secret
   scope/key, catalog/schema, offsets, rates, and sizing. Bundle variables (`databricks.yml`) flow into
   pipeline `configuration:` as `hl7.<key>` and are read with `hl7rt.config.conf(spark, "<key>")`.
   `tests/test_config_wiring.py` enforces this. Never hardcode names in `src/`.
2. **No secrets anywhere in the repo.** The Event Hubs connection string lives only in a Databricks
   secret scope (`make secrets <env>` pipes it from Terraform output). `tests/test_no_secrets.py` enforces
   this. Don't print secrets in logs or command output.
3. **Native Spark for parsing; no JARs or UDFs on the hot path.** HL7 v2 is delimited text, so use
   `split` / `try_element_at` / `try_to_timestamp`. A full parse costs less than the 16 ms measurement
   resolution.
4. **Delta is never an RTM source or sink.** RTM hops go Kafka → Kafka. Delta tables are written by
   separate non-RTM SDP pipelines that read the topics.
5. **Measure before claiming.** Performance "improvements" must be benchmarked against the original.
   Phase 1's first "improved" silver was 2.4× *slower* until measured and fixed.
6. **Honesty in reporting.** Report p99 *and* max, say what wasn't verified, and correct earlier
   conclusions in `docs/latency.md` when evidence changes them.

## Repo layout

```
databricks.yml        variables + targets; include: resources/**
resources/<type>/     bundle resources grouped by type: jobs/, pipelines/, schemas/, (dashboards/)
src/hl7rt/            shared library: pure DataFrame transforms, Event Hubs helpers, generators (unit-tested)
src/pipelines/<name>/ thin SDP sources that call into hl7rt (pipeline root_path = src/)
infra/                Terraform only (Azure Event Hubs). tfvars/state are gitignored.
tests/                pytest with local Spark (.venv via uv)
docs/                 latency results + design notes
```

- Put logic in `src/hl7rt` as functions of DataFrames, so it's testable without a cluster. Keep
  `src/pipelines/*` to decorators + wiring.
- Throwaway experiments go in `spikes/` (gitignored, never committed).
- Generators are console scripts in the `hl7rt` wheel (built by the bundle `artifacts:` block), run as
  serverless `python_wheel_task` jobs with `environment_version: "3"` (Python 3.12).

## Real-Time Mode rules (learned in Phase 0, see docs/latency.md)

- **Enable it in SDP:** pipeline `continuous: true` + `spark.databricks.streaming.realTimeMode.enabled: "true"`
  in `configuration`, and `@dp.update_flow(target=<kafka sink>, spark_conf={"pipelines.trigger": "RealTime",
  "pipelines.trigger.interval": "5 minutes"})`. Sinks are `dp.create_sink(name, "kafka", {..., "topic": ...})`.
  The topic is static, so use one sink per output topic.
- **One RTM flow per pipeline, one pipeline per cluster.** Flows that share slots contend.
- **Compute:** classic, fixed `num_workers` (no autoscale), `photon: false`, `availability: ON_DEMAND`.
  DBR 18.3+ (SDP `CURRENT` channel).
- **Size with headroom:** total task slots ≥ tasks across all stages (Kafka `maxPartitions` + each
  shuffle's partitions) **plus ~20% or more spare**. With exactly as many slots as tasks, the epoch
  handoff fails with `CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT`, the flow crashes, and SDP restarts it.
  That looks like a 6–10 s "latency spike". Current sizing: stateless hop = 2 workers (8 slots) for 4
  partitions; windowed hop = 3 workers (12 slots) for 4 + 4 shuffle tasks.
- **Never use bundle `mode: development`** for RTM targets: it turns `continuous` off.
- **A first non-RTM run leaves a v1 checkpoint** → `CHECKPOINT_FORMAT_V1_NOT_SUPPORTED`. Fix with a
  full refresh.
- **`asyncProgressTrackingEnabled` is rejected on SDP classic** (standard access mode). It isn't needed:
  async progress tracking and async state checkpointing are on by default in DBR 18.3.
- **Supported in RTM:** stateless ops, `explode`, tumbling/sliding windows with watermark, aggregations,
  `dropDuplicates`, `transformWithState` (row API), broadcast stream-static joins.
  **Not supported:** Delta source/sink, `foreachBatch`, session windows, `mapPartitions`,
  `transformWithStateInPandas`.
- **Epoch boundaries:** every `pipelines.trigger.interval`, each pipeline pauses briefly (measured
  ~0.6–1.6 s, affecting ~0.1% of messages). p99 is unaffected, but max is. This is an open item.
- **Event Hubs Premium is required.** Standard's multi-tenant tail missed 200 ms p99 on two hops;
  Premium gives ~31 ms p99 per hop.

## Measuring latency

- Event Hubs stamps every record with **LogAppendTime** (`timestampType = 1`). Each layer carries the
  upstream topic's broker timestamp forward (`raw_ts`, `bronze_ts`, `silver_ts`, `gold_ts`), so
  `hop_ms = this_topic.timestamp - upstream_ts` uses broker clocks only, with no client clock skew.
- **Resolution is ±16 ms** (Event Hubs timestamps tick in ~16 ms steps; broker clocks differ by a few
  ms). Readings of 0 ms are real "same tick" results, not bugs. For sub-16 ms precision use RTM's
  `processingLatencyMs`.
- **Telemetry is live:** the mirror's streaming tables carry `hop_ms` / `e2e_ms` per record. Query
  percentiles over them; don't build one-shot probe jobs.
- **Always check the pipeline event log for flow `WARN`/`ERROR` before reading latency.** A crash and
  restart looks exactly like a latency spike.
- **Exclude backlog replay:** after downtime, hops drain backlog at "minutes" of apparent latency.
  Filter on fresh records (`upstream_ts` within the window), or `make reset` for demos.
- **The target bar:** every hop under 200 ms at **p99**, over a soak that spans at least 2 epoch
  boundaries. Report max separately.

## Operating the demo

- `make deploy <env>` → continuous pipelines **start automatically** (`bundle run` then reports "active
  update already exists"). `make generate <env>` / `make generate-stop <env>` control load.
  `make stop <env>` stops compute billing.
- **Don't restart RTM pipelines between tests.** Classic cold start is ~9 min. Change load, not
  pipelines.
- Spark's Kafka source **resumes from its checkpoint, not the consumer group**. `kafka_starting_offsets`
  only applies to a new checkpoint. `make reset <env>` (full refresh) starts at the live edge, for demos
  only. Production must resume from the checkpoint so no HL7 message is skipped.
- Several CLI profiles can match one host. Set `DATABRICKS_CONFIG_PROFILE`.

## Spark patterns and pitfalls

- **ANSI is on** (Spark 4 / DBR 18): use `try_to_timestamp`, `try_element_at`, `try_cast`. A single
  malformed value with `to_timestamp` fails the whole stream.
- **Parse JSON once.** The optimizer pushes filters below projections and re-inlines `from_json`
  (seen: 5 parses per row). Wrap the parse in `inline(array(struct(...)))` as a barrier (see
  `epic_json._with_fields`), and verify with the optimized plan.
- **Parse timestamps once.** Pick the format from the value's shape (`rlike`) rather than trying
  formats in sequence (~5× slower).
- `map_from_entries` + `spark.sql.mapKeyDedupPolicy=LAST_WIN` over a reversed array = first occurrence
  wins. That conf is set via the table's `spark_conf` (`epic_json.SPARK_CONF`).
- Tests run with `TZ=UTC` and `spark.sql.session.timeZone=UTC` (collected timestamps convert to the
  process zone).

## Workflow

- **Branches:** one per phase/unit of work, PR'd with summary / rationale / testing / "not verified".
  Stack PRs when the base isn't merged yet. Never commit to `main` directly.
- **TDD for library code and the subscription runner:** write specs first (RED), then implement (GREEN).
- **Before pushing:** `make test`, `make lint`, `databricks bundle validate -t dev`. For pipeline changes,
  deploy to dev and verify against live data (row counts, quarantine reasons, event-log errors, latency),
  then `make stop dev`.
- **Infra:** always `make infra-plan` → review → `make infra-apply` (applies only the saved plan).
  Never auto-approve. Azure sandbox policy requires an `owner` tag (`var.owner`).
- **Confirm before** creating paid or outward-facing resources (namespaces, tiers, new clusters), destroying
  anything, or forcing destructive bundle deploys (e.g. schema recreation). Don't bypass those prompts.
- Commit messages end with `Co-authored-by: Isaac <no-reply@databricks.com>`.
