# Spike: full SQL subscriptions, one dedicated pipeline per app ("channels")

Status: **design spike, nothing built.** Written 2026-11-05 to plan how apps get more than one-record filters. Every
platform claim below is tagged **verified** (measured or run in this repo) or **probe** (to test before building).

## The problem

Today a subscription rule is a filter over one gold record. The shared hub runs up to 1,000 of them at p99 ~7 s
(docs/subscriptions.md). That's deliberate: the hub evaluates every app's SQL with its own permissions in one pass, so a
rule can't join, aggregate, look at other records, or use windows.

Apps want more than that:

- **Over time:** "3 rising potassium results for one patient within 1 hour", or "no vitals for 30 minutes".
- **Across sources:** "an order with no result within 2 hours" (orders ⋈ observations), or "an admission whose first lab
  is critical" (encounters ⋈ observations).
- **Enriched:** a match carrying unit, attending, or care team from reference tables.
- **Result changes:** "tell me when this census query's rows change" (ADDED / UPDATED / REMOVED).

Fabric Activator covers part of this with built-in conditions over time. It has "becomes", "increases", "stays true for",
and "no data for", plus averages over 10 s–24 h windows. Its documentation describes no SQL rule language and no joins
across streams (Microsoft Learn, 2026-11-05). Full streaming SQL would go further than Activator, at the price of a different latency and change
model than rules.

The customer's feasibility spec (`Databricks_Dynamic_Subscription_Feasibility_Spec_v0_3.md` §3) already names these
types:

- **EVENT_MATCH** is today's rules.
- **TEMPORAL_CORRELATION** covers windows and joins over time.
- **QUERY_RESULT_CHANGES** covers keyed result diffs.

This spike is the backend for the second and third.

## The idea: a "channel" next to the shared hub

The rule tier stays. Next to it, an app onboards one or more **channels**. Each channel is a dedicated serverless SDP
pipeline that runs one streaming SQL query and owns its output table and topic. It's registered through the same
Subscription API, as a tenant of it.

```
                          Subscription API (one, shared)
                   rules ──────┘          └────── channels
                     │                                │
       shared hub (1 pipeline, ≤1,000 filters)   one pipeline per channel, run as the app's SP
       per-record, seconds, change → ACTIVE ~5 s  windows · stream-stream joins · enrichment · result diffs
                     │                                │       change → restart (≈1–2 min, probe)
                     └──────────► the app's own topic ◄┘ + the channel's own table <catalog>.sub_<app>.<channel>
```

| | Rules (today) | Channels (proposed) |
|---|---|---|
| SQL | A filter on one record | A streaming SELECT: joins, windows, aggregates, dedup, lookups |
| Runs in | The shared hub, as the hub | A dedicated pipeline, **as the app's service principal** |
| Who enforces grants | Our shape check, plus the hub re-checking granted columns | Unity Catalog, on every read: the query can't read what the SP can't |
| Change → live | ~5–8 s, no restart (verified) | A pipeline update. Stateful changes start from new state (probe: time) |
| Cost | Shared: one pipeline for every app | One continuous serverless pipeline per channel, so a quota per app |
| Scale | 1,000 filters per hub | Few queries, each as heavy as it needs |

**Why "run as the app's SP" matters most.** The shape check exists because the hub runs app SQL with the hub's
permissions: a subquery once read data the app wasn't granted. A channel runs with the app's own identity, so Unity
Catalog decides every read, column, and table. Joins, subqueries, and reference tables become safe by construction. Our
own check shrinks to the output contract (a key, the selected fields) and a denylist for functions that leave the
platform. Deploying `run_as` an SP needs the deployer to have **Service Principal: User** on it, which we already hit with
the E2E job (verified). The API's SP would need that role on every onboarded app.

## Name

Our own term, not Activator. Things to avoid: **Actuator** (to Java developers, Spring Boot Actuator means health
endpoints); **Monitor** (Lakehouse Monitoring); **Agent** (AI agents); **Flow** (an SDP flow is a different thing);
**Sentinel** (Microsoft Sentinel).

| Candidate | For | Against |
|---|---|---|
| **Channel** (recommended working name) | HL7 integration engines (Mirth Connect, Rhapsody, Cloverleaf) all call a routed, transformed feed a *channel*, so the customer's integration developers already know the word | Close to "topic" in casual speech |
| Watch | Short, reads well in code: `MyWatches` | Sounds passive, and less familiar in healthcare |
| Lens | "A view over the stream" | Too abstract |
| Stream app | Literal | Generic, and it sounds like the app itself |

## How it would work

1. **Onboard** (`make registry`, per app): a channel quota and a schema `<catalog>.sub_<app>` that already holds the
   app's views. The app's SP gets permission to own pipelines, or the API creates them `run_as` the SP.
2. **Declare** (Java, as today):
   - `MyChannels.java` maps a name to a SQL statement plus a key.
   - `PUT /api/v1/channels` declares the full set.
3. **Validate**, with errors passed through as they are for rules:
   - First the fast check: `DESCRIBE QUERY` on the warehouse (verified for rules).
   - Then the streaming check: a **validate-only pipeline update** (`pipelines start-update --validate-only`; the CLI
     has it, verified). It reports streaming-only errors, such as a stream-stream join without a watermark, in Spark's
     own words. (Probe: time to result.)
4. **Run.** The API creates or updates one pipeline from a fixed, generic source file. The file reads its SQL from the
   Operational Store at startup (configuration `hl7.channel_id`). The pipeline:
   - reads the app's views;
   - writes `<catalog>.sub_<app>.<channel>` as a streaming table, which is the audit and replay copy;
   - sinks matches to the app's topic, in the same notification envelope plus `channel`.
5. **Change.** A new revision is a pipeline update.
   - **Stateless changes** keep the checkpoint.
   - **Stateful changes** start fresh, because Spark state isn't compatible across query changes. That's the spec's
     `ABORT_OPEN_WINDOWS` for temporal types and `RESET_AND_SNAPSHOT` for result diffs, so the new revision announces
     its reset.
   - The Java app waits for ACTIVE as it does now, but over minutes instead of seconds.
6. **Status and monitoring:** pipeline state and the event log go into the Operational Store's status table.
   `HUB_STALE` becomes `CHANNEL_STALE`.

## What runs where (latency)

| Pattern | Engine | Expected latency | Basis |
|---|---|---|---|
| Windows, dedup, stream-static enrichment | Real-Time Mode is possible: tumbling and sliding windows, `dropDuplicates`, and broadcast joins are GA in RTM | ms (the hot path's tier) | **probe:** RTM flows are Python (`update_flow` plus a Kafka sink) and read topics, not views, so SQL text and UC-governed sources need a workaround |
| Stream-stream joins (order ⋈ result within 2 h) | Micro-batch SDP, with watermarks on both sides | seconds | RTM has no stream-stream joins (GA docs, verified in AGENTS.md); micro-batch supports them |
| Result changes (census) | A materialized view plus a diff publisher (spec §6) | the MV refresh interval | **probe:** incremental MV refresh, and how to diff two refreshes |

The first version should be **micro-batch only**, reading the app's views. It's simple and governed by Unity Catalog. RTM
channels are a later optimization, for patterns that need milliseconds and avoid stream-stream joins.

## Is it dynamic and real time?

- **Dynamic, yes, but not instantly.** SQL is declared from Java at runtime with no redeploy of the platform. But a
  pipeline can't swap its query while running: a change restarts it. Serverless flows reach RUNNING within about a
  minute (verified for our pipelines), so expect about 1–2 minutes from change to ACTIVE. That's fine for complex
  rules, which change rarely; filters stay on the hub for ~5 s changes.
- **Real time, in seconds.** Micro-batch stream-stream joins and windows deliver within the trigger interval plus the
  watermark delay. A rule like "within 2 hours" can only fire once the evidence exists, or at window close for absence
  patterns. Reading views of the streaming tables adds the mirror's own lag, which needs measuring.

## Probes, before building anything

| # | Question | Pass |
|---|---|---|
| C1 | A stream-stream join (orders ⋈ observations, watermarks) in SDP micro-batch, reading `sub_<app>` views of the streaming tables | runs; p99 end-to-end measured |
| C2 | A pipeline `run_as` the app's SP reads only its views and is refused anything else | the denial is Unity Catalog's own error |
| C3 | Streaming reads from per-app views (projection views over streaming tables) | supported; any limits listed |
| C4 | Validate-only update: time to result, and the errors it reports (no watermark, unsupported op) | ≤ ~1 min; Spark's error is passed through |
| C5 | Change → ACTIVE, stateless and stateful | measured; the reset is announced |
| C6 | Cost of one idle and one busy channel; workspace pipeline limits | numbers for the quota |
| C7 | RTM channel: SQL over a topic in `update_flow`, with window plus dedup | works or doesn't, latency |
| C8 | QUERY_RESULT_CHANGES: MV refresh plus diff, keyed ADDED / UPDATED / REMOVED | correct on a census query |

If C1–C5 pass, build micro-batch channels for TEMPORAL_CORRELATION first: it's the most-asked pattern, and the one
Activator does natively. QUERY_RESULT_CHANGES follows on C8.

## Open questions

- **Quota and cost owner:** how many channels per app, and who pays for each pipeline's compute.
- **Sources:** reading views of streaming tables is governed but adds the mirror's lag. Reading gold topics directly is
  faster but isn't governed per column by Unity Catalog. Measure C1 before choosing.
- **Shared reference data:** which lookup tables (units, providers) the platform publishes, so channels can join them.
- **Name:** confirm "channel" with the customer's integration team.
