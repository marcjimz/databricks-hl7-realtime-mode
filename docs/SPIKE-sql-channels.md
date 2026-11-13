# Spike: full SQL subscriptions, built on the app's behalf

Status: **design spike, nothing built.** Written 2026-11-05 to plan how apps get more than one-record filters. Every
platform claim below is tagged **verified** (measured or run in this repo) or **probe** (to test before building).
Fabric facts are from Microsoft Learn, fetched 2026-11-05.

## The goal

A Java developer onboards once as a subscriber, then declares SQL. **The Java app only ever sends SQL.** The platform
looks at each statement and builds whatever it needs, on the app's behalf:

| What the SQL needs | Example | Runs in | Change → ACTIVE |
|---|---|---|---|
| **1. A filter on one record** (today) | `WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5` | the shared hub | **~5–8 s**, no restart (verified) |
| **2. Batch SQL on new records**: joins to tables or recent history, window functions over that history, subqueries | 3 rising potassium results for one patient within 1 hour | the app's channel, per micro-batch | **seconds**, no restart (probe) |
| **3. A new data structure**: a join between two streams, a streaming windowed aggregate, a new source | an order with no result within 2 hours | the app's channel, as part of its streaming plan | **~1–2 min**, a restart managed by the API (probe) |

The app's code doesn't change between the three. It sends `PUT /api/v1/subscriptions` with a name → SQL map, as today,
and waits on `GET ?wait`. Only the time to ACTIVE differs.

## Why the three speeds

A Structured Streaming query's plan is fixed when it starts: its sources and its stateful operators (stream-stream
joins, windowed aggregates, watermarks, `dropDuplicates`) live in its checkpoint. Changing them means a restart, and a
new checkpoint when the old state no longer fits. Everything inside a `foreachBatch` call is ordinary batch SQL over that
batch and can change between batches. The hub already works this way: its stream is fixed, and the rules it applies each
batch come from the Operational Store at that moment (verified).

So the dynamic part is everything per batch:
- filters;
- functions;
- joins to tables and to recent history in Delta;
- window functions over that history.

Only a new streaming structure needs a restart. Serverless flows reach RUNNING within about a minute (verified for our
pipelines). After a restart the pipeline resumes from its checkpoint and catches up, so a restart pauses delivery but
nothing is skipped.

## The channel: what the API builds for each subscriber

A **channel** (working name, see "Name") is the app's own pipeline plus its own table. The Subscription API creates it
on the app's behalf the first time the app declares tier-2 or tier-3 SQL. That first time takes about a minute; later
changes don't, unless they need a new structure.

- **One serverless SDP pipeline per subscriber, run as the app's service principal.** Unity Catalog decides every read,
  column, and table, so joins, subqueries, and reference tables are safe by construction. Rules need the shape check
  because the hub runs everyone's SQL with the hub's own permissions. A channel runs with the app's.
- **Its output:** a table `<catalog>.sub_<app>.<subscription>` (the audit and replay copy), plus notifications to the
  app's existing topic in the same envelope.
- **Its streaming plan** holds only the app's tier-3 structures, such as a watermarked join of orders and observations.
  Its `foreachBatch` runs all of the app's tier-2 SQL, which the Operational Store supplies at each batch.
- **One generic, fixed source file** reads the app's declared structures from the Operational Store at startup.
  Pipelines differ only by configuration (`hl7.subscriber`). No code is generated per app.
- **History for tier 2:** the channel appends each batch to a short-retention history table and gives the SQL two views
  per source: `new_<source>` (this batch) and `recent_<source>` (last N hours). Because the channel writes its own
  history, it doesn't depend on the lakehouse mirror's lag.
- **A stable notification ID** (result key + revision) means a match found again over overlapping history is sent once.

## How the API classifies SQL

The API parses the statement with sqlglot (as today), has Databricks compile it, then picks the tier from its shape:

- **Tier 1:**
  - one silver source;
  - a WHERE built from per-record expressions only.
- **Tier 2:**
  - one streaming source (`new_<source>`);
  - joins only to non-streaming relations: `recent_<source>`, reference tables, or the app's own views;
  - window functions, subqueries, and aggregates over those.
- **Tier 3:**
  - two streaming sources joined with a time bound, for example
    `FROM silver_orders o LEFT JOIN silver_observations r ON r.visit_id = o.visit_id AND r.source_ts BETWEEN o.source_ts AND o.source_ts + INTERVAL 2 HOURS`;
  - or `GROUP BY window(...)` / `session_window(...)` over a stream.
  - The API derives the watermarks from the time bound.
  - A stream-stream join with no time bound is refused: its state would grow without limit. Spark itself refuses
    some of these shapes, and that error is passed through.
- **Refused in every tier:**
  - functions that leave the platform;
  - randomness;
  - anything Databricks rejects, with Databricks' error passed through.

Validation has two steps. First, `DESCRIBE QUERY` on the warehouse for the fast check (verified for rules). For tier 3,
also a **validate-only pipeline update** (`pipelines start-update --validate-only`; the CLI has it, verified), which
reports streaming-only errors such as a missing watermark in Spark's words. (Probe: time to result.)

## The handshake, including restarts

The API is the control plane for every change.

1. `PUT` returns `ACCEPTED` per subscription, with its `tier` and `requiresRestart`.
2. **Tiers 1 and 2:** the hub or the channel picks the change up at its next batch. `GET ?wait` reports ACTIVE within seconds.
3. **Tier 3 (or the app's first channel):** the API starts or updates the app's pipeline through the Pipelines API,
   using its own service principal with CAN_MANAGE on that pipeline.
   - `GET ?wait` reports `BUILDING` / `RESTARTING`, then ACTIVE once the pipeline runs the new revision.
   - The Java client keeps polling in short calls. Its wait grows from 2 to about 5 minutes for these.
4. **Several changes, one restart:** changes that arrive within a short window (about 30 s) are applied together.
5. **No surprise outages:** a restart only ever touches the app's own channel. The shared hub never restarts for an
   app's change.
6. **Rollback:** if the new plan won't start, the API restores the previous definition and reports `FAILED` with Spark's
   error. This is the spec's "failed replacement while the old rule serves".
7. **State after a restart:** a structural change means new state. Per the feasibility spec, the new revision
   announces it: `ABORT_OPEN_WINDOWS` for temporal subscriptions, and `RESET_AND_SNAPSHOT` for query-result changes.

## Onboarding as a subscriber

The SQL side is already dynamic. Onboarding isn't yet: `make registry` and Terraform create each app's pieces. For "the
API creates the subscriber on their behalf", there would be an admin endpoint (`POST /api/v1/subscribers`, platform
admins only) that creates:

- **Databricks objects** (the API's service principal can create these; probe the exact grants):
  - the app's registry role and binding;
  - its views and grants;
  - CAN_USE on the API;
  - the app's channel pipeline, created when first needed.
- **Azure objects**, which today are Terraform: the app's Event Hub, its access connector, and the Unity Catalog service
  credential. The options:
  - (a) the API calls Azure Resource Manager with a managed identity;
  - (b) Terraform pre-creates a pool of hubs and credentials that onboarding assigns;
  - (c) onboarding stays a platform step and only SQL is self-service.

  (b) is the simplest that's still dynamic.

The app's service principal itself stays the customer's to create: it's their identity and their secret.

## Compared with Fabric

| | Fabric | This design |
|---|---|---|
| Simple condition change | Activator: edit, then **Update**. Applies to new data only. Rules start and stop individually. | Tier 1: ~5–8 s, no restart (verified) |
| How fast it fires | Up to 1 min of backend processing, a 2-minute late-arrival wait by default, "in some cases up to 10 minutes" | p99 ~5 s (1 rule), ~7 s (1,000 rules) (verified) |
| Conditions over time | Activator built-ins: becomes, increases, stays true for, no data for, windowed averages | Tier 2 (dynamic) or tier 3, in SQL |
| Joins across streams | Activator documents none (it tracks state per object). The Eventstream Join operator combines two streams. | Tier 3, in SQL |
| Changing a join | Eventstream: takes effect on **Publish**. Ingestion and routing restart at "Now", and some events may not reach the destination. Downtime isn't documented. | API-managed restart of ~1–2 min, resuming from the checkpoint, nothing skipped (probe) |
| Joins in Eventhouse (KQL) | Materialized views: dimension-table joins only, new records only. An update policy with a join requires turning off streaming ingestion. | n/a |
| Language | No-code designer | Databricks SQL, tested in the SQL editor |
| Who enforces access | Fabric item permissions | Unity Catalog, per app (rules), or the app's own identity (channels) |

Neither platform changes a stream-stream join instantly. The difference is that our restart resumes from where it
stopped, and every tier is declared in SQL from the app.

## Name

We want our own term, not Activator. Avoid **Actuator** (to Java developers, Spring Boot Actuator means health endpoints),
**Monitor** (Lakehouse Monitoring), **Agent** (AI agents), **Flow** (an SDP flow is a different thing), and **Sentinel**
(Microsoft Sentinel).

| Candidate | For | Against |
|---|---|---|
| **Channel** (recommended working name) | HL7 integration engines (Mirth Connect, Rhapsody, Cloverleaf) all call a routed, transformed feed a *channel*, so the customer's integration developers already know the word | Close to "topic" in casual speech |
| Watch | Short, reads well in code | Sounds passive, and less familiar in healthcare |
| Lens | "A view over the stream" | Too abstract |

## Probes, before building anything

| # | Question | Pass |
|---|---|---|
| C1 | Tier 3: a stream-stream join (orders ⋈ observations, time-bounded, watermarks) in SDP micro-batch | runs; p99 end-to-end measured |
| C2 | A pipeline `run_as` the app's SP reads only its grants and is refused anything else | the denial is Unity Catalog's own error |
| C3 | Tier 2: per-batch SQL with window functions over `new_` ∪ `recent_` history in `foreachBatch`, changed between batches | the change applies at the next batch; batch time measured for 1–24 h of history |
| C4 | Validate-only update: time to result, and the errors it reports (no watermark, unsupported op) | ≤ ~1 min; Spark's error passed through |
| C5 | Restart handshake: change → ACTIVE (first channel, structural change, rollback on a bad plan) | measured; no records skipped; the reset is announced |
| C6 | Cost of an idle and a busy channel; workspace pipeline limits | numbers for a per-app quota |
| C7 | Onboarding by the API: which grants its SP needs for roles, views, pipelines, and `run_as`; and option (b), the hub pool | works without a human |
| C8 | QUERY_RESULT_CHANGES: MV refresh plus a keyed diff | correct ADDED / UPDATED / REMOVED on a census query |

Order: C2 and C3 first. They decide whether tier 2 works, and tier 2 covers most of "more than a filter" with no
restart. Then C1, C4, and C5 for tier 3, and C7 for self-service onboarding.

## Open questions

- **Quota and cost:** channels are one continuous pipeline per subscriber. Who pays, and what's the per-app limit?
- **History size:** how many hours of `recent_` history tier 2 keeps, which bounds both cost and the longest window.
- **Shared reference data:** which lookup tables (units, providers, care teams) the platform publishes for channels to join.
- **Platform-built sources:** a structure many apps want (orders ⋈ results) could be built once as a shared source
  instead of per channel. When should the API suggest that?
- **Name:** confirm "channel" with the customer's integration team.
