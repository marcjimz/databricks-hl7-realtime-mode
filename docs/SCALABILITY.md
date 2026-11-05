# Scalability, security, and production-readiness test plan

This plan is for showing that runtime subscriptions hold up with many apps, many subscriptions, and changing load,
and that apps stay isolated from each other while that happens.

Today's evidence:
- one live app, with 24/24 end-to-end checks through the Subscription API;
- registry and API tests against a real PostgreSQL;
- a first hub measurement at 1, 100, and 1,000 rules (below). It shows the hub's batch time growing with the rule
  count.

Each section names the test that will prove it, and the pass bar.

## What has to scale

| Dimension | Drives | Today |
|---|---|---|
| Subscriber apps (unique service principals) | hubs, consumer groups, access connectors, UC credentials, Postgres roles | 1 |
| Subscriptions per app and in total | hub batch time, registry reload size | ≤ 3 |
| Gold event rate | hub input per batch | ~200 msg/s HL7 (plus Epic JSON) |
| Match fan-out (matches per event) | hub output, Event Hubs ingress | low |
| Rule churn (changes per minute) | registry writes, reloads, activation time | manual |
| Consumers per app | partitions per app hub | 1 |

## Limits and where each one bites

| Component | Hard limit (published) | Consequence for this design | Lever |
|---|---|---|---|
| Event Hubs Premium hubs | 100 × PU per namespace (max 16 PU) | one hub per app: ~390 apps at 4 PU, ~1,590 at 16 PU (minus the 8 hot-path hubs) | more PU, more namespaces |
| Event Hubs partitions | 200 × PU per namespace; 100 per hub | app hubs at 4 partitions: ~190 apps at 4 PU. At 1 partition: ~760 | `partition_count` per app (1 for low-volume apps) |
| Kafka consumer groups | 1,000 per namespace | one group per app instance set, plus the E2E's throwaway groups | delete stale groups; namespace sharding |
| Brokered connections | 10,000 × PU | each app consumer ≈ 1 connection per broker it reads | fine to thousands of apps |
| Lakebase connections | by CU: 105 at 0.5 CU … 1,795 at 8 CU (min(max, 8 × min) rule) | hub: 1 per hub pipeline; the API: a small pool per instance | raise min/max CU; read replica for the hub |
| Subscription API | 1–5 fixed instances per app (autoscaling: future private preview); each declare compiles new rules on a SQL warehouse | app declare/wait calls; the app polls status every 30 s | more instances; warehouse sizing; measure (test P3) |
| UC temporary service credentials | **unpublished** | one call per app per token lifetime (~1 h), cached | measure (test P3) |
| Azure RBAC role assignments | 4,000 per subscription | 1 per app (Receiver on its hub) | fine to thousands; group-based assignment beyond |
| Hub compute | serverless autoscaling; one filter per rule per source per batch | batch time grows with rules: **the first bottleneck to measure** | single-pass evaluation, then hub shards (below) |

### The hub is the part that needs work (measured)

The hub evaluates every rule of a source in one projection per micro-batch: the matching rules per record, as an array,
then explode, in chunks of 200 rules. That's step 2 of the ladder below, built. First measurement (P1, 2026-11-05, demo
load, every rule matching 0.1% of patients, one source):

| Rules | Hub batch interval | Delivery p50 | p99 | Keeps up |
|---|---|---|---|---|
| 1 | ~2–3 s | 2.7 s | 4.7 s | yes |
| 100 | 6.5–8.4 s | 18 s | 24 s | barely |
| 1,000 | ~67 s | 155 s | 171 s | **no**: the backlog grows |

All 1,000 rules compiled on serverless and went ACTIVE, with none FAILED. The data per batch is small, so batch time
growing with rule count points at per-batch, per-rule work on the driver, not at data volume. A bigger driver isn't the
lever, and serverless doesn't size the driver anyway. Suspects, to measure before fixing:
- each rule re-checked with sqlglot every batch;
- a 1,000-expression plan rebuilt and analyzed every batch;
- each 200-rule chunk re-parsing the batch.

The ladder, each step only if the previous measurement calls for it:

1. **Measure each phase** per batch: refresh, check, plan, write. Timings go to a `hub_progress` table.
2. **Single pass per batch** (built): one projection per chunk of rules.
3. **Do per change what doesn't change per batch:**
   - check and compile the rules once per registry generation, not every batch;
   - parse the batch once, not once per chunk.
4. **Index rules by their leading equality.** Most clinical rules start with `observation_cd_desc = 'Potassium'` or
   `observation_cd = '2823-3'`. Group rules by that key and join each record to just the rules for its key (a broadcast
   join against a small rules table); only the rest of the filter is evaluated. For example, 10,000 rules over
   200 lab codes is about 50 evaluations per record instead of 10,000.
5. **Shard the hub:**
   - **How:** the API assigns each rule a `shard` (hash of its subscription ID, or by app for tenant isolation) when it's
     registered. N hub pipelines each read the whole gold topic and evaluate only their shard's rules, with their own
     checkpoint and heartbeat.
   - **Gains:** capacity grows with N, planning is split across N drivers, and a bad batch on one shard doesn't stall
     the others.
   - **Costs:** each shard reads the full gold stream (N× Event Hubs read) and has its own compute.

The Subscription API scales separately. It's stateless, with 2 fixed instances; Databricks Apps allows up to 5, and
load-based autoscaling is a future private preview. Its load is registration calls and waits, not the data path. Test:
concurrent `PUT` + `GET ?wait` from many SPs at 2 vs 5 instances, against p99 latency and errors.

## Test plan

Each test runs against a dedicated environment built by `make up` with a `perf` prefix, never the demo. It reports
p50/p99/max plus resource use, and is repeated twice.

### Security and isolation (must pass before any scale claim)

Run with at least **three real service principals** (A, B, C), each onboarded through `subscriber_apps`. Each check
runs for every ordered pair (X attacks Y):

| # | Attempt by X against Y | Expected | Covered today |
|---|---|---|---|
| S1 | `list_subscriptions` | sees only its own | Postgres test |
| S2 | `unsubscribe` Y's subscription name | NOT_FOUND_OR_INACCESSIBLE; Y unaffected | Postgres test |
| S3 | Direct SQL insert claiming `owner = Y` | row owned by X (trigger) | Postgres test |
| S4 | Direct SQL update of Y's row | 0 rows (RLS) | Postgres test |
| S5 | Same subscription name as Y | separate subscriptions; no conflict (unique per owner) | Postgres test |
| S6 | Change `apps` (its own grants) or write `status` | permission denied | Postgres test |
| S7 | Mint a token from Y's UC credential | PERMISSION_DENIED | live, against the hub credential |
| S8 | Read Y's hub with X's token | TopicAuthorizationException | live, against a gold topic |
| S9 | **Write** to its own hub, or Y's (forge notifications) | denied: app connectors have Receiver only | live (own hub) |
| S10 | Read any gold or hot-path topic | TopicAuthorizationException | live |
| S11 | Filter on a column it isn't granted (oracle) | UNRESOLVED_COLUMN (its view has no such column) | live |
| S12 | Revocation: remove X from `subscriber_apps`, `make up` | subscribe → APP_NOT_ONBOARDED; hub stops X's rules within 1 batch; token mint denied; record time to effect | not yet |
| S13 | Expired or other-workspace token on the Subscription API | 401 | not yet (anonymous → 401 verified) |
| S14 | A user (not an SP) token | registry refuses (no role); the app exits 5 | live (exit 5 path) |

Plus two fuzz tests:
- **Rule fuzzing:** property-based SQL: random filters, unicode, huge literals, deep or wide nesting, wrong types, and
  odd syntax. Every input must give ACCEPTED, Databricks' error, or `SQL_NOT_ALLOWED`, never a 5xx.
- **Differential check (built for each operator):** `tests/test_subscriptions.py` compares the hub with `spark.sql` for
  the same rule over the same records. Extend it to generated rules.

**Concurrency:** two instances of the same app revise one subscription at once (100 rounds). Revisions must be
strictly increasing with no lost update (the final row equals one of the writes), and the hub ends on the latest
revision.

**Must fix before production:**
- **Per-app quota:** max subscriptions and max predicate nodes per app, enforced in the trigger with a QUOTA_EXCEEDED
  error. Today one app could register thousands of rules and slow the hub for everyone.
- **Hub identity:** a dedicated service principal with SELECT on the registry and write access to `status` and
  `hub_heartbeat` only, instead of the deploying user (who owns the registry and bypasses RLS).
- **App credentials:** workload identity (Entra federated credentials) for apps in production, instead of
  client secrets in `~/.databrickscfg`.

### Performance

The registry and the hub can be loaded without creating hundreds of service principals. **Virtual apps** are rows in
`apps` with synthetic roles that route to a handful of test hubs, seeded by a platform-only function that the API
can't reach. Real service principals are only needed for the paths that authenticate (P3).

| # | Test | Vary | Measure | Pass bar (proposed) |
|---|---|---|---|---|
| P1 | Hub rule scaling | rules per source 10 → 10,000; match ratio 0.1% / 1% / 10% | batch duration, gold → app latency (`gold_ts` vs the app hub's broker time) | p99 delivery < 5 s at 1,000 rules |
| P2 | Event rate | gold 200 → 1,000 → 5,000 msg/s at 1,000 rules | same, plus hub input rows per batch | keeps up (no growing lag) |
| P3 | Many real apps | 1 → 10 → 50 SPs running the Java app at once | Subscription API p99, UC token calls, activation time (subscribe → ACTIVE), Event Hubs connections | activation p99 < 15 s; no 429s |
| P4 | Rule churn | 1 → 100 changes per minute across apps | reload time, activation p99, hub batch time | activation p99 < 15 s |
| P5 | Fan-out | one event matching 1 → 500 apps | hub output rate, Event Hubs ingress, namespace CPU | namespace CPU < 70% |
| P6 | Registry | 10,000 → 1,000,000 subscription rows | `max(generation)` and reload time; Lakebase CPU | poll < 50 ms p99 |
| P7 | Soak | P1 at 1,000 rules for 12 hours | the above, plus memory and duplicate counts | no drift, no growth |

**Correctness at scale:** for every test, recompute the expected matches from the gold Delta tables using each
rule's `predicate_sql` rendering, and compare with the delivered notifications after dedup by `notificationId`. The
sets must be equal: no missing match, no extra one, nothing for a cancelled or superseded revision.

**Instrumentation to add first:** the hub writes per-batch rows (rules, input rows, matches, batch ms) to a
`hub_progress` table, as the spike did. Then a dashboard tile per test.

### Elasticity and failure

| # | Scenario | Expected |
|---|---|---|
| E1 | Load step ×5 on gold | serverless hub scales up; delivery p99 recovers within 5 min |
| E2 | Lakebase scaled to zero, then the first subscribe | succeeds; cold-start latency recorded |
| E3 | Lakebase min/max CU change under load | no failed calls |
| E4 | Event Hubs PU change under load | no data loss; record the latency blip |
| E5 | Kill the hub mid-batch | replays from its checkpoint; duplicates only, all with known `notificationId`s |
| E6 | Database token expiry (hourly) during a long run | hub reconnects; no gap in heartbeat > 2 min |
| E7 | App crash between poll and commit | replays its uncommitted records; app dedupes |
| E8 | Event Hubs throttling (exceed PU) | hub backs off; the heartbeat keeps advancing; app warns if not |
| E9 | Rebuild (`make up` after a wipe) | E2E 24/24 PASS on the new environment |

## How elastic scaling works, per component

- **Hub compute:** serverless SDP sizes the pipeline. Past one pipeline, run hub shards (above).
- **Registry:** Lakebase autoscales between min and max CU; size min CU from connection needs (min × 8 caps the
  effective size). Add a read replica for the hubs if reads dominate.
- **Event Hubs:** PUs scale manually (1 to 16; Premium has no auto-inflate). Past one namespace, assign apps to
  namespaces (`subscriber_apps.<app>.namespace`); the hub writes to each through its own credential.
- **App hubs:** partition count per app sets that app's consumer parallelism. Default 4; 1 for low-volume apps.

## Phases

1. **Instrument and quota:** `hub_progress`, the per-app quota, and the hub service principal.
2. **Isolation suite** with three real SPs (S1–S14) and the fuzzers, in CI where possible.
3. **P1 and P6**, then the single-pass hub if P1 misses the bar.
4. **P2–P5 and E1–E9**, then the 12-hour soak (P7).
5. **Results** in `docs/latency.md`, with p50/p99/max and what wasn't verified.

## Implementation plan (agreed, not built yet; the hub items above come first)

**Where it runs:** on the dev environment, not a separate stack, with dedicated perf hubs. Don't run scale tests during
RTM latency soaks.
**Identities:** three real service principals: `sp-hl7-realtime-demo` plus two more (A, B, C).

```
subscription_scale job (platform bundle, serverless, runs as the deployer)
  seed     virtual apps = NOLOGIN Postgres roles perf_NNNN in `subscriber`, onboarded in `apps`, routed to perf hubs.
           Rules are registered by SET ROLE perf_NNNN + subscription_api.subscribe: the real trigger and RLS, no bypass.
  load     optional: an extra hl7-generator task at the scenario's rate, on top of the demo feed (no pipeline restarts)
  churn    optional: revise k rules per minute as their owners (P4)
  measure  after warm-up, over a window [t0, t1]: hub_progress percentiles, delivery latency, correctness vs an oracle
  clean    run_if ALL_DONE: drop the perf subscriptions, apps rows, and roles
results → Delta <catalog>.<schema>.scale_results (run_id, scenario, metric, p50, p99, max, bar, pass), also printed.
          The task exits non-zero if any bar fails.
```

1. **Hub instrumentation**
   - A platform-only `subscription_registry.hub_progress` table: source, batch_id, started_at, rules, input_rows,
     refresh_ms, batch_ms.
   - `Registry.progress()` writes it, under the existing lock.
   - The hub times each batch and counts its input rows.
2. **Per-app quota:** an `apps.max_subscriptions` column (default 100) and a `QUOTA_EXCEEDED` error in `stamp()`.
3. **Perf hubs:** a Terraform variable `perf_hubs` (default 0) creates `<catalog>.subscribers.perf-<k>`, with
   4 partitions each and no app connector. Only the platform reads them. Premium bills per PU, so these add no cost.
4. **`hl7rt.scale`** (unit-tested), with a console script `hl7-scale` (`seed|churn|measure|clean`):
   - **Synthetic rules with an exact match ratio:** patient MRNs are uniform, so `patient_id ends_with "7"`, `"17"`,
     and `"117"` match 10%, 1%, and 0.1%. Each rule combines that with realistic `eq`, `ge`, and `in` clauses.
   - **Scenarios:**

     | Test | Scenarios |
     |---|---|
     | P1 | p1-10 … p1-10000, and p1-1000-m10 |
     | P2 | p2-1000-r1000 and p2-1000-r5000 |
     | P4 | p4-churn-100 |
     | P6 | p6-rows-1m |

   - **Correctness oracle:**
     1. A batch read of the gold topic over the window applies each ACTIVE revision, independent of streaming,
        batching, routing, and redelivery.
     2. It compares the set {(subscriptionId, revision, sourcePartition, sourceOffset)} with the delivered
        notifications, deduplicated by `notificationId`.
     3. A run passes only with 0 missing and 0 extra. It also reports the duplicate rate.
   - **Delivery latency:** the app hub's broker time minus `gold_ts`.
   - **Activation time:** `status.observed_at` minus `subscriptions.updated_at`.
5. **Job `resources/jobs/subscription_scale.yml`:** `databricks bundle run subscription_scale --params scenario=p1-1000`.
6. **Semantics tests in CI** (local PostgreSQL + Spark):
   - **Differential:** Postgres `render()` and Spark `compile_predicate` select the same rows.
   - **Fuzzing:** with `hypothesis`, every input gives ACCEPTED or a domain error, never an internal error.
   - **Concurrency:** 100 concurrent revisions with no lost update.
7. **Live isolation suite** (Java, real SPs):
   - `java -jar … --isolation` runs as app X against every other onboarded app, after each has registered an
     `iso-canary`.
   - It covers S1, S2, S5, S7–S10, and S13, and prints a PASS/FAIL table.
   - Looping over the three apps covers all 6 ordered pairs.
   - S12 (revocation) becomes a timed runbook step.
8. **Docs:** a "Run it" section, a Results table filled from the first runs, and AGENTS.md rules.

**Out of scope until a measurement calls for it:** the single-pass or indexed hub (only if P1 misses its bar), a
dedicated hub service principal, workload identity, P5 at 500 apps, and the 12-hour soak (P7).

**Prerequisites (your steps):**
1. Create the two service principals and their secrets. You can do it from the CLI: `databricks service-principals
   create`, then `databricks service-principal-secrets-proxy create <id>`, writing the secret straight into a
   `~/.databrickscfg` profile named after the app.
2. Add both to `subscriber_apps`, set `perf_hubs = 4`, and run `make up`.
