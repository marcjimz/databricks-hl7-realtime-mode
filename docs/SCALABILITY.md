# Scalability, security, and production-readiness test plan

This plan is for showing that runtime subscriptions hold up with many apps, many subscriptions, and changing load,
and that apps stay isolated from each other while that happens. Today's evidence: one live app with a handful of
rules (20/20 end-to-end checks), plus 25 registry tests against real PostgreSQL. Nothing here has been run at scale
yet. Each section names the test that will prove it, and the pass bar.

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
| Lakebase connections | by CU: 105 at 0.5 CU … 1,795 at 8 CU (min(max, 8 × min) rule) | hub: 1 per hub pipeline; Data API pools its own | raise min/max CU; read replica for the hub |
| Lakebase Data API rate | **unpublished** | app subscribe/list calls; the app polls `list` while activating and every 30 s | measure (test P3); ask Databricks for the limit |
| UC temporary service credentials | **unpublished** | one call per app per token lifetime (~1 h), cached | measure (test P3) |
| Azure RBAC role assignments | 4,000 per subscription | 1 per app (Receiver on its hub) | fine to thousands; group-based assignment beyond |
| Hub compute | serverless autoscaling; one filter per rule per source per batch | batch time grows with rules: **the first bottleneck to measure** | single-pass evaluation, then hub shards (below) |

### The hub is the part most likely to need work

Today each micro-batch builds one filter per rule and unions the results. That is simple and correct, but planning
and execution grow linearly with the number of rules. The steps, in order, each only if the previous measurement
calls for it:

1. **Measure** batch time at 10, 100, 1,000, and 10,000 rules per source (test P1).
2. **Single pass per batch:** compute `array(when(rule_1, 1), when(rule_2, 2), …)` in one projection, drop nulls,
   explode, and join to the rule's topic and fields. That's one scan and one plan whatever the rule count, and the
   expressions stay `F.col` and literals.
3. **Index equality rules:** most clinical rules start with `observation_cd = X` or `msg_type = Y`. Group rules by
   that leading equality and join on it (broadcast) instead of evaluating every rule against every event.
4. **Shard the hub:** add `shard` to `subscription_registry.subscriptions` (hash of the subscription ID) and run N
   hub pipelines, each serving `shard = k`. That adds capacity and isolates failures. Each shard keeps its own
   checkpoint and heartbeat.

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
| S11 | Predicate on a column it isn't granted (oracle) | FIELD_DENIED | live |
| S12 | Revocation: remove X from `subscriber_apps`, `make up` | subscribe → APP_NOT_ONBOARDED; hub stops X's rules within 1 batch; token mint denied; record time to effect | not yet |
| S13 | Expired or other-workspace token on the Data API | 401 | not yet |
| S14 | A user (not an SP) token | registry refuses (no role); the app exits 5 | live (exit 5 path) |

Plus two fuzz tests:
- **Predicate fuzzing:** property-based, random trees, unicode, huge literals, deep or wide nesting, wrong types, and
  extra keys. Every input must give ACCEPTED or a domain error, never a 5xx.
- **Differential check:** every ACCEPTED predicate compiles in `hl7rt.subscriptions` and renders the same semantics.
  Run it in CI against a local PostgreSQL, the way `tests/test_registry.py` runs.

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
| P3 | Many real apps | 1 → 10 → 50 SPs running the Java app at once | Data API p99, UC token calls, activation time (subscribe → ACTIVE), Event Hubs connections | activation p99 < 15 s; no 429s |
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
| E9 | Rebuild (`make up` after a wipe) | E2E 19/19 PASS on the new environment |

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
