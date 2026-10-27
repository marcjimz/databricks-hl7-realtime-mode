# Phase 0 — Real-Time Mode feasibility results

Measured 2026-10-22 on `fevm-real-time-mode-demo` (AWS us-east-1) against Azure Event Hubs Standard
(`eastus`, 2 TU, 4 partitions per hub). Synthetic HL7 v2 (ADT^A04 / ORU^R01 / ORM^O01) at ~50 msg/s for 5 minutes.

## Topology under test
```
generator ─► hl7.raw ─[RTM bronze]─► hl7.bronze ─[RTM silver: native HL7 parse]─► hl7.silver
          ─[RTM gold: explode OBX]─► hl7.gold.observations ─[RTM sub: critical lab filter]─► hl7.sub.critical_lab
                                     hl7.silver ─[RTM sub: 1-min tumbling window count]─► hl7.sub.ed_arrival_surge
```
Each hop is its own Lakeflow Spark Declarative Pipeline (`continuous: true`, one `@dp.update_flow` with
`pipelines.trigger = RealTime`, Kafka sink via `dp.create_sink`).

## How latency is measured
Event Hubs stamps every record with **LogAppendTime** (`timestampType = 1`), which is the broker enqueue time. Each layer
carries the previous layer's broker timestamp forward in its payload. Hop latency is
`this_topic.timestamp - previous_topic.timestamp`, all on the broker's clock, with no client clock skew. Each hop
therefore includes consumer fetch + Spark processing + produce + Event Hubs append.

## Results

### Serverless pipelines
| Hop | n | p50 ms | p90 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|
| producer → raw (EH ingest, reference) | 14,821 | 6 | 23 | 32 | 94 | 277 |
| raw → bronze | 14,821 | 21 | 52 | 82 | 198 | 890 |
| bronze → silver (HL7 parse) | 14,821 | 16 | 47 | 63 | 187 | 1,015 |
| silver → gold (explode OBX) | 22,470 | 16 | 48 | 79 | 437 | 2,813 |
| gold → subscription | 5,828 | 16 | 62 | 79 | 266 | 2,797 |
| **raw → subscription (4 hops, e2e)** | 5,828 | **94** | 203 | 281 | 968 | 3,000 |

The event log shows no flow failures during this run, so its 1–3 s outliers were real latency (broker or epoch
boundary), not restarts.

### Classic fixed-size pipelines (1× m6id.xlarge worker = 4 slots, on-demand, no Photon)
| Hop | n | p50 ms | p90 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|
| producer → raw | 16,528 | 5 | 24 | 40 | 85 | 456 |
| raw → bronze | 16,529 | 22 | 62 | 117 | 6,522 | 9,829 |
| bronze → silver | 16,532 | 16 | 47 | 141 | 6,654 | 9,968 |
| silver → gold | 24,762 | 16 | 32 | 47 | 156 | 891 |
| gold → subscription | 6,432 | 16 | 47 | 94 | 4,683 | 4,735 |

### Diagnosis: the multi-second outliers were flow crashes, not slow checkpoint boundaries
My first read was that these were slow RTM epoch boundaries (the long-running "batch" that acts as a checkpoint interval),
because they lined up with each pipeline's 5-minute mark. **The event log shows otherwise.** At each of those moments the
flow **failed** and SDP restarted it:

```
[CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT] The minimum number of free slots required in the cluster is 4,
however, the cluster has only has 3 slots free. Query will stall or fail. Increase cluster size to proceed.
```

At an epoch boundary, RTM's concurrent scheduler must start every task of the next epoch while the last task of the
previous one is still releasing its slot. With exactly 4 slots for 4 tasks there was no room, so the flow crashed. The
~6–10 s was the automatic restart. A normal epoch boundary should take ~0.5 s (under 3 s per the internal RTM runbook).

**Rule:** slots ≥ total tasks across all stages (docs: Compute sizing) **plus headroom** (runbook: target
`busyTimeFraction` ≈ 0.8). Always check the event log for flow `WARN`/`ERROR` before reading latency numbers.

### Experiment 2 (invalid): async progress tracking
`asyncProgressTrackingEnabled` is **rejected** on SDP classic clusters (standard access mode:
`UNSUPPORTED_STREAMING_OPTIONS_PERMISSION_ENFORCED`). The flows crash-looped, so the run is discarded. It isn't needed
anyway: on **DBR 18.3** async progress tracking (stateless) and async state checkpointing (stateful) are on by default.

### Experiment 3: correctly sized, Event Hubs Standard (live telemetry, steady state, zero flow failures)
Stateless hops: 2 workers = 8 slots for 4 tasks. Windowed subscription: 3 workers = 12 slots for 8 tasks.
`maxPartitions = 4`. Continuous generator at 50 msg/s. Live telemetry streaming tables, 2-minute window after backlog drained.

| Hop | p50 ms | p95 ms | p99 ms | max ms | < 200 ms p99 |
|---|---|---|---|---|---|
| producer → raw (EH ingest, reference) | 13 | 59 | 86 | 248 | n/a |
| raw → bronze | 23 | 81 | 272 | 1,391 | ❌ |
| bronze → silver | 31 | 78 | 219 | 984 | ❌ |
| silver → gold | 16 | 63 | 125 | 985 | ✅ |
| gold → subscription | 16 | 47 | 94 | 234 | ✅ |

- Outliers are uniform across partitions (not a slow task) and appear as isolated ~1 s blips on one hop at a time.
- Each hop makes two broker round trips (consume + produce). The producer → EH append alone has p99 86 ms, so the remaining
  per-hop tail is dominated by **Event Hubs Standard** (multi-tenant) rather than Spark (p50 16–31 ms).

### Experiment 4: same, on Event Hubs Premium (1 PU, isolated compute)
Same pipelines, sizing, and load. Repointed through bundle variables, with fresh checkpoints (full refresh, start at latest).

2-minute steady window (no epoch boundary inside), zero flow failures:

| Hop | p50 ms | p95 ms | p99 ms | max ms | < 200 ms p99 |
|---|---|---|---|---|---|
| producer → raw (EH ingest, reference) | 15 | 28 | 33 | 164 | n/a |
| raw → bronze | 10 | 21 | 31 | 110 | ✅ |
| bronze → silver | 15 | 16 | 31 | 156 | ✅ |
| silver → gold | 15 | 16 | 31 | 94 | ✅ |
| gold → subscription | 15 | 16 | 32 | 172 | ✅ |
| **raw → subscription (e2e, 4 hops)** | | | **78** | | ✅ |

Soak across ≥2 RTM epoch boundaries (15-minute window, 44k+ messages per hop, zero flow failures):

| Hop | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|
| raw → bronze | 13 | 21 | 39 | 1,312 |
| bronze → silver | 11 | 16 | 32 | 1,282 |
| silver → gold | 15 | 16 | 47 | 1,584 |
| gold → subscription | 15 | 16 | 46 | 3,721 (pipeline startup at 02:58) |

**p99 passes on every hop. The max does not: each pipeline pauses ~0.6–1.6 s at every RTM epoch boundary, 5 minutes apart.**

| Pipeline | Boundary pause 1 | Boundary pause 2 (+5 min) |
|---|---|---|
| sub_critical_lab | 03:03:44 (1,358 ms) | 03:08:45 (578 ms) |
| silver | 03:04:34 (656 ms) | 03:09:35 (953 ms) |
| bronze | 03:05:09 (1,094 ms) | 03:10:10 (810 ms) |
| gold | 03:05:11 (1,584 ms) | 03:10:12 (922 ms) |

Each pause affects ~20–60 messages (~0.1%). That's within the runbook's "normal" inter-batch time (< 3 s), but above
a strict "no message > 200 ms" bar. Open item: break down the inter-batch time from driver logs (suspect: Kafka client
reconnect/TLS+SASL to Event Hubs at each new epoch) and test the checkpoint interval in both directions.

**Measurement resolution.** Event Hubs LogAppendTime advances in ~16 ms ticks (Windows timer resolution), and broker
clocks differ by a few ms across partitions. Hop latency is therefore accurate to about ±16 ms: a hop inside one tick
reads 0 ms, and one crossing a tick reads 15–16 ms. That's fine for a 200 ms target. For sub-16 ms precision, use RTM's
built-in `processingLatencyMs`.

## Findings
- **RTM works natively in SDP** (GA). Kafka/Event Hubs in and out. Delta is not a valid RTM source or sink, so Delta
  tables are mirrored off the hot path.
- **No data loss** across the chain: every raw message reached bronze and silver, and every OBX reached gold.
- **Native Spark HL7 parsing is effectively free.** bronze → silver (full parse) has the same p50 as the raw → bronze
  passthrough. No JAR or UDF is needed.
- **An Event Hubs hop costs ~16–21 ms at p50.** Chaining four hops gives ~94 ms p50 end to end.
- **Windowed aggregation runs under RTM** (`withWatermark` + tumbling `window` + `count`).
- Gotchas found:
  - Bundle `mode: development` drops `continuous: true` from pipelines. RTM pipelines must deploy in a non-development
    target (or set `continuous` explicitly per target).
  - An RTM flow that first ran in non-RTM mode keeps a **v1 checkpoint** and then fails with
    `CHECKPOINT_FORMAT_V1_NOT_SUPPORTED`. The fix is a full refresh (fresh checkpoint).
  - Continuous pipelines auto-start on `bundle deploy`, so `bundle run` reports "active update already exists".
  - `create_sink` takes a static `topic`, so there is one sink per output topic.
  - RTM progress metrics only reach the event log at the end of the long batch (5 min). Per-record latency comes from
    broker timestamps.
