# Latency

## Current results

Serverless Real-Time Mode (GA), Azure westus2 (workspace, Event Hubs Premium at 4 PU, and storage in one region),
200 msg/s HL7 plus 200 msg/s Epic JSON, 18 minutes at full demo load, measured from the topics:

| Hop | p50 | p95 | p99 | max |
|---|---|---|---|---|
| producer → raw | −1 ms | 14 ms | **57 ms** | 0.6 s |
| raw → bronze | 7 ms | 47 ms | **86 ms** | 1.4 s |
| bronze → silver (full HL7 parse) | 0 ms | 31 ms | **78 ms** | 2.1 s |
| silver → gold | 0 ms | 31 ms | **78 ms** | 1.5 s |

Every hop is under 200 ms at p99, and there are 0 duplicate keys. The max comes from checkpoint boundaries and
occasional Event Hubs connection hangs (both below).

## How it's measured

- Event Hubs stamps every record with its broker time (LogAppendTime). Each layer carries the upstream broker time
  forward (`raw_ts`, `bronze_ts`, `silver_ts`), so `hop = this record's broker time − upstream broker time`. That
  compares broker clocks only.
- Resolution is ±16 ms (Event Hubs timestamps tick in ~16 ms steps). producer → raw compares the generator's clock with
  the broker's, so it can read slightly negative.
- The mirror's streaming tables carry `hop_ms` / `e2e_ms` per record (the dashboard). For an exact, mirror-free reading,
  query a topic with `read_kafka(..., serviceCredential => '<catalog>_eventhubs')` and group by minute or partition.

## What it took (each change measured)

| Change | Why | Effect |
|---|---|---|
| Event Hubs Premium, not Standard | Standard's multi-tenant tail missed 200 ms p99 | p99 per hop ~30–50 ms |
| Microsoft's Kafka client settings (`eventhubs._JAVA_CLIENT`) | Kafka defaults keep connections past Azure's 240 s idle close, so a partition stalled 0.5–1 s | p99 550–875 → 35–47 ms |
| Parse HL7 with staged projections (`hl7v2.with_segments`) | Inlining the segment split built a 379 KB plan with 290 `split()` calls per record | Silver started and kept up |
| 4 PU (default) | 1 PU ran at ~83% namespace CPU, and every hop's tail grew | Namespace-wide ~300 ms stalls halved from 2 PU |
| 5 s request timeout on RTM hops (`rtm.HUNG_REQUEST_GUARD`) | Event Hubs connections occasionally go silent until the 60 s timeout; the hop stalls that long | Hangs capped at ~5 s (were 18–77 s) |
| Checkpoint every 15 minutes (`rtm_trigger_interval`) | Each boundary delays ~2 s of traffic (~0.7% of records per 5-minute epoch) | p99 under 200 ms on every hop |
| Serverless, GA channel | No sizing, no 9-minute classic cold start | Same latency as classic |

Ruled out by measurement:
- **Entra versus SAS auth.** New-connection setup: write 285 vs 291 ms, read 1315 vs 1211 ms.
- **Bursty micro-batch consumers.** Turning the mirror off didn't help.
- **A single slow partition.** The tail was even across all four.

## Still open

- **Why Event Hubs connections hang, and what causes the namespace-wide ~300 ms stalls.** Both are outside Spark. A fresh
  namespace or a private endpoint would test the remaining suspects.
- **The tail beyond p99** (seconds) is not addressed.

The full history (AWS cross-cloud phases 0–5, the classic-compute investigation, driver-log breakdowns) is in
[latency-history.md](https://github.com/marcjimz/databricks-hl7-realtime-mode/blob/de0fd16/examples/aws-cross-cloud/latency-history.md) (removed from the repo with the AWS setup; kept in git history).
