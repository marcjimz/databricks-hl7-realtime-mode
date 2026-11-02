# The existing Epic JSON feed (Phase 1)

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

Starting offsets: Spark's Kafka source resumes from its **checkpoint**, not the consumer group. a full refresh
(full refresh) starts at the live edge, which is for demos only. In production, always resume from the checkpoint.
