# Subscriptions: the earlier SQL-file design (superseded)

**Superseded by runtime subscriptions** ([docs/subscriptions.md](../../docs/subscriptions.md)): rules registered at
runtime in a Lakebase registry as structured predicates, one hub pipeline for all apps. Kept for reference.

Self-service subscriptions over the gold topics: a SQL (or YAML) rule per subscription, run in Real-Time Mode
(milliseconds, classic cluster) or micro-batch (seconds, serverless). They were built and measured on the AWS
workspace (p99 31-79 ms gold -> subscription for RTM, ~5 s micro-batch) and are parked here for a later phase. Nothing in
this folder is deployed, linted, or tested.

The layout mirrors the repo root, so restoring is mostly moving files back:

| Here | Back to |
|---|---|
| `src/subscriptions/` | `src/subscriptions/` (rules, `_template*.sql` / `_template.yml`) |
| `src/pipelines/subscriptions/runner.py` | same path |
| `src/hl7rt/subscriptions.py` | same path |
| `src/hl7rt/lakehouse_subscriptions.py` | append to `src/hl7rt/lakehouse.py` |
| `src/pipelines/lakehouse/mirror_subscriptions.py` | append to `src/pipelines/lakehouse/mirror.py` |
| `resources/pipelines/` | `resources/pipelines/` (`sub_*.yml`, `_templates/`) |
| `tests/` | `tests/` (append the `*_subscriptions.py` files to their namesakes) |

Also needed when restoring:
- `infra/azure`: one hub per rule. Add back to `azurerm_eventhub.topics`:
  `fileset("${path.module}/../../src/subscriptions", "[a-z]*.{sql,yml}")`.
- `databricks.yml`: the `rtm_join_cluster` variable (4 x Standard_D4ds_v5 = 16 slots for a join's 12 tasks).
- `Makefile`: `sub-new`, `mirror-restart`, and the `SUBSCRIPTIONS` list.
- The dashboard's subscription bars and the micro-batch panel (see git history before this folder was created).
