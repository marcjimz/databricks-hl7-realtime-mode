# Benchmarks

Load tests for the subscription hub, run against a deployed dev environment. Not part of the platform: they write
benchmark rules into the registry as a virtual app, measure, and remove it.

## patient_lists.py: dynamic patient rules at scale

Many apps, each following its own census of patients with the ED monitor's five rules (one per silver source), while
the ED monitor itself measures how fast an edit goes live and how fast its records arrive. Results and method:
docs/SCALABILITY.md, "Dynamic patient lists at the cap".

Prerequisites:
- `make demo dev`, with the generator running;
- `make registry`, with the `sp-hl7-ed-monitor` app onboarded;
- Azure CLI login as the deployer.

```bash
export DATABRICKS_HOST=$(terraform -chdir=infra/azure output -raw workspace_url) DATABRICKS_AUTH_TYPE=azure-cli
B="uv run python examples/benchmarks/patient_lists.py"

$B seed 990                 # 198 censuses x 5 rules x 2,000 patients (the cap is 1,000 rules across all apps)
$B status                   # wait for [['ACTIVE', 990]]
uv run python examples/subscribers/python/monitor.py examples/subscribers/apps/sp-hl7-ed-monitor.properties \
  --duration=600            # static step: edit -> ACTIVE and delivery
$B churn 30 --minutes=10 &  # churn step: 30 census edits a minute, with the monitor again
$B clean                    # cancel; then, once status shows CANCELLED:
$B offboard                 # remove the virtual app and its role
```

While a step runs, read the hub's side from `<catalog>_registry.subscription_registry.hub_progress`. It has one row per
batch and source:
- `write_ms`: collect, match, and write;
- `age_ms`: the newest record's age when the batch function started;
- `compile_ms`: the rule set's last compile;
- `spark_ms`: Spark's own split of the batch.

Benchmark rules' delivery is on the monitor app's topic (subscription names `perf-*`): compare each record's
`source_ts` with its broker timestamp, using `read_kafka`.

Notes:
- **The benchmark rules belong to their own app, `perf-bench`.** In the monitor's app, the monitor's status would wait
  on every benchmark rule.
- **`make registry` removes `perf-bench`**, because it isn't in Terraform. Don't run it mid-benchmark.
- **Rules are written through the registry's own `subscribe` function,** as the app's role. The owner trigger, RLS,
  and the rule cap all apply. The canonical SQL comes from the API's parser, but the warehouse compile is skipped.
