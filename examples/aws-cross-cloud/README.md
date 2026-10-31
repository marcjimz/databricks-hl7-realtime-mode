# AWS workspace → Azure Event Hubs (legacy, phases 0–5)

The original setup: an AWS Databricks workspace reaching Event Hubs Premium in Azure eastus across clouds, authenticated
with a SAS connection string in a Databricks secret scope. Results are in [docs/latency.md](../../docs/latency.md)
(phases 0–5). The repo now targets Azure end to end (`infra/azure`); this stack is kept only so the eastus namespace
can still be managed.

- `infra/eventhubs/`: Terraform for that namespace. Your local state and tfvars live next to it (gitignored).
- To delete the namespace: `terraform -chdir=examples/aws-cross-cloud/infra/eventhubs destroy`.
- Its subscription-hub `fileset` points at `src/subscriptions`, which is now `examples/subscriptions/src/subscriptions`.
