# Subscriber app (Java 17)

A local app that subscribes to gold records at runtime and consumes its own Event Hub. It signs in only as its
Databricks service principal. Setup, security model, and rule reference: [docs/subscriptions.md](../../docs/subscriptions.md).
The full demo, with expected output, is in the [README](../../README.md#subscriptions-demo).

```bash
# from the repo root (or from here, with mvn -q package and java -jar target/hl7-subscriber.jar)
mvn -q -f src/subscriber package                 # build and run the unit tests; again after each edit
java -jar src/subscriber/target/hl7-subscriber.jar         # register MySubscriptions, then print notifications; Ctrl-C to stop
java -jar src/subscriber/target/hl7-subscriber.jar --e2e   # the end-to-end checks: a PASS/FAIL table, exit 1 on any failure
```

- `MySubscriptions.java`: what this app receives. Stop the app, edit, rebuild, run.

Code (`src/main/java/com/databricks/hl7/subscriber/`), one package per responsibility. Tests mirror it:

| Package | What it does |
|---|---|
| `App`, `MySubscriptions` | Entry point (sign in → register → wait for the hub → listen) and the rules you edit |
| `config/` | `SubscriberConfig`: the settings file plus `key=value` args; refuses secrets |
| `identity/` | `Databricks`: signs in as the service principal (OAuth M2M), supplies the bearer token for the Data API |
| `rules/` | `Predicate`, `Subscription`: what an app can ask for, as structured JSON (never SQL) |
| `registry/` | `RegistryClient` (Lakebase Data API), `Sync` (code → subscribe/cancel), `Activation` (wait for the hub) |
| `eventhubs/` | `EventHubsTokens` (Unity Catalog-issued Entra tokens), `UcOAuthBearerHandler`, `KafkaSettings` |
| `delivery/` | `Notification`, `Listener`: dedupe, skip other revisions, catch up on backlog, commit after handling |
| `e2e/` | `E2E`: the 20 live checks |
- `apps/<app>.properties` (gitignored): written by `make registry` on every run, because the registry URL changes when
  the workspace is rebuilt. The profile name defaults to the app name. No secrets.
- Any setting can also be passed as `key=value`, and `--duration=PT5M` bounds a run.
- In the workspace, the same checks run as a serverless job, as the app's service principal:
  `DATABRICKS_BUNDLE_ROOT=src/subscriber databricks bundle deploy`, then `... bundle run e2e` (from the repo root).
