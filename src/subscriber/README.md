# Subscriber app (Java 17)

A reference client. It subscribes to gold records at runtime with Databricks SQL, and consumes its own Event Hub. It
signs in only as its Databricks service principal. The logic lives on the server: the Subscription API, Databricks,
and the hub. This app declares rules, waits, and listens.

Setup, security model, API, and rule reference: [docs/subscriptions.md](../../docs/subscriptions.md). The full demo,
with expected output, is in the [README](../../README.md#subscriptions-demo).

```bash
# from the repo root (or from here, with mvn -q package and java -jar target/hl7-subscriber.jar)
mvn -q -f src/subscriber package                           # build and run the unit tests; again after each edit
java -jar src/subscriber/target/hl7-subscriber.jar         # declare MySubscriptions, then print notifications; Ctrl-C to stop
java -jar src/subscriber/target/hl7-subscriber.jar --e2e   # the end-to-end checks: a PASS/FAIL table, exit 1 on any failure
```

One package (`src/main/java/com/databricks/hl7/subscriber/`):

| Class | What it does |
|---|---|
| `MySubscriptions` | What this app receives: name → SQL. Stop the app, edit, rebuild, run. |
| `App` | Settings, sign-in as the SP, declare to the API, wait for the hub, listen. Exit codes 2–6. |
| `SubscriptionApi` | The HTTP calls: declare, put, cancel, status (with wait), validate |
| `EventHubs` | Unity Catalog-issued Entra token, the Kafka OAUTHBEARER callback, the consumer settings |
| `Listener`, `Notification` | The delivery contract: dedupe by `notificationId`, current revisions only, catch-up summary, commit after handling |
| `E2E` | The 24 live checks |

- `apps/<app>.properties` (gitignored): written by `make registry` on every run, because URLs change when the
  workspace is rebuilt. The profile name defaults to the app name. No secrets.
- Any setting can also be passed as `key=value`, and `--duration=PT5M` bounds a run.
- In the workspace, the same checks run as a serverless job, as the app's service principal:
  `DATABRICKS_BUNDLE_ROOT=src/subscriber databricks bundle deploy`, then `... bundle run e2e` (from the repo root).
- A client in another language needs the same three things: an HTTP call to the API, a Kafka consumer with an
  OAUTHBEARER token from Unity Catalog, and the dedupe/current-revision rule from `Listener`.
