# Subscriber app (Java 17)

A local app that subscribes to gold records at runtime and consumes its own Event Hub. It signs in only as its
Databricks service principal. Setup, security model, and rule reference: [docs/subscriptions.md](../../docs/subscriptions.md).
The full demo, with expected output, is in the [README](../../README.md#subscriptions-demo).

```bash
mvn -q package                              # build and run the unit tests; again after each edit
java -jar target/hl7-subscriber.jar         # register MySubscriptions, then print notifications; Ctrl-C to stop
java -jar target/hl7-subscriber.jar --e2e   # the end-to-end checks: a PASS/FAIL table, exit 1 on any failure
```

- `MySubscriptions.java`: what this app receives. Edit, rebuild, run.
- `apps/<app>.properties` (gitignored): written by `make registry` on every run, because the registry URL changes when
  the workspace is rebuilt. The profile name defaults to the app name. No secrets.
- Any setting can also be passed as `key=value`, and `--duration=PT5M` bounds a run.
- In the workspace, the same checks run as a serverless job, as the app's service principal:
  `databricks bundle deploy && databricks bundle run e2e` (from `src/subscriber`).
