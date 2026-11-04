# Subscriber app (Java 17)

A local app that subscribes to gold records at runtime and consumes its own Event Hub. It signs in only as its
Databricks service principal. Setup, security model, and rule reference: [docs/subscriptions.md](../../docs/subscriptions.md).
The full demo, with expected output, is in the [README](../../README.md#subscriptions-demo).

```bash
mvn -q package                       # build (and run the tests); again after each edit
java -jar target/hl7-subscriber.jar  # register MySubscriptions, then print notifications; Ctrl-C to stop
```

- `MySubscriptions.java`: what this app receives. Edit, rebuild, run.
- `subscriber.properties` (gitignored; copy from `.example`): the SP's profile name, the registry, and the app's
  Event Hubs credential. No secrets.
