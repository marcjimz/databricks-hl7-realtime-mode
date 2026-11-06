# Example subscriber (Java 17)

An example of an external application using the platform. It isn't part of the platform: any language can do the same
through the contract ([docs/integration.md](../../docs/integration.md)). It subscribes to gold records at runtime with
Databricks SQL, then consumes its own Event Hub, signing in only as its Databricks service principal.

**The API client is generated from the contract.** At build time, Maven runs openapi-generator on
[`docs/api/openapi.json`](../../docs/api/openapi.json). That produces `SubscriptionsApi` (one Java method per
operation: `declare`, `getStatus`, `putSubscription`, `cancel`, `validate`) and `api.model` (one class per request,
response, and the `Notification` message) under `target/generated-sources`. The app's code uses only those types, so it
can't drift from the API.

## Prerequisites

| # | What | Setting | Where it comes from |
|---|---|---|---|
| 1 | **A Databricks service principal** (the app's identity): host, client ID, OAuth secret | Profile `[<app>]` in `~/.databrickscfg` (`databricks.profile`, default: the app's name) | The app's team creates it. The secret stays in the profile, never in a settings file. |
| 2 | **The Subscription API's URL** | `api.url` | `make registry` |
| 3 | **The Unity Catalog service credential for Event Hubs (Kafka)**, with ACCESS for this service principal only | `eventhubs.credential` (`sub_<app>_eventhubs`) | Onboarding (`make up` → `make uc`) |
| 4 | **The Event Hubs namespace** (Kafka bootstrap `<namespace>.servicebus.windows.net:9093`) | `eventhubs.namespace` | `make registry` |
| — | The topic to read, with read access | none: the API returns it (`destination`) on every declare | Onboarding creates it with Data Receiver for this app only |
| — | Onboarding as a subscriber: the sources and columns it may read, and CAN_USE on the API | none | The platform team: `subscriber_apps` in `infra/azure/terraform.tfvars`, then `make up` |
| — | Java 17+ and Maven 3.9+ | none | If your network blocks Maven Central, point Maven at a mirror in `~/.m2/settings.xml` |

`make registry` writes settings 2–4 (and the profile name) to `apps/<app>.properties`. That file is gitignored, holds
no secrets, and is rewritten on every run because URLs change when the workspace is rebuilt. Any setting can also be
passed as `key=value`. `consumer.group` is optional and defaults to the profile name.

## Run

```bash
# from the repo root
mvn -q -f examples/subscribers/java package                           # generate the client, build, run the unit tests
java -jar examples/subscribers/java/target/hl7-subscriber.jar         # declare MySubscriptions, then print notifications
java -jar examples/subscribers/java/target/hl7-subscriber.jar --e2e   # 24 live checks: a PASS/FAIL table, exit 1 on any failure
```

With more than one file in `apps/`, name the app: `java -jar … examples/subscribers/java/apps/<app>.properties`.
`--duration=PT5M` bounds a run. The walkthrough with expected output is in the
[README](../../README.md#subscriptions-demo).

## Code

Classes by role (`src/main/java/com/databricks/hl7/subscriber/`). Read the first two first:

| Class | What it does |
|---|---|
| `MySubscriptions` | What this app receives: name → SQL. The file you edit: stop the app, edit, rebuild, run. |
| `App` | The entry point. Settings, sign-in as the SP, then `api.declare(...)`, `api.getStatus(wait)` until READY, then listen. Exit codes 2–6. |
| `api/Api` | Wires the generated `SubscriptionsApi`: the URL, the SP's token on every call, and reading a refusal (`Refusal`: Databricks' error per rule) |
| `notifications/Listener` | The delivery contract on the generated `Notification`: dedupe by `notificationId`, current revisions only, catch-up summary, commit after handling |
| `platform/EventHubs` | The Unity Catalog-issued Entra token, the Kafka OAUTHBEARER callback, the consumer settings |
| `e2e/E2E` | The 24 live checks (`--e2e`) |

In the workspace, the same checks run as a serverless job, as the app's service principal:
`DATABRICKS_BUNDLE_ROOT=examples/subscribers/java databricks bundle deploy`, then `... bundle run e2e` (from the repo
root). Binding the job to the service principal needs the deployer to have **Service Principal: User** on it.
