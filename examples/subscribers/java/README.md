# Example subscriber (Java 17)

An example of an external application using the platform. It isn't part of the platform: any language can do the same
through the contract ([docs/integration.md](../../../docs/integration.md)). It subscribes to silver records at runtime with
Databricks SQL, then consumes its own Event Hub, signing in as its Databricks service principal (or, explicitly, as
you: see [Run as yourself](#run-as-yourself)).

**The API client is generated from the contract.** At build time, Maven runs openapi-generator on
[`docs/api/openapi.json`](../../../docs/api/openapi.json). That produces `SubscriptionsApi` (one Java method per
operation: `getConnection`, `declare`, `getStatus`, `putSubscription`, `cancel`, `validate`) and `api.model` (one class per request,
response, and the `Notification` message) under `target/generated-sources`. The app's code uses only those types, so it
can't drift from the API.

## Prerequisites

The app configures **two things**:

| # | What | Setting | Where it comes from |
|---|---|---|---|
| 1 | **A Databricks service principal** (the app's identity): host, client ID, OAuth secret | Profile `[<app>]` in `~/.databrickscfg` (`databricks.profile`, default: the app's name) | The app's team creates it. The secret stays in the profile, never in a settings file. |
| 2 | **The Subscription API's app name** (the app looks up its URL, so a rebuilt workspace needs no change) | `api.app` (or `api.url` to set the URL directly) | `make registry` |

**Everything else comes from the API** (`getConnection`, at start-up):

| What | Field |
|---|---|
| The topic to read (this app's own; Data Receiver for this app only) | `topic` (also `destination` on every declare) |
| The Kafka bootstrap servers | `bootstrapServers` |
| The Unity Catalog service credential for Event Hubs (ACCESS for this service principal only) | `credential` |
| What to request the Kafka token for | `tokenScope` |
| The hub that runs its realtime rules (`edp_shared`, or its team's own) | `hub` |
| The sources and columns it may read | `sources`, `fields` |

Before that works, the platform team onboards the app: `subscriber_apps` in `infra/azure/terraform.tfvars` (the
sources and columns it may read), then `make up`. To build it you need Java 17+ and Maven 3.9+; if your network blocks
Maven Central, point Maven at a mirror in `~/.m2/settings.xml`.

## Which hub runs your rules

The app's code and settings are the same either way. The platform assigns the hub, and `getConnection` names it.

- **The shared hub, `edp_shared` (the default).** There's nothing to deploy. The platform team onboards the app, and
  the shared hub runs its rules.
- **Your own hub**, for a team that wants compute of its own. It's one always-on pipeline, however many apps it serves.
  1. The platform team adds the hub to Terraform (`subscriber_hubs`, with the service principal that runs it). It
     assigns the app to that hub (`subscriber_apps.<app>.subscription_hub = "<hub>"`), then runs `make up`.
  2. Deploy the hub bundle **first**: `make hub dev HUB=<hub>` on dev. Elsewhere, run `databricks bundle deploy` in
     `hub/` with `--var subscription_hub=<hub> --var hub_principal=<the SP's application ID>`.
  3. Run the app unchanged. The e2e check "This app's hub" shows `<hub>`, and the heartbeat check reads that hub's.

  Rules with `tier: "deferred"` still run on `edp_shared`'s deferred hub and arrive on the same topic.

`make registry` writes both settings to `apps/<app>.properties`. That file is gitignored, holds no secrets, and is
rewritten on every run (the `e2e.*` URLs change when the workspace is rebuilt). Any setting can also be passed as
`key=value`. `consumer.group` is optional and defaults to the profile name. The `e2e.*` lines are only for `--e2e`.

## Run

```bash
# from the repo root
mvn -q -f examples/subscribers/java package                           # generate the client, build, run the unit tests
java -jar examples/subscribers/java/target/hl7-subscriber.jar         # declare MySubscriptions, then print notifications
java -jar examples/subscribers/java/target/hl7-subscriber.jar --e2e   # 26 live checks: a PASS/FAIL/SKIP table, exit 1 on any failure
```

With more than one file in `apps/`, name the app: `java -jar … examples/subscribers/apps/<app>.properties`.
`--duration=300` (seconds) or `--duration=PT5M` bounds a run. The dynamic patient monitor is
[Python only](../python/README.md). The walkthrough with expected output is in the
[README](../../../README.md#subscriptions-demo).

### Run as yourself

By default the app must sign in as a service principal (exit 5 otherwise). `identity=user` runs it as you, explicitly,
with your own CLI profile (`databricks.profile=<yours>`, from `databricks auth login`; it isn't pinned to OAuth M2M) or
the ambient identity. The platform team onboards you as an app with your email as its principal.

```bash
java -jar examples/subscribers/java/target/hl7-subscriber.jar identity=user databricks.profile=<yours> api.app=<the API's app>
```

`--e2e` stays a service principal's: as a person, its first check fails and the run stops. To run as yourself from a
Databricks notebook, use the [Python example's notebook](../python/README.md#from-a-databricks-notebook).

## Code

Classes by role (`src/main/java/com/databricks/hl7/subscriber/`). Read the first two first:

| Class | What it does |
|---|---|
| `MySubscriptions` | What this app receives: name → SQL. The file you edit: stop the app, edit, rebuild, run. |
| `App` | The entry point. Settings, sign-in as the SP (or you, with `identity=user`), then `api.getConnection()`, `api.declare(...)`, `api.getStatus(wait)` until READY, then listen. Exit codes 2–6. |
| `api/Api` | Wires the generated `SubscriptionsApi`: the URL, the SP's token on every call, and reading a refusal (`Refusal`: Databricks' error per rule) |
| `notifications/Listener` | The delivery contract on the generated `Notification`: dedupe by `notificationId`, current revisions only, catch-up summary, commit after handling |
| `platform/EventHubs` | The Unity Catalog-issued Entra token, the Kafka OAUTHBEARER callback, the consumer settings |
| `e2e/E2E` | The 26 live checks (`--e2e`), the same as the Python example's `e2e.py` |

In the workspace, the same checks run as a serverless job, as the app's service principal:
`DATABRICKS_BUNDLE_ROOT=examples/subscribers/java databricks bundle deploy`, then `... bundle run e2e` (from the repo
root). Binding the job to the service principal needs the deployer to have **Service Principal: User** on it.
