# Example subscriber (Python)

The same app as the [Java example](../java/README.md), run from the repo root with the repo's `uv` environment and a
Databricks CLI profile. It isn't part of the platform: any language can do the same through the contract
([docs/integration.md](../../../docs/integration.md)). It subscribes to silver records at runtime with Databricks SQL,
then consumes its own Event Hub, signing in as its Databricks service principal (or, explicitly, as you: see
[Run as yourself](#run-as-yourself)).

**The API client is generated from the contract.** `build.py` runs openapi-python-client on
[`docs/api/openapi.json`](../../../docs/api/openapi.json) whenever the client is missing or the contract is newer. That
produces `hl7_subscriptions_client.api` (one function per operation: `get_connection`, `declare`, `get_status`,
`put_subscription`, `cancel`, `validate`) and `.models` (one class per request, response, and the `Notification`
message) in `generated/` (gitignored). The app's code uses only those, so it can't drift from the API.
`make python-client` regenerates it on demand.

## Prerequisites

The app configures **two things**, the same as the Java example:

| # | What | Setting | Where it comes from |
|---|---|---|---|
| 1 | **A Databricks service principal** (the app's identity): host, client ID, OAuth secret | Profile `[<app>]` in `~/.databrickscfg` (`databricks.profile`) | The app's team creates it. The secret stays in the profile, never in a settings file. |
| 2 | **The Subscription API's app name** (the app looks up its URL) | `api.app` (or `api.url`) | `make registry` |

Everything else comes from the API (`get_connection`): its topic, the Kafka bootstrap servers, its Unity Catalog
credential for Event Hubs, the token scope, the hub that runs its rules (`hub`), and what it may read (`sources`,
`fields`). `make registry` writes both settings to `examples/subscribers/apps/<app>.properties`, shared with the Java
example. You also need `uv sync` (it installs confluent-kafka and openapi-python-client).

## Which hub runs your rules

The app's code and settings are the same either way. The platform assigns the hub, and `get_connection` names it.

- **The shared hub, `edp_shared` (the default).** There's nothing to deploy. The platform team onboards the app
  (`subscriber_apps` in `infra/azure/terraform.tfvars`, then `make up`), and the shared hub runs its rules.
- **Your own hub**, for a team that wants compute of its own. It's one always-on pipeline, however many apps it serves.
  1. The platform team adds the hub to Terraform (`subscriber_hubs`, with the service principal that runs it). It
     assigns the app to that hub (`subscriber_apps.<app>.subscription_hub = "<hub>"`), then runs `make up`.
  2. Deploy the hub bundle **first**: `make hub dev HUB=<hub>` on dev. Elsewhere, run `databricks bundle deploy` in
     `hub/` with `--var subscription_hub=<hub> --var hub_principal=<the SP's application ID>`.
  3. Run the app unchanged. The e2e check "This app's hub" shows `<hub>`, and the heartbeat check reads that hub's.

  Rules with `tier: "deferred"` still run on `edp_shared`'s deferred hub and arrive on the same topic.

## Run (from the repo root)

```bash
uv run python examples/subscribers/python/subscriber.py --e2e          # the 26 end-to-end checks (~2 min)
uv run python examples/subscribers/python/subscriber.py                # declare my_subscriptions.py, then listen
uv run python examples/subscribers/python/subscriber.py --duration=40  # listen for 40 s (or PT40S), then exit 0
uv run python examples/subscribers/python/monitor.py --duration=600    # dynamic patient monitoring (Python only)
```

- **Edit `my_subscriptions.py`**, then run again. No build: the API compiles the SQL, and the hub applies it at its next
  micro-batch.
- **Exit codes and output match the Java example:** 2 refused, 3 the hub can't run a rule, 4 not confirmed or the hub
  stopped, 5 not the identity the settings name (a service principal unless `identity=user`), 6 not onboarded.
- **Same service principal, same rules.** An app declares its service principal's whole set. This example starts with
  the same rule as the Java one, so either can run as `sp-hl7-realtime-demo` and they agree (`(unchanged)`). If you
  change one, the other's next run puts its own set back.
- **Its own consumer group** (`<profile>-python` by default; `consumer.group=...` to change it), so it can run beside the
  Java example and each gets every notification.
- With more than one file in `examples/subscribers/apps/`, name the app:
  `uv run python examples/subscribers/python/subscriber.py examples/subscribers/apps/<app>.properties`.

## Run as yourself

By default the app must sign in as a service principal (exit 5 otherwise). To run it as yourself, say so with
`identity=user`, and use your own Databricks CLI profile (`databricks auth login`; it isn't pinned to OAuth M2M) or none
for the ambient identity. The platform team onboards you as an app with your email as its principal.

```bash
uv run python examples/subscribers/python/subscriber.py identity=user databricks.profile=<yours> api.app=<the API's app>
```

`--e2e` stays a service principal's: as a person, its first check fails and the run stops.

## From a Databricks notebook

[`notebook.py`](notebook.py) runs the same app as you from a serverless notebook. Open it from a Git folder of this repo.
Its first cells check for Python 3.11 or later (pick Environment version 4) and install the example's packages at the
versions in `uv.lock`. It then declares its own `RULES` (`subscriber.main(argv, rules=...)`) and listens for `seconds`;
its last cell cancels them. A notebook's token can't call a Databricks App, so there the app exchanges it for one whose
audience is the API's App (`api.Exchanged`). That's why the notebook names the App (`api.app`), not only its URL. Its
first cell lists what you need.

| File | Role (the Java example's class) |
|---|---|
| `subscriber.py` | Entry point: settings, sign-in, connection, declare, wait, listen (`App`) |
| `my_subscriptions.py` | Name → Databricks SQL: the file you edit (`MySubscriptions`) |
| `api.py` | Wires the generated client: URL, the SP's token per call, refusals (`api/Api`) |
| `notifications.py` | The delivery contract: dedupe, current revisions, catch-up (`notifications/Listener`) |
| `eventhubs.py` | Unity Catalog token, OAUTHBEARER, Event Hubs client settings (`platform/EventHubs`) |
| `e2e.py` | The 26 live checks (`e2e/E2E`) |
| `build.py` | Generates the client from the contract with openapi-python-client (the Java build's openapi-generator step, for Python) |
| `monitor.py` | Dynamic patient monitoring: follows ED registrations and edits the patient rules per census change (no Java version) |
| `patient_rules.py` | The monitor's rules and census logic, with no I/O |
| `notebook.py` | Run as yourself from a Databricks notebook, with your own rules (no Java version) |

Unit tests (run by `make test`):
- `tests/test_python_subscriber.py`. Among them, the generated client runs the whole handshake against the real
  Subscription API in-process.
- `tests/test_subscriber_examples.py`. It checks that both examples run the same checks in the same order, and that the
  monitor exits from its main thread.
- `tests/test_monitor_example.py`.
- `tests/test_subscriber_notebook.py`. The notebook's token exchange, its pins against `uv.lock`, and its cells.
