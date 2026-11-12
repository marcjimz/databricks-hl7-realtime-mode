# Example subscriber (Python)

The same app as the [Java example](../java/README.md), run from the repo root with the repo's `uv` environment and a
Databricks CLI profile. It isn't part of the platform: any language can do the same through the contract
([docs/integration.md](../../../docs/integration.md)). It subscribes to gold records at runtime with Databricks SQL,
then consumes its own Event Hub, signing in only as its Databricks service principal.

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
credential for Event Hubs, and the token scope. `make registry` writes both settings to
`examples/subscribers/apps/<app>.properties`, shared with the Java example. You also need `uv sync` (it installs
confluent-kafka and openapi-python-client).

## Run (from the repo root)

```bash
uv run python examples/subscribers/python/subscriber.py --e2e          # the 25 end-to-end checks (~2 min)
uv run python examples/subscribers/python/subscriber.py                # declare my_subscriptions.py, then listen
uv run python examples/subscribers/python/subscriber.py --duration=40  # listen for 40 s, then exit 0
```

- **Edit `my_subscriptions.py`**, then run again. No build: the API compiles the SQL, and the hub applies it at its next
  micro-batch.
- **Exit codes and output match the Java example:** 2 refused, 3 the hub can't run a rule, 4 not confirmed or the hub
  stopped, 5 not a service principal, 6 not onboarded.
- **Same service principal, same rules.** An app declares its service principal's whole set. This example starts with
  the same rule as the Java one, so either can run as `sp-hl7-realtime-demo` and they agree (`(unchanged)`). If you
  change one, the other's next run puts its own set back.
- **Its own consumer group** (`<profile>-python` by default; `consumer.group=...` to change it), so it can run beside the
  Java example and each gets every notification.
- With more than one file in `examples/subscribers/apps/`, name the app:
  `uv run python examples/subscribers/python/subscriber.py examples/subscribers/apps/<app>.properties`.

| File | Role (the Java example's class) |
|---|---|
| `subscriber.py` | Entry point: settings, sign-in, connection, declare, wait, listen (`App`) |
| `my_subscriptions.py` | Name → Databricks SQL: the file you edit (`MySubscriptions`) |
| `api.py` | Wires the generated client: URL, the SP's token per call, refusals (`api/Api`) |
| `notifications.py` | The delivery contract: dedupe, current revisions, catch-up (`notifications/Listener`) |
| `eventhubs.py` | Unity Catalog token, OAUTHBEARER, Event Hubs client settings (`platform/EventHubs`) |
| `e2e.py` | The 25 live checks (`e2e/E2E`) |
| `build.py` | Generates the client from the contract (Maven's openapi-generator step) |

Unit tests: `tests/test_python_subscriber.py` (run by `make test`). Among them, the generated client runs the whole
handshake against the real Subscription API in-process.
