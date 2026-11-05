# Integrating an application

Applications meet the platform at two points, in any language:
1. **The Subscription API** (HTTPS and JSON): declare rules in Databricks SQL and wait until they run.
2. **The app's own topic** (the Kafka protocol on Azure Event Hubs): receive the matches.

**The contract:** [`api/openapi.json`](api/openapi.json), generated from the API (`tests/test_openapi.py` keeps them
equal), with Swagger UI at `<api>/docs`. Generate a client from it in any language.

## What an app needs

- **A Databricks service principal** (client ID and OAuth secret). It's the app's identity, and the secret is the app's own.
- **Onboarding by the platform team:** the sources and columns it may read (docs/subscriptions.md, "Set up"). This
  produces three settings, with no secrets in them: `api.url`, `eventhubs.namespace`, and `eventhubs.credential`.
- **Network access:** HTTPS to the workspace and the API, and port 9093 to `<namespace>.servicebus.windows.net`.

## The handshake

`$WS` is the workspace URL and `$API` is `api.url`.

```bash
# 1. The service principal's token (OAuth M2M; any Databricks SDK can do this)
TOKEN=$(curl -s -u "$CLIENT_ID:$CLIENT_SECRET" "$WS/oidc/v1/token" \
  -d grant_type=client_credentials -d scope=all-apis | jq -r .access_token)

# 2. Declare the whole set. Atomic: all accepted, or nothing applied. Undeclared rules are cancelled.
curl -s -X PUT "$API/api/v1/subscriptions" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"subscriptions": [{"name": "critical-potassium", "sql": "SELECT patient_id, value_num FROM gold_observations WHERE value_num >= 6.5"}]}'
#   200 → {"applied": true, "accepted": [{"subscriptionId", "revision", "query", "destination", "unchanged"}], "cancelled": [...]}
#   422 → {"applied": false, "refused": [{"name", "code", "message"}]}   (code: Databricks' error class, or ours)

# 3. Wait, in short calls, until the hub runs exactly these revisions
curl -s "$API/api/v1/subscriptions?wait=25" -H "Authorization: Bearer $TOKEN"
#   state: READY (start reading) | PENDING (poll again) | FAILED (see problems) | HUB_STALE (the hub is stopped)

# 4. An Event Hubs token for the app's topic, issued by Unity Catalog to this service principal only
curl -s -X POST "$WS/api/2.1/unity-catalog/temporary-service-credentials" -H "Authorization: Bearer $TOKEN" \
  -d '{"credential_name": "<eventhubs.credential>", "azure_options": {"resources": ["https://<eventhubs.namespace>.servicebus.windows.net/.default"]}}'
#   → {"azure_aad": {"aad_token"}, "expiration_time"}   (refresh before it expires)
```

Test a rule in the SQL editor first, against `<catalog>.sub_<app>.<source>`: it returns the records the app will
receive. `POST /api/v1/validate` runs every check without applying anything.

## Reading the topic

Use any Kafka client on `destination`. Settings:
- `bootstrap.servers`: `<namespace>.servicebus.windows.net:9093`
- `security.protocol`: `SASL_SSL`, with `sasl.mechanism` `OAUTHBEARER` (the token is `aad_token`)
- **one stable `group.id` per app**, so it resumes where it stopped
- `enable.auto.commit`: `false`; commit after handling
- `metadata.max.age.ms` and `connections.max.idle.ms`: `180000`

Each message is keyed by patient ID. The value is JSON:

```json
{"notificationId": "4b0e…", "subscriptionId": "6f1c…", "subscription": "critical-potassium", "revision": 1,
 "source": "gold_observations", "sourcePartition": 3, "sourceOffset": 1048211, "gold_ts": "2026-11-05T05:02:15.775Z",
 "payload": {"patient_id": "MRN021417", "value_num": 6.7}}
```

- **Delivery is at least once:** deduplicate on `notificationId`.
- **Act only on the `(subscriptionId, revision)` pairs your last declare returned.** Anything else is backlog from an
  earlier revision.
- **`payload`** holds exactly the columns the rule selects, nulls included.

## Errors

| HTTP | `code` |
|---|---|
| 401 | `UNAUTHENTICATED`: no valid token |
| 403 | `APP_NOT_ONBOARDED`: the service principal isn't a subscriber app |
| 422 | Databricks' error class (`UNRESOLVED_COLUMN…`, `PARSE_SYNTAX_ERROR`, …), `SQL_NOT_ALLOWED` (not one filter over one record), `INVALID_NAME`, `DUPLICATE_NAME`, `RULE_LIMIT_REACHED` (1,000 rules). Nothing applied. |
| 404 | `NOT_FOUND_OR_INACCESSIBLE`: `DELETE` of a name with no active subscription |

## The Java reference client

`src/subscriber` maps one class to each step:
- `MySubscriptions`: the rules;
- `App`: steps 1–3;
- `api/SubscriptionApi`: HTTP;
- `platform/EventHubs`: step 4 and the Kafka settings;
- `notifications/Listener`: dedupe and current revisions;
- `e2e/E2E`: 24 live checks.
