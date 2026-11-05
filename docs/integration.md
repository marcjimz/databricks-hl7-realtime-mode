# Integrating an application

The platform is Databricks-native. Real-Time Mode pipelines parse HL7 into gold topics and tables, a hub applies each
app's SQL rules, and Unity Catalog governs every step. **An application meets it at two places only:**

1. **The Subscription API**: HTTPS and JSON, with an OpenAPI contract. The app declares what it wants in Databricks
   SQL and waits until the platform runs it.
2. **The app's own topic**: the Kafka protocol on Azure Event Hubs. Matches arrive there as JSON.

Anything that can make an HTTPS call and run a Kafka consumer can integrate, in any language. The Java app in
`src/subscriber` is one reference client. Every step below maps to one of its classes.

## What an application needs

| What | Where it comes from |
|---|---|
| A Databricks **service principal** (client ID + OAuth secret) | Created by the app's team. It's the app's identity, and the secret stays theirs. |
| **Onboarding** of that service principal: which gold sources and columns it may read | The platform team: `subscriber_apps` in Terraform, then `make up` (docs/subscriptions.md, "Set up") |
| The **API URL**, the **Event Hubs namespace**, and the app's **Unity Catalog credential** name | Written by `make registry` to `src/subscriber/apps/<app>.properties` (`api.url`, `eventhubs.namespace`, `eventhubs.credential`). No secrets. |
| Network access | HTTPS to the workspace (`*.azuredatabricks.net`) and the API (`*.databricksapps.com`), and port 9093 to `<namespace>.servicebus.windows.net` |

The app never holds an Azure key or connection string. Unity Catalog issues short-lived Event Hubs tokens to the app's
service principal.

## The contract

- **Browse it live:** `<api.url>/docs` (Swagger UI) and `<api.url>/openapi.json`. Sign in as a workspace user with
  access to the app, or send an onboarded service principal's token.
- **In the repo:** [`docs/api/openapi.json`](api/openapi.json). It's generated from the API itself, and
  `tests/test_openapi.py` fails if the two differ. Generate a client in any language from it, for example with
  `openapi-generator-cli generate -i docs/api/openapi.json -g python -o client/`.

| Call | What it does |
|---|---|
| `PUT /api/v1/subscriptions` | Declare the app's **whole set** of `{name, sql}`. Atomic: either every rule is accepted, or nothing is applied. Cancels anything no longer declared. |
| `GET /api/v1/subscriptions?wait=N` | The app's subscriptions and whether the hub runs them. With `wait`, it holds up to N seconds (max 60) for `READY` or `FAILED`. |
| `PUT /api/v1/subscriptions/{name}` | Create or change one subscription |
| `DELETE /api/v1/subscriptions/{name}` | Cancel one |
| `POST /api/v1/validate` | Every check `PUT` runs, including Databricks' compile, without applying anything |
| `GET /api/v1/health` | Liveness |

## The handshake, step by step

The examples use `curl`. `$WS` is the workspace URL, and `$API` is `api.url`.

### 1. Get the service principal's token

```bash
TOKEN=$(curl -s -u "$CLIENT_ID:$CLIENT_SECRET" "$WS/oidc/v1/token" \
  -d grant_type=client_credentials -d scope=all-apis | jq -r .access_token)
```

The token lasts about an hour. Get a new one before it expires, or let a Databricks SDK do it (Java, Python, Go, and
the CLI all support OAuth M2M profiles).

### 2. Declare the subscriptions

```bash
curl -s -X PUT "$API/api/v1/subscriptions" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{
  "subscriptions": [{"name": "critical-potassium",
    "sql": "SELECT patient_id, value_num, units FROM gold_observations WHERE observation_cd_desc = '\''Potassium'\'' AND value_num >= 6.5"}]}'
```

Databricks compiles each rule in this call, against the app's own views. Test the same SQL first in the SQL editor
against `<catalog>.sub_<app>.<source>`. It returns the records the app will receive.

**200: accepted.** Keep `subscriptionId` and `revision`, and read from `destination`:

```json
{"applied": true,
 "accepted": [{"subscriptionId": "6f1c…", "name": "critical-potassium", "revision": 1, "state": "ACCEPTED",
               "query": "SELECT patient_id, value_num, units FROM gold_observations WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5",
               "destination": "hl7_rtm_demo.subscribers.sp-hl7-realtime-demo", "unchanged": false}],
 "cancelled": []}
```

**422: refused, nothing applied.** The error is Databricks' own:

```json
{"applied": false,
 "refused": [{"name": "typo", "code": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
              "message": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `value_numm` cannot be resolved. Did you mean one of the following? [`value_num`, …]"}]}
```

### 3. Wait until the hub runs exactly these revisions

```bash
until s=$(curl -s "$API/api/v1/subscriptions?wait=25" -H "Authorization: Bearer $TOKEN" | jq -r .state) \
      && [ "$s" != PENDING ]; do :; done; echo "$s"
```

| `state` | Meaning | What the app does |
|---|---|---|
| `READY` | The hub runs every current revision (each subscription's `state` is `ACTIVE`) | Start reading |
| `PENDING` | Not yet. Usually ~5–8 s after a change. | Poll again. Keep each call ≤ 25 s, because a proxy in between may cut longer ones. |
| `FAILED` | The hub can't run a revision. `problems` carries Spark's error. | Fix the rule and declare again |
| `HUB_STALE` | The hub hasn't processed one of the app's sources for 2 minutes | Alert: the platform's hub is stopped |

### 4. Read the app's topic

**Get an Event Hubs token from Unity Catalog**, as the same service principal:

```bash
curl -s -X POST "$WS/api/2.1/unity-catalog/temporary-service-credentials" -H "Authorization: Bearer $TOKEN" -d '{
  "credential_name": "<eventhubs.credential>",
  "azure_options": {"resources": ["https://<eventhubs.namespace>.servicebus.windows.net/.default"]}}'
# -> {"azure_aad": {"aad_token": "…"}, "expiration_time": <epoch ms>}
```

Only the app's own service principal can mint with its credential. Refresh before `expiration_time`; the Java client
refreshes 5 minutes early.

**Consume `destination` with any Kafka client**, using that token for SASL OAUTHBEARER:

| Setting | Value |
|---|---|
| `bootstrap.servers` | `<eventhubs.namespace>.servicebus.windows.net:9093` |
| `security.protocol` / `sasl.mechanism` | `SASL_SSL` / `OAUTHBEARER`. The token callback returns `aad_token`. |
| `group.id` | One stable group per app, so it resumes where it stopped and never skips |
| `enable.auto.commit` | `false`: commit only after handling, so a crash replays instead of skipping |
| `metadata.max.age.ms`, `connections.max.idle.ms` | `180000` (Azure closes idle connections at 240 s; Microsoft's Kafka guidance) |
| `request.timeout.ms`, `session.timeout.ms` | `60000`, `30000` |

## What arrives: the notification

The key is the patient ID, so one patient's notifications stay in order within a partition. The value is JSON:

```json
{"notificationId": "4b0e…", "subscriptionId": "6f1c…", "subscription": "critical-potassium", "revision": 1,
 "source": "gold_observations", "sourcePartition": 3, "sourceOffset": 1048211,
 "gold_ts": "2026-11-05T05:02:15.775Z",
 "payload": {"patient_id": "MRN021417", "units": "mmol/L", "value_num": 6.7}}
```

| Field | Meaning |
|---|---|
| `notificationId` | Stable across redeliveries (subscription revision + source record). **Deduplicate on it.** |
| `subscriptionId`, `revision` | Which rule revision matched. Act only on the pairs your last declare returned; anything else is backlog from an earlier revision. |
| `subscription` | The rule's name |
| `source`, `sourcePartition`, `sourceOffset` | Where the gold record came from |
| `gold_ts` | When the gold record was produced (the broker's time) |
| `payload` | Exactly the columns the rule selects, nulls included |

**Delivery is at least once.** After a restart the app may see a notification again. Dedupe on `notificationId`.

## Errors

| HTTP | `code` | Meaning |
|---|---|---|
| 401 | `UNAUTHENTICATED` | No verified caller: the token is missing or invalid |
| 403 | `APP_NOT_ONBOARDED` | The service principal isn't an onboarded subscriber app |
| 422 | Databricks' error class (`UNRESOLVED_COLUMN…`, `PARSE_SYNTAX_ERROR`, `TABLE_OR_VIEW_NOT_FOUND`, …) | Databricks refused the SQL. Nothing applied. |
| 422 | `SQL_NOT_ALLOWED` | Not one filter over one record: a join, a subquery, a window, or a function that reaches outside the record |
| 422 | `INVALID_NAME`, `DUPLICATE_NAME` | Names are 1–63 lowercase letters, digits, or `-`, each used once |
| 422 | `RULE_LIMIT_REACHED` | The hub runs at most 1,000 rules. Nothing applied. |
| 404 | `NOT_FOUND_OR_INACCESSIBLE` | `DELETE` of a name with no active subscription |

## The reference client (Java)

`src/subscriber` implements every step above in 7 classes; see its README to build and run it.

| Step | Class |
|---|---|
| The rules (name → SQL) | `MySubscriptions` |
| 1, the SP's token | `App.signIn` (Databricks Java SDK, OAuth M2M profile) |
| 2–3, declare and wait | `api/SubscriptionApi`, called from `App` |
| 4, the Event Hubs token and consumer settings | `platform/EventHubs` |
| Dedupe, current revisions only, commit after handling | `notifications/Listener`, `notifications/Notification` |
| Checking an integration end to end | `e2e/E2E` (`--e2e`, 24 live checks) |
