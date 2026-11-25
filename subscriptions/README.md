# The subscription layer on its own, in a workspace that already has its data

This folder deploys only the subscription layer:

- the Lakebase registry;
- the Subscription API (a Databricks App);
- the `edp_shared` subscription hub.

It sits over a streaming table you already have, such as an Epic JSON `epic_realtime` table. Nothing from the rest of
this repo is deployed: no Real-Time Mode pipelines, no generators, no Terraform.

Everything runs inside the workspace, from a Git folder of this repo: notebooks, plus the bundle deployed from the Git
folder's Deployments panel. Your environment's values (host, catalog, tables, credentials, people) live in one
gitignored file, so they never reach git.

```
your streaming table  <catalog>.<schema>.epic_realtime
        │  read as a stream (appends only)
        ▼
edp_shared hub ──── each app's rules ──── Lakebase registry ◄── Subscription API (App) ◄── apps register SQL rules
(serverless pipeline, micro-batch)                                    ▲
        │  a match = one notification                                 │ GET /connection, PUT /subscriptions, GET ?wait
        ▼                                                             │
each app's own Event Hub (topic) ──────────────► the app (a notebook, the Python or Java example)
```

An app writes rules in Databricks SQL against its own view of the table, for example:

```sql
SELECT patient_id, msg_type FROM epic_realtime WHERE msg_type LIKE 'ADT^A04%'
```

The API compiles the rule with Databricks and stores it. The hub applies it at its next micro-batch, with no redeploy.
Each match lands on the app's own topic, carrying only the fields its rule selects. The contract (API, notification,
errors) is the same as everywhere else in this repo: [docs/integration.md](../docs/integration.md).

## What gets deployed, and what it costs

| Resource | What it is | Cost while running |
|---|---|---|
| `hl7rt-<target>-hub-edp_shared` | the hub: a continuous serverless pipeline (PREVIEW channel), one flow per source table | about $3.80/h, flat in apps and rules |
| `hl7rt-<target>-hub-edp_shared-deferred` | the same hub for rules on the `deferred` tier (a longer trigger) | about $3.80/h |
| the Subscription API App (`subscription_api_app`) | FastAPI, its own service principal; 1 instance is enough for a test | App compute, per instance |
| a Lakebase project (`registry_project`) | the registry: rules, status, the hub's state | small; it suspends after 5 idle minutes |
| `<catalog>.sub_<app>` schemas, `<catalog>.<hub_schema>` | each app's views over the table (its granted columns); the hub pipeline's target (no tables) | none |

Stop the two pipelines and the App when you're not testing (Step 7). Rules and status stay in the registry.

## Requirements (check these first)

The preflight notebook (Step 1) checks most of them for you.

**Workspace**

- [ ] Serverless compute for notebooks, jobs, and pipelines.
- [ ] Lakebase Autoscaling. A feature available in a region isn't always on for every workspace there; the preflight
  tries it.
- [ ] Databricks Apps.
- [ ] A SQL warehouse (serverless is fine). The API compiles each rule on it with `DESCRIBE QUERY`.
- [ ] PyPI reachable from serverless (or a mirror). The App, the hub, and the notebooks install their Python packages
  from it.

**You (phase 1: you deploy, own the registry, run the hub, and are the first subscriber)**

- [ ] SELECT on the source table, and USE on its catalog and schema.
- [ ] CREATE SCHEMA on the catalog where the subscription objects go (`catalog`), or ask for the schemas
  `sub_<app>` and `<hub_schema>` to be created for you.
- [ ] Permission to create a Lakebase project and a Databricks App.
- [ ] CAN MANAGE on the SQL warehouse. Adding a warehouse to an App needs it; the App itself gets CAN USE.
- [ ] ACCESS on the two UC service credentials below: the hub's (to send) and your app's (to read).

**Event Hubs (your Azure admin)**

Notifications go to topics in your existing Event Hubs namespace. Nothing uses keys or connection strings: access is
Entra ID through Unity Catalog service credentials (Kafka OAUTHBEARER on port 9093).

- [ ] One Event Hub (topic) per subscriber app, e.g. `<catalog>.subscribers.<app>`. One day of retention is enough to
  start.
- [ ] An access connector whose managed identity has **Azure Event Hubs Data Sender** on the app topics. This is the
  hub's. Make it a UC service credential (`eh_service_credential`), with ACCESS for whoever runs the hub (you, in
  phase 1).
- [ ] For each app, an access connector with **Azure Event Hubs Data Receiver** on that app's topic only. Make it a UC
  service credential with ACCESS for that app's principal (you, in phase 1).
  For a first test, one credential with both roles on one topic is fine. Split them before more than one app
  subscribes: an app's credential is what keeps it from reading another app's topic.
- [ ] Network: serverless reaches `<namespace>.servicebus.windows.net:9093`.

## Step 1. Preflight (read-only)

1. In the workspace, create a Git folder from this repository: Workspace → Create → Git folder. Use this branch.
2. Open `subscriptions/preflight` on serverless.
3. Set `source_table`, for example `<catalog>.<schema>.epic_realtime`. Optionally set the Event Hubs namespace and a
   service credential, to test the network path and your ACCESS.
4. Run all.

It prints PASS, WARN, or FAIL per check, then a summary table and a JSON block. It prints no row values: only schema,
metadata, counts, rates, null percentages, and (optionally) the count per `msg_type` code.

The **verdict** at the bottom suggests the three values the hub needs from the table (Step 2):

| Value | What the hub uses it for | Typical |
|---|---|---|
| `subscription_table_offset` | a stable 64-bit ID per row. Notification IDs derive from it, so a retried batch sends the same IDs and apps can drop repeats | `_metadata.row_id` when the table has row tracking; else an integer ID column |
| `subscription_table_partition` | an integer that, with the offset, identifies a row | `0` |
| `subscription_table_ts` | the row's time, sent as each notification's `source_ts` | `_ingested_at` |

## Step 2. Configure: one target file

Copy `subscriptions/target.example.yml` to `subscriptions/targets/<env>.yml`. In a Git folder: right-click →
Duplicate, then move it. `targets/` is gitignored, so this file stays in your Git folder and is never committed.

Fill it in from the preflight's output. Every value is commented in the example. The ones you always set:

| Variable | What |
|---|---|
| `workspace.host` | this workspace's URL |
| `catalog` | where the subscription objects go: the `sub_<app>` view schemas and the hub's schema |
| `subscription_tables` | the source table(s), comma-separated FQNs. A rule names a table by its last part: `FROM epic_realtime` |
| `subscription_table_offset`, `_partition`, `_ts` | from the preflight's verdict |
| `eh_namespace` | the Event Hubs namespace name (without `.servicebus.windows.net`) |
| `eh_service_credential` | the hub's UC service credential (Sender on the app topics) |
| `registry_project`, `subscription_api_app` | the Lakebase project and the App's name |
| `warehouse` | the SQL warehouse's name |
| `subscriber_apps` | who may subscribe, to what (below) |

Each subscriber app is an entry in `subscriber_apps`:

```yaml
subscriber_apps:
  notebook-test:
    principal: you@example.com           # phase 1: your email. Later: the app's service principal (application ID)
    topic: <catalog>.subscribers.notebook-test   # its Event Hub
    eventhubs_credential: <credential>   # Receiver on that topic; ACCESS for the principal
    sources: [epic_realtime]
    fields: [patient_id, msg_type, patient_point_of_care]   # the columns its rules may select or filter on; ["*"] = all
```

An app can filter and select only its `fields`: filtering on a column is reading it. The API refuses anything else
with Databricks' own error (`UNRESOLVED_COLUMN`).

## Step 3. Set up the registry (before the first deploy)

Open `subscriptions/setup`, set `target` to `<env>`, and run **1. Your target** and **2. Before you deploy the
bundle**. This:

- creates the Lakebase project and the registry schema;
- seeds `edp_shared`;
- onboards each app: its registry role, its row, and its views `<catalog>.sub_<app>.<table>` over the granted columns;
- creates the hub's schema.

It's idempotent: run it again whenever you change the target file.

## Step 4. Deploy the bundle (from the Git folder)

1. Open `subscriptions/databricks.yml`.
2. **Deployments** panel → target `<env>` → **Deploy**. This creates the App and the two hub pipelines; the pipelines
   start on their own.
3. Start the App: in the Deployments panel, run `subscription_api`, or open the App and click Deploy. Wait until it's
   Running.

## Step 5. Grant the App (after the deploy)

In `subscriptions/setup`, run **4. After you deploy the bundle and start the App**. It grants the App's service
principal its registry role and SELECT on the apps' views, and gives each app's principal CAN USE on the App. Then run
**Status**: the App should be RUNNING, both pipelines RUNNING, and both hubs beating (heartbeats a few seconds old).

## Step 6. Subscribe from a notebook, as yourself

1. Open `examples/subscribers/python/notebook` on serverless, with environment version 2 or later (4 recommended): its
   API client needs Python 3.11+.
2. Set `api_app` (the App's name). Edit `RULES` if you like: one rule per name, written against your view.
3. Run all. It:
   - installs its packages;
   - declares your rules (the API compiles each one, or prints Databricks' error);
   - waits until the hub runs them (READY);
   - prints each notification as it arrives, for `seconds`.
4. The last cell cancels your rules.

Try a rule in the SQL editor first, against your own view (`SELECT ... FROM <catalog>.sub_notebook_test.epic_realtime
WHERE ...`): it returns the records the hub will deliver.

From a notebook, the example exchanges the notebook's token for one the App accepts (a Databricks App takes OAuth
tokens with its own audience). Reading the topic uses a token Unity Catalog issues from your app's service credential.

## Is it working?

- **The notebook** prints `READY`, then `notification ...` lines. A rule edit prints `ACTIVE` within seconds.
- **The hub:** both pipelines RUNNING, with no WARN or ERROR in their event logs.
- **The registry:** `setup` → **Status** shows heartbeats per hub and tier.
- **Latency:** delivery time is the table's commit delay plus the hub's micro-batch. On our dev, the hub alone measured
  p99 about 2.5 s. Each notification's `source_ts` (the row's `subscription_table_ts`) against the topic's broker
  timestamp is the end-to-end figure.

## Moving to service principals

Phase 1 runs everything as you. To give an app its own identity:

| Principal | Needs | Doesn't need |
|---|---|---|
| each app's service principal | CAN USE on the API App (setup grants it); ACCESS on **its own** Receiver credential (your admin); its own topic | Lakebase or the tables: the API is the only way in |
| the API App's service principal | created with the App; setup grants it the registry and the views | the source table: the views carry the owner's rights |
| the hub (today: the deployer) | SELECT on the source table; ACCESS on the Sender credential; the registry | — |

Change the app's `principal` to the service principal's application ID, rerun **2.** and **4.** in `setup`, and run
the app as that service principal. The Python and Java examples (`examples/subscribers/`) sign in with its OAuth M2M
profile. Its rules, topic, and status carry over. Revoke your own grants by hand.

## Step 7. Stop it

`subscriptions/setup` → **Stop everything** stops both pipelines and the App. Deploying again, or starting the
pipelines and the App, resumes. The hub resumes from its checkpoint, so it never re-sends what it already delivered.

## Troubleshooting

| You see | Means | Do |
|---|---|---|
| `APP_NOT_ONBOARDED` (403) | your principal isn't in `subscriber_apps`, or setup hasn't run since | add it, rerun setup 2 and 4 |
| `HUB_STALE` | the hub isn't beating: a pipeline stopped or failed | start it; read its event log |
| `SQL_NOT_ALLOWED` | the rule names a table that isn't in `subscription_tables`, or isn't one simple SELECT | fix the rule |
| `UNRESOLVED_COLUMN` | the column isn't in your granted `fields` | grant it, or don't use it |
| the notebook gets 401 or 403 from the App | you lack CAN USE on it, or `api_app` names another App | rerun setup 4; set `api_app` |
| the topic read fails with authorization errors | no ACCESS on your credential, or its identity lacks Receiver on your topic | your admin's step |
| the hub fails reading the table | no SELECT for the hub's identity, or the offset expression doesn't resolve | rerun the preflight; check `subscription_table_offset` |

Changed rows (MERGE, UPDATE) aren't re-notified: the hub reads the table's appends (`skipChangeCommits`). A new
checkpoint starts at the table's latest version, so a first start doesn't replay its history.
