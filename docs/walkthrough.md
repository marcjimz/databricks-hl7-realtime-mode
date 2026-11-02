# Demo walkthrough (~20 minutes)

## Before the demo (once, and again after the Sunday sandbox wipe)
```bash
make install
cp infra/azure/terraform.tfvars.example infra/azure/terraform.tfvars   # once: subscription, tenant, metastore, prefix, catalog
make up              # Terraform plan -> confirm -> apply (workspace, ADLS, Event Hubs), then Unity Catalog (make uc)
```

## 1. Start everything (10 minutes before you present)
```bash
make demo dev        # deploy, start every pipeline, start the HL7 feed, print the dashboard URL
```
Serverless Real-Time Mode pipelines are running within ~2 minutes; the dashboard fills in once data flows. Then
`make clear-data dev` so start-up catch-up doesn't skew the numbers.

## 2. Show the flow
1. **The source:** raw HL7 v2 (ADT, ORU, ORM, MDM) arrives at 200 msg/s in topic `<catalog>.<schema>.raw_hl7`.
2. **The hot path** (Real-Time Mode, one Event Hubs topic between each layer): bronze → silver (HL7 parsed with
   plain Spark into the Epic silver contract) → gold (encounters, observations, orders).
3. **The lakehouse path:** every topic also lands in the streaming table of the same name. Open `silver_hl7` in
   Catalog Explorer: same columns as the existing Epic silver table.

## 3. Show the numbers (dashboard)
- **p99 per hop:** every bar is well under 200 ms.
- **p99 by minute:** flat lines, with a short blip every 5 minutes when each pipeline checkpoints.
- **Critical labs:** HH / LL results in gold, with their raw → gold latency.
- **Quarantine:** malformed messages are kept with a reason, not dropped.

## 4. Show the security story
- Catalog Explorer → External data → Credentials → `<catalog>_eventhubs`: a Unity Catalog service credential backed by
  the workspace's access connector (managed identity).
- The pipeline settings have no secrets. Kafka options are just the bootstrap server and
  `databricks.serviceCredential`. The Event Hubs namespace has SAS keys disabled, and access is Azure RBAC
  (Event Hubs Data Sender / Receiver) on the managed identity.

## 5. Stop
```bash
make stop dev        # stops every pipeline and the feed; Event Hubs keeps billing until the sandbox is wiped or destroyed
```
