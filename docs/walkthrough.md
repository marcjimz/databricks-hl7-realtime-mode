# Demo walkthrough (~20 minutes)

## Before the demo (once)
```bash
make install
cp infra/eventhubs/terraform.tfvars.example infra/eventhubs/terraform.tfvars   # subscription, owner, catalog
make infra-plan && make infra-apply      # Event Hubs Premium + one hub per table
make secrets dev                         # connection string -> secret scope
```

## 1. Start everything (10 minutes before you present)
```bash
make demo dev        # deploy, start every pipeline, start the HL7 feed, print the dashboard URL
```
Classic Real-Time Mode clusters take ~9 minutes to start. The dashboard fills in once they're running.

## 2. Show the flow
1. **The source:** raw HL7 v2 (ADT, ORU, ORM, MDM) arrives at 200 msg/s in topic `<catalog>.<schema>.raw_hl7`.
2. **The hot path** (Real-Time Mode, one Event Hubs topic between each layer): bronze → silver (HL7 parsed with
   plain Spark into the Epic silver contract) → gold (encounters, observations, orders) → subscriptions.
3. **The lakehouse path:** every topic also lands in the Delta table of the same name. Open `silver_hl7` in Catalog
   Explorer: same columns as the existing Epic silver table.

## 3. Show the numbers (dashboard)
- **p99 per hop:** every bar is well under 200 ms (typically 30–80 ms).
- **p99 by minute:** flat lines, with a short blip every 5 minutes when each pipeline checkpoints.
- **Alerts table:** an ED registration followed by a critical lab for the same patient within 60 minutes, delivered
  ~30–80 ms after the lab reached gold.
- **Quarantine:** malformed messages are kept with a reason, not dropped.

## 4. Self-service: add a subscription live
```bash
make sub-new NAME=hyperkalemia MODE=microbatch
```
Edit `src/subscriptions/hyperkalemia.yml`:
```yaml
when:
  source: gold_observations
  filter: "observation_cd = '2823-3' AND value_num > 6.0"
output: [patient_id, visit_id, patient_facility, value_num, abnormal_flag]
```
```bash
make infra-plan && make infra-apply     # topic sub_hyperkalemia
make deploy dev                          # table + topic sub_hyperkalemia fill within ~2 minutes
make mirror-restart dev                  # the dashboard picks it up
```
Use `MODE=rtm` for millisecond delivery (classic cluster), or `MODE=microbatch` for seconds on serverless.

## 5. Stop
```bash
make stop dev        # stops every pipeline and the feed; Event Hubs keeps running (Terraform owns it)
```
