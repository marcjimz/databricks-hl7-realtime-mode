# Your own subscription hub

Every app's rules run on a subscription hub: the pipeline that matches silver records to the app's rules and writes
the matches to the app's own topic. There are two ways to get one, with the same rules and the same API:

| | `edp_shared` (the default) | Your own hub (this bundle) |
|---|---|---|
| Who deploys it | the platform (`make demo dev`, main bundle) | your team, before your apps register rules |
| Runs as | the platform's deployer | your team's service principal |
| Serves | every app not assigned elsewhere, plus **every** app's `deferred` rules | only your apps' `realtime` rules |
| Event Hubs | `<catalog>_eventhubs` | `hub_<name>_eventhubs`: reads silver, writes only your apps' topics |
| Registry | the owner | its own Lakebase role: reads only your apps' rules, writes only its own rows |
| Cost | shared by everyone on it | one always-on serverless pipeline (~$3.80/h estimated), whatever the apps or rules |

Both deploy the same pipeline definition, `hub/resources/subscription_hub.yml`. The main bundle includes it with
`subscription_hub=edp_shared`; this bundle deploys it under your hub's name. One pipeline with a flow per silver
source carried 50 apps and 2,000 rules at delivery p99 2.9 s (docs/SCALABILITY.md).

Your hub's service principal reads every column of silver. The hub enforces each app's column grants in its own code,
so running a hub means your team is trusted with silver.

## Set it up

1. **The platform onboards the hub** (in `infra/azure/terraform.tfvars`). It names the hub and its service principal,
   and assigns apps to it:

   ```hcl
   subscriber_hubs = {
     "lab_team" = { application_id = "<the hub's service principal>" }   # optional: max_enabled_rules (default 2000)
   }
   subscriber_apps = {
     "lab-alerts" = { application_id = "...", templates = ["..."], subscription_hub = "lab_team" }
   }
   ```

   Then `make up`, which shows the Terraform plan and applies it once you confirm. It runs `make uc` and
   `make registry` too, which set up everything the hub needs:
   - an access connector with Data Receiver on the five silver hubs and Data Sender on each assigned app's hub;
   - credential `hub_lab_team_eventhubs`, with ACCESS for the service principal only;
   - registry role `hub_lab_team`, with the service principal as its member;
   - schema `<catalog>.hub_lab_team`, the pipeline's target (it creates no tables there);
   - use of the registry's Lakebase project.

2. **Your team deploys the hub.** The deployer needs the **Service Principal: User** role on the hub's service
   principal, because the pipeline runs as it.

   ```bash
   make hub dev HUB=lab_team        # from the repo root: deploys this bundle and starts the pipeline
   make hub-stop dev HUB=lab_team   # stops it (stops its billing)
   ```

   Without the Makefile:

   ```bash
   cd hub && databricks bundle deploy -t dev --var subscription_hub=lab_team --var hub_principal=<application ID> \
     --var catalog=<catalog> --var eh_namespace=<namespace>
   ```

3. **Your apps run unchanged.** `GET /api/v1/connection` names the hub, and status reports its heartbeat.

## Notes

- **Deploy each hub from one identity.** Bundle state lives under the deployer's home folder. Two people deploying the
  same hub would create two pipelines, and the second fails while the first runs. Deploying from CI as the hub's
  service principal avoids this.
- **The code isn't in `/Workspace/Shared`.** It runs with the service principal's access, so it stays where only the
  deployer can change it, and the service principal can only read it.
- **Deferred rules always run on `edp_shared`**, whatever an app's hub is, and still reach the app's own topic.
- **For the tightest tail under churn,** deploy one pipeline per source by setting `hl7.subscriptions.sources`. That
  measured p99 2.8 s against 6.5 s for one pipeline, at about 3.7× the cost.
- **Moving an app to another hub** (change `subscription_hub`, then `make up`) lets the new hub start from the live
  edge. First wait until the old hub has sent the app's pending notifications (held while its topic was down).
