# Team subscription hubs: bring your own hub (see docs/subscriptions.md). A team deploys the platform's hub pipeline
# from hub/ (before its apps), run as its own service principal, and it serves only the apps assigned to it
# (subscriber_apps.<app>.subscription_hub). Each hub gets its own access connector (managed identity) with
#   - Data Receiver on each silver entity hub it reads (not the rest of the hot path),
#   - Data Sender on each of its apps' hubs only (not another hub's apps),
# so its UC service credential (`make uc`: hub_<name>_eventhubs, ACCESS granted to its SP) can't write anywhere else.
# edp_shared, the platform's hub, uses the main connector (eventhubs.tf).

locals {
  # The silver entity topics the hub reads (hl7rt.subscriptions.SOURCES; tests/test_infra.py keeps them equal).
  subscription_sources = ["silver_encounters", "silver_observations", "silver_orders", "silver_diagnoses", "silver_documents"]

  hub_reads  = { for p in setproduct(keys(var.subscriber_hubs), local.subscription_sources) : "${p[0]}/${p[1]}" => { hub = p[0], source = p[1] } }
  hub_writes = { for app, a in var.subscriber_apps : app => a.subscription_hub if a.subscription_hub != "edp_shared" }
}

resource "azurerm_databricks_access_connector" "hub" {
  for_each            = var.subscriber_hubs
  name                = "${var.prefix}-hub-${replace(each.key, "_", "-")}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "hub_receiver" {
  for_each             = local.hub_reads
  scope                = azurerm_eventhub.topics[each.value.source].id # one silver hub, not the namespace
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = azurerm_databricks_access_connector.hub[each.value.hub].identity[0].principal_id
}

resource "azurerm_role_assignment" "hub_sender" {
  for_each             = local.hub_writes
  scope                = azurerm_eventhub.subscriber[each.key].id # the app's hub, not the namespace
  role_definition_name = "Azure Event Hubs Data Sender"
  principal_id         = azurerm_databricks_access_connector.hub[each.value].identity[0].principal_id
}
