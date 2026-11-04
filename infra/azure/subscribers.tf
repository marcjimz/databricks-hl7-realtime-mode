# Subscriber apps (see docs/subscriptions.md). Each app brings its own Databricks service principal; this gives it:
#   - its own hub <catalog>.subscribers.<app>, where the subscription hub delivers its notifications,
#   - its own access connector (managed identity) with Data Receiver on that hub only, so its UC service credential
#     (`make uc`: sub_<app>_eventhubs, ACCESS granted to the SP) can't read gold or any other app's hub.
# The hub pipeline writes these hubs with the main connector (namespace-wide Data Sender).

resource "azurerm_eventhub" "subscriber" {
  for_each          = var.subscriber_apps
  name              = "${var.catalog}.subscribers.${each.key}"
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = var.partition_count
  message_retention = 1
}

resource "azurerm_databricks_access_connector" "subscriber" {
  for_each            = var.subscriber_apps
  name                = "${var.prefix}-sub-${each.key}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "subscriber_receiver" {
  for_each             = var.subscriber_apps
  scope                = azurerm_eventhub.subscriber[each.key].id # the app's hub, not the namespace
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = azurerm_databricks_access_connector.subscriber[each.key].identity[0].principal_id
}
