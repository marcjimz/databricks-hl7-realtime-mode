resource "azurerm_eventhub_namespace" "this" {
  name                         = "${var.prefix}-ehns"
  location                     = azurerm_resource_group.this.location
  resource_group_name          = azurerm_resource_group.this.name
  sku                          = "Premium"
  capacity                     = var.processing_units
  local_authentication_enabled = false # Entra ID only: no SAS keys exist to leak
  tags                         = local.tags
}

# Hub name = the Delta table it lands in: <catalog>.<schema>.<table>.
resource "azurerm_eventhub" "topics" {
  for_each          = toset(var.tables)
  name              = "${var.catalog}.${var.schema}.${each.value}"
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = var.partition_count
  message_retention = 1
}

# The access connector's identity (behind the UC service credential) produces and consumes on every hub.
resource "azurerm_role_assignment" "connector_eventhubs" {
  for_each             = toset(["Azure Event Hubs Data Sender", "Azure Event Hubs Data Receiver"])
  scope                = azurerm_eventhub_namespace.this.id
  role_definition_name = each.value
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}
