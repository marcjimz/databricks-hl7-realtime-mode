# Azure for the HL7 Real-Time Mode demo, in one region: Databricks workspace, ADLS Gen2 for the Unity Catalog
# catalog, an access connector (managed identity), and Event Hubs Premium with one hub per layer topic.
#
# No keys or connection strings: pipelines and generators reach Event Hubs through a Unity Catalog service credential
# backed by the access connector (`make uc` creates it), authorized by the Azure RBAC roles below.

locals {
  tags = merge(var.tags, { owner = var.owner })
}

resource "azurerm_resource_group" "this" {
  name     = "${var.prefix}-rg"
  location = var.location
  tags     = local.tags
}

# One identity for both storage (UC storage credential) and Event Hubs (UC service credential).
resource "azurerm_databricks_access_connector" "this" {
  name                = "${var.prefix}-connector"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}
