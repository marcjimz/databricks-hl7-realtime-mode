resource "azurerm_databricks_workspace" "this" {
  name                = "${var.prefix}-dbw"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "premium" # Unity Catalog
  tags                = local.tags
  # No fixed managed_resource_group_name: the sandbox wipe can leave the old managed RG behind, and a fixed name would
  # then block the rebuild. Azure generates a unique one per workspace.
}

# Attach the workspace to the account's existing Unity Catalog metastore in this region.
resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  metastore_id = var.metastore_id
}
