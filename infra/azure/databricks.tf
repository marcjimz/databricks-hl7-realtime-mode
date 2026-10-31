resource "azurerm_databricks_workspace" "this" {
  name                        = "${var.prefix}-dbw"
  location                    = azurerm_resource_group.this.location
  resource_group_name         = azurerm_resource_group.this.name
  sku                         = "premium" # Unity Catalog
  managed_resource_group_name = "${var.prefix}-dbw-managed-rg"
  tags                        = local.tags
}

# Attach the workspace to the account's existing Unity Catalog metastore in this region.
resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  metastore_id = var.metastore_id
}
