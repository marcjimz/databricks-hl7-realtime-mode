resource "azurerm_databricks_workspace" "this" {
  name                = "${var.prefix}-dbw"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "premium" # Unity Catalog
  tags                = local.tags
  # Azure's default managed RG name (databricks-rg-<resource group>) ignores the region. A deleted workspace's managed
  # RG can outlive it, and a rebuild in another region then fails ("Resource group already exists in location ...").
  # Naming it per region keeps a region move from colliding with a leftover.
  managed_resource_group_name = "databricks-rg-${var.prefix}-${var.location}"
}

# Attach the workspace to the account's existing Unity Catalog metastore in this region.
resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  metastore_id = var.metastore_id
}
