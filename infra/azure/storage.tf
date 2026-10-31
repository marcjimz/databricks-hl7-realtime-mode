# ADLS Gen2 for the catalog's managed storage (the shared metastore has no root). The name is deterministic, so a
# rebuild after the weekly wipe recreates the same URL and the Unity Catalog external location stays valid.
resource "azurerm_storage_account" "uc" {
  name                            = "${var.prefix}uc"
  resource_group_name             = azurerm_resource_group.this.name
  location                        = azurerm_resource_group.this.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  tags                            = local.tags
}

resource "azurerm_storage_container" "uc" {
  name               = "unity-catalog"
  storage_account_id = azurerm_storage_account.uc.id
}

resource "azurerm_role_assignment" "connector_storage" {
  scope                = azurerm_storage_account.uc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}
