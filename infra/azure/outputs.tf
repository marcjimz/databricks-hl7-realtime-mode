# Read by `make uc` and the bundle target (none are secrets).
output "workspace_url" {
  description = "Databricks workspace URL (bundle target host)."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "workspace_id" {
  description = "Databricks workspace ID (recorded on the catalog; a new ID means the environment was rebuilt)."
  value       = azurerm_databricks_workspace.this.workspace_id
}

output "access_connector_id" {
  description = "Access connector behind the UC storage and service credentials."
  value       = azurerm_databricks_access_connector.this.id
}

output "access_connector_principal_id" {
  description = "The connector's managed identity. It changes when the connector is recreated, even though its ID doesn't."
  value       = azurerm_databricks_access_connector.this.identity[0].principal_id
}

output "storage_url" {
  description = "ADLS Gen2 URL for the UC external location."
  value       = "abfss://${azurerm_storage_container.uc.name}@${azurerm_storage_account.uc.name}.dfs.core.windows.net/"
}

output "eh_namespace" {
  description = "Event Hubs namespace name (bundle variable eh_namespace)."
  value       = azurerm_eventhub_namespace.this.name
}

output "catalog" {
  description = "Unity Catalog catalog (bundle variable catalog)."
  value       = var.catalog
}

output "subscriber_apps" {
  description = "Per subscriber app: SP application ID, its hub, its connector (+ identity), and its grants. Read by make uc / registry."
  value = {
    for k, v in var.subscriber_apps : k => {
      application_id         = v.application_id
      sources                = v.sources
      fields                 = v.fields
      hub                    = azurerm_eventhub.subscriber[k].name
      connector_id           = azurerm_databricks_access_connector.subscriber[k].id
      connector_principal_id = azurerm_databricks_access_connector.subscriber[k].identity[0].principal_id
    }
  }
}
