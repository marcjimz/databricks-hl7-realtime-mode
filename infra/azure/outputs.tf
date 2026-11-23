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

output "schema" {
  description = "Schema of the hot-path tables and topics (bundle variable schema)."
  value       = var.schema
}

output "subscriber_apps" {
  description = "Per subscriber app: SP application ID, its hub, its connector (+ identity), and its grants. Read by make uc / registry."
  value = {
    for k, v in var.subscriber_apps : k => {
      application_id = v.application_id
      # its grant: its own sources and columns plus every template's
      sources                = distinct(concat(v.sources, flatten([for t in v.templates : var.subscriber_templates[t].sources])))
      fields                 = distinct(concat(v.fields, flatten([for t in v.templates : var.subscriber_templates[t].fields])))
      templates              = { for t in v.templates : t => var.subscriber_templates[t] }
      hub                    = azurerm_eventhub.subscriber[k].name
      connector_id           = azurerm_databricks_access_connector.subscriber[k].id
      connector_principal_id = azurerm_databricks_access_connector.subscriber[k].identity[0].principal_id
      subscription_hub       = v.subscription_hub # the subscription hub that runs its realtime rules
    }
  }
}

output "subscriber_hubs" {
  description = "Per team subscription hub: its SP (which deploys and runs hub/), rule cap, connector (+ identity), and apps. Read by make uc / registry / hub."
  value = {
    for k, v in var.subscriber_hubs : k => {
      application_id         = v.application_id
      max_enabled_rules      = v.max_enabled_rules
      connector_id           = azurerm_databricks_access_connector.hub[k].id
      connector_principal_id = azurerm_databricks_access_connector.hub[k].identity[0].principal_id
      apps                   = sort([for app, a in var.subscriber_apps : app if a.subscription_hub == k])
    }
  }
}
