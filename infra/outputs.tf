output "namespace" {
  description = "Event Hubs namespace name (bundle variable eh_namespace)."
  value       = azurerm_eventhub_namespace.this.name
}

output "bootstrap_servers" {
  description = "Kafka bootstrap endpoint."
  value       = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
}

output "connection_string" {
  description = "Namespace SAS connection string. Stored in the Databricks secret scope by `make secrets`; never commit it."
  value       = azurerm_eventhub_namespace_authorization_rule.databricks.primary_connection_string
  sensitive   = true
}
