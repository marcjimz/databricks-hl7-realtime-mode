output "capacity_name" {
  value = azurerm_fabric_capacity.this.name
}

output "capacity_id" {
  description = "ARM ID (suspend / resume)."
  value       = azurerm_fabric_capacity.this.id
}

output "tenant_id" {
  value = var.tenant_id
}
