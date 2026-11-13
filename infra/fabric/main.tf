# The live Power BI dashboard's own Fabric capacity: isolated, so its cost is its own and its page-refresh limits are
# ours to set. No role assignments: a capacity names its administrators by user principal name.
locals {
  tags = merge(var.tags, { owner = var.owner })
}

resource "azurerm_resource_group" "this" {
  name     = "${var.name}-rg"
  location = var.location
  tags     = local.tags
}

resource "azurerm_fabric_capacity" "this" {
  name                   = var.name
  resource_group_name    = azurerm_resource_group.this.name
  location               = azurerm_resource_group.this.location
  administration_members = var.admins
  tags                   = local.tags

  sku {
    name = var.sku
    tier = "Fabric"
  }
}
