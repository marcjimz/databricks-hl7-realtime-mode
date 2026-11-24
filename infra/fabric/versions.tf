terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.40"
    }
  }
  # Local state, like infra/azure.
}

# Pinned to the Power BI / Fabric tenant, which is not the tenant of infra/azure: a Fabric capacity serves the tenant
# of the subscription it is created in, so it must live where the Power BI workspace does.
provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}
