terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.40"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.130"
    }
  }
  # Local state. After the weekly sandbox wipe, `make infra-plan` sees the resources gone and recreates them.
}

# Pinned to the sandbox tenant: the az CLI can otherwise hand out tokens for the user's home tenant.
provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id     = var.subscription_id
  tenant_id           = var.tenant_id
  storage_use_azuread = true # the storage account has shared keys disabled
}

# Account level only (metastore assignment). Unity Catalog objects are created by `make uc` (Databricks CLI), not
# Terraform: they live in the shared metastore and outlive the workspace, so Terraform state for them would go stale.
provider "databricks" {
  alias           = "account"
  host            = "https://accounts.azuredatabricks.net"
  account_id      = var.databricks_account_id
  azure_tenant_id = var.tenant_id
  auth_type       = "azure-cli"
}
