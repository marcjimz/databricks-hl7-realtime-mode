# Azure Event Hubs for the HL7 Real-Time Mode demo.
#
# One namespace (Standard or Premium, Kafka endpoint enabled) with one hub per layer topic. A single
# namespace-scoped SAS rule (send + listen) is used by every Databricks pipeline; its
# connection string is never written to the repo. `make secrets <env>` reads the
# sensitive output below and stores it in the Databricks secret scope.

terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

locals {
  tags = merge(var.tags, { owner = var.owner })
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

resource "azurerm_resource_group" "this" {
  name     = "${var.prefix}-rg"
  location = var.location
  tags     = local.tags
}

resource "azurerm_eventhub_namespace" "this" {
  name                = "${var.prefix}-ehns"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = var.sku
  capacity            = var.sku == "Premium" ? var.processing_units : var.throughput_units
  tags                = local.tags
}

resource "azurerm_eventhub" "topics" {
  for_each          = toset(var.topics)
  name              = each.value
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = var.partition_count
  message_retention = 1
}

resource "azurerm_eventhub_namespace_authorization_rule" "databricks" {
  name                = "databricks-rtm"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = azurerm_resource_group.this.name
  listen              = true
  send                = true
  manage              = false
}
