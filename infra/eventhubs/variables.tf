variable "subscription_id" {
  description = "Azure subscription that hosts the Event Hubs namespace."
  type        = string
}

variable "prefix" {
  description = "Name prefix for the resource group and namespace (namespace names are globally unique)."
  type        = string
  default     = "hl7rtm"
}

variable "location" {
  description = "Azure region. Pick the region closest to the Databricks workspace (AWS us-east-1 -> eastus)."
  type        = string
  default     = "eastus"
}

variable "sku" {
  description = "Event Hubs tier. Premium (isolated compute) is required for <200 ms per hop at p99; see docs/latency.md."
  type        = string
  default     = "Premium"

  validation {
    condition     = contains(["Standard", "Premium"], var.sku)
    error_message = "sku must be Standard or Premium."
  }
}

variable "processing_units" {
  description = "Premium-tier processing units (ignored for Standard)."
  type        = number
  default     = 1
}

variable "throughput_units" {
  description = "Standard-tier throughput units (ignored for Premium) (1 TU = 1 MB/s or 1000 events/s ingress)."
  type        = number
  default     = 2
}

variable "partition_count" {
  description = "Partitions per hub. RTM needs one task slot per partition per stage, so keep this modest."
  type        = number
  default     = 4
}

variable "topics" {
  description = "One hub per layer topic, plus the existing Epic JSON feed. (Standard tier: max 10 hubs.)"
  type        = list(string)
  default = [
    "epic.json",
    "hl7.raw",
    "hl7.bronze",
    "hl7.silver",
    "hl7.quarantine",
    "hl7.gold.encounters",
    "hl7.gold.observations",
    "hl7.gold.orders",
    "hl7.sub.critical_lab",
    "hl7.sub.ed_arrival_surge",
  ]
}

variable "owner" {
  description = "Owner email, applied as the `owner` tag (required by sandbox subscription policy)."
  type        = string
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default = {
    project = "hl7-realtime-mode"
  }
}
