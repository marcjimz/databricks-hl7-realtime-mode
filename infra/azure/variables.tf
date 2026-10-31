variable "subscription_id" {
  description = "Azure subscription to deploy into."
  type        = string
}

variable "tenant_id" {
  description = "Entra tenant that owns the subscription and the Databricks account."
  type        = string
}

variable "databricks_account_id" {
  description = "Azure Databricks account ID (accounts.azuredatabricks.net)."
  type        = string
}

variable "metastore_id" {
  description = "Existing Unity Catalog metastore in var.location to assign the workspace to (e.g. fevm-shared-sandbox-westus2)."
  type        = string
}

variable "location" {
  description = "One region for the workspace, Event Hubs, and storage (same region keeps every hop local)."
  type        = string
  default     = "westus2"
}

variable "prefix" {
  description = "Name prefix (lowercase letters and digits). Event Hubs and storage names are globally unique, so pick your own."
  type        = string
  default     = "hl7rtm"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{2,15}$", var.prefix))
    error_message = "prefix must be 3-16 lowercase letters/digits, starting with a letter."
  }
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

# Event Hubs
variable "processing_units" {
  description = "Event Hubs Premium processing units. 1 PU ran at ~83% CPU under the demo load; 4 PU halves the namespace-wide stalls seen at 2 PU. See docs/latency.md."
  type        = number
  default     = 4
}

variable "partition_count" {
  description = "Partitions per hub. RTM needs one task slot per partition per stage, so keep this modest."
  type        = number
  default     = 4
}

variable "catalog" {
  description = "Unity Catalog catalog the topics land in (the bundle's `catalog`). Hubs are named <catalog>.<schema>.<table>."
  type        = string
}

variable "schema" {
  description = "Unity Catalog schema the topics land in (the bundle's `schema` variable)."
  type        = string
  default     = "hl7_realtime"
}

variable "tables" {
  description = "One hub per Delta table the hot path lands in, plus the Epic JSON feed."
  type        = list(string)
  default = [
    "epic_near_realtime_json_from_eventhub",
    "raw_hl7",
    "bronze_hl7",
    "silver_hl7",
    "quarantine_hl7",
    "gold_encounters",
    "gold_observations",
    "gold_orders",
  ]
}
