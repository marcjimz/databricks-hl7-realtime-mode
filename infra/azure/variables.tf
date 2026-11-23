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
  description = "Existing Unity Catalog metastore in var.location to assign the workspace to (e.g. fevm-shared-sandbox-eastus2)."
  type        = string
}

variable "location" {
  description = "One region for the workspace, Event Hubs, and storage (same region keeps every hop local)."
  type        = string
  default     = "eastus2"
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
    "silver_messages",
    "silver_encounters",
    "silver_observations",
    "silver_orders",
    "silver_diagnoses",
    "silver_documents",
    "quarantine_hl7",
  ]
}

variable "subscriber_templates" {
  description = "Data templates: name -> the silver sources and columns one workflow needs (minimum necessary). Apps are granted templates; a subscription that names one may read, filter on, and receive only its columns."
  type = map(object({
    sources = list(string)
    fields  = list(string)
  }))
  default = {}
}

variable "subscriber_apps" {
  description = "Subscriber apps: name -> its Databricks service principal's application ID and what it may read: data templates (subscriber_templates) and/or sources and columns of its own. Each gets its own hub and a hub-scoped identity (subscribers.tf). subscription_hub picks the subscription hub that runs its realtime rules: edp_shared (the platform's, the default) or a team hub of subscriber_hubs."
  type = map(object({
    application_id   = string
    templates        = optional(list(string), [])
    sources          = optional(list(string), [])
    fields           = optional(list(string), [])
    subscription_hub = optional(string, "edp_shared")
  }))
  default = {}
  validation {
    condition     = alltrue([for k in keys(var.subscriber_apps) : can(regex("^[a-z0-9][a-z0-9-]{0,40}$", k))])
    error_message = "Subscriber app names are lowercase letters, digits, and hyphens."
  }
  validation {
    condition     = alltrue([for a in values(var.subscriber_apps) : a.subscription_hub == "edp_shared" || contains(keys(var.subscriber_hubs), a.subscription_hub)])
    error_message = "An app's subscription_hub is edp_shared or a key of subscriber_hubs."
  }
}

variable "subscriber_hubs" {
  description = "Team subscription hubs (bring your own hub): name -> the service principal that deploys and runs it (hub/, run as that SP) and its rule cap. Each gets an identity that reads the silver hubs and writes only its apps' hubs (hubs.tf). edp_shared, the platform's hub, isn't listed: it's the default."
  type = map(object({
    application_id    = string
    max_enabled_rules = optional(number, 2000)
  }))
  default = {}
  validation {
    condition     = alltrue([for k in keys(var.subscriber_hubs) : can(regex("^[a-z][a-z0-9_]{1,30}$", k)) && k != "edp_shared"])
    error_message = "Hub names are 2-31 lowercase letters, digits, and underscores, starting with a letter; edp_shared is the platform's."
  }
}
