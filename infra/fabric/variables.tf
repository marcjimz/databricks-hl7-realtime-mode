variable "subscription_id" {
  description = "Azure subscription in the Power BI / Fabric tenant to create the capacity in."
  type        = string
}

variable "tenant_id" {
  description = "Entra tenant of Power BI / Fabric (where the dashboard's workspace lives)."
  type        = string
}

variable "admins" {
  description = "Capacity administrators (user principal names). They set the page-refresh minimums."
  type        = list(string)
}

variable "location" {
  description = "Region of the capacity. Same region as the Databricks SQL warehouse keeps DirectQuery local."
  type        = string
  default     = "eastus2"
}

variable "name" {
  description = "Capacity name (lowercase letters and digits, 3-63) and resource group prefix."
  type        = string
  default     = "hl7rtmfabric"
}

variable "sku" {
  description = "Fabric capacity size (F2 is the smallest; billed per hour while running, nothing while paused)."
  type        = string
  default     = "F2"
}

variable "owner" {
  description = "Owner tag (sandbox policy)."
  type        = string
}

variable "tags" {
  description = "Extra tags."
  type        = map(string)
  default     = {}
}
