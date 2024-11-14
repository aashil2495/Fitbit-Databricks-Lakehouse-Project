# Define variables for reusability
variable "resource_group_name" {
  default = "terraform_Databricks_Fitbit"
}

variable "location" {
  default = "Central US"
}

variable "databricks_workspace_name" {
  default = "terraform_Fitbit_dev"
}

variable "adls_account_name" {
  default = "terraformfitbitadlsg2dev"
}

variable "databricks_access_connector_name" {
  default = "terraform_fitbit_databricks-access-connector"
}

variable "metastore_name" {
  default = "metastore_azure_eastus"
}

variable "metastore_container_name" {
  default = "metastore-root"
}

variable "managed_container_name"{
    default="managed"
}

variable "unmanaged_container_name"{
    default="unmanaged"
}