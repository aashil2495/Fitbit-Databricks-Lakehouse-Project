# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.7.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.56.0"
    }
  }

  required_version = ">= 1.0"
}


provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id = "d066615d-e286-4bf1-92e7-7df087f1f4d0"
}


provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.main.id
}

# Create a Resource Group
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

# Create an Azure Databricks Workspace
resource "azurerm_databricks_workspace" "main" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "premium"
}

# Create an ADLS Gen2 Storage Account
resource "azurerm_storage_account" "adls_gen2" {
  name                     = var.adls_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # Enables ADLS Gen2

  # Disable Soft Delete for Blob Properties
  blob_properties {
    delete_retention_policy {
      days = 1  # Setting days to 0 disables soft delete
    }
  }
}

# Create the metastore-root Storage Container
resource "azurerm_storage_container" "metastore-root" {
  name                  = "metastore-root"
  storage_account_name  = azurerm_storage_account.adls_gen2.name
  container_access_type = "private"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "rawdatazone" {
  name               = "example"
  storage_account_id = azurerm_storage_account.adls_gen2.id
}


resource "azurerm_databricks_access_connector" "main" {
  name                = var.databricks_access_connector_name
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }
}

# Assign the "Storage Blob Data Contributor" role to the Access Connector on the ADLS Gen2 account
resource "azurerm_role_assignment" "blob_contributor" {
  scope                = azurerm_storage_account.adls_gen2.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}

# Create a Metastore
/*resource "databricks_metastore" "main" {
  name                 = var.metastore_name
  storage_root         = "abfss://metastore-root@${azurerm_storage_account.adls_gen2.name}.dfs.core.windows.net/meta"
  region               = "centralus"
  force_destroy        = true
}

# Attach the Metastore to the Databricks Workspace
resource "databricks_metastore_assignment" "main" {
  metastore_id         = databricks_metastore.main.id
  workspace_id         = azurerm_databricks_workspace.main.workspace_id
}/*


# Output information
output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.main.id
}

output "adls_gen2_account_name" {
  value = azurerm_storage_account.adls_gen2.name
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.main.id
}

/*output "metastore_id" {
    value = databricks_metastore.main.id
}*/
