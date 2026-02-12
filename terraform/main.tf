terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "location" {
  type    = string
  default = "eastus"
}

variable "prefix" {
  type    = string
  default = "consent"
}

variable "tags" {
  type = map(string)
  default = {
    project = "consent-pipeline"
    owner   = "arbreton"
  }
}

# Globally-unique names: Event Hubs namespaces and storage accounts live in
# one global namespace, so a second person running this gets a collision
# without the suffix.
resource "random_string" "suffix" {
  length  = 6
  upper   = false
  lower   = true
  numeric = true
  special = false
}

locals {
  name = "${var.prefix}${random_string.suffix.result}"
}

resource "azurerm_resource_group" "rg" {
  name     = "${local.name}-rg"
  location = var.location
  tags     = var.tags
}

# --- Ingestion: Event Hubs (Kafka-protocol entry point) ---
resource "azurerm_eventhub_namespace" "ns" {
  name                = "${local.name}-ehns"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
  tags                = var.tags
}

resource "azurerm_eventhub" "events" {
  name                = "consent-events"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 8
  # 3-day retention: replay is our recovery story, 1 day is thin for it.
  message_retention = 3
}

# Separate consumer group for the Databricks job (never share $Default).
resource "azurerm_eventhub_consumer_group" "databricks" {
  name                = "databricks-cg"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  eventhub_name       = azurerm_eventhub.events.name
  resource_group_name = azurerm_resource_group.rg.name
}

# --- Storage: Data Lake Gen2, one container per medallion layer ---
resource "azurerm_storage_account" "lake" {
  name                     = "${replace(local.name, "-", "")}lake"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

resource "azurerm_storage_container" "bronze" {
  name                 = "bronze"
  storage_account_name = azurerm_storage_account.lake.name
}

resource "azurerm_storage_container" "silver" {
  name                 = "silver"
  storage_account_name = azurerm_storage_account.lake.name
}

resource "azurerm_storage_container" "quarantine" {
  name                 = "quarantine"
  storage_account_name = azurerm_storage_account.lake.name
}

resource "azurerm_storage_container" "gold" {
  name                 = "gold"
  storage_account_name = azurerm_storage_account.lake.name
}

# --- Compute: Databricks workspace + lake access ---
resource "azurerm_databricks_workspace" "ws" {
  name                        = "${local.name}-dbw"
  location                    = azurerm_resource_group.rg.location
  resource_group_name         = azurerm_resource_group.rg.name
  sku                         = "standard"
  managed_resource_group_name = "${local.name}-dbw-mrg"
  tags                        = var.tags
}

# Access Connector lets Databricks reach the lake via managed identity —
# no storage keys in notebooks. The pipeline reads the Event Hubs
# connection string from a Databricks secret scope instead of env.
resource "azurerm_databricks_access_connector" "lake" {
  name                = "${local.name}-dbw-connector"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  identity {
    type = "SystemAssigned"
  }
  tags = var.tags
}

resource "azurerm_role_assignment" "lake_reader" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.lake.identity[0].principal_id
}

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.ns.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.ws.workspace_url
}

output "datalake_dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}
