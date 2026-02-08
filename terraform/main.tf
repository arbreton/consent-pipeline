terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
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

resource "azurerm_resource_group" "rg" {
  name     = "${var.prefix}-pipeline-rg"
  location = var.location
}

# --- Ingestion: Event Hubs (Kafka-protocol entry point) ---
resource "azurerm_eventhub_namespace" "ns" {
  name                = "${var.prefix}-ehns"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "events" {
  name                = "consent-events"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 8
  message_retention   = 1
}

# --- Storage: Data Lake Gen2, one container per medallion layer ---
resource "azurerm_storage_account" "lake" {
  name                     = "${var.prefix}datalake01"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
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

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.ns.name
}

output "datalake_dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}
