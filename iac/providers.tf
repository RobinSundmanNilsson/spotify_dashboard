terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>4.3"
    }
  }
  required_version = "~> 1.13.0"
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id != null && var.subscription_id != "" ? var.subscription_id : local.subscription_id_from_file
}
