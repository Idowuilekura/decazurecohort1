terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "3.114.0"
    }
  }
}

provider "azurerm" {
  # Configuration options
  features {}

  skip_provider_registration = true
}

provider "local" {
  # Configuration options
}