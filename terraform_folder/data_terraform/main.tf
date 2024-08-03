data "azurerm_storage_account" "example" {
  name                = "decstg2"
  resource_group_name = "decgrp"
}

output "storage_account_tier" {
  value = data.azurerm_storage_account.example.account_tier
}

resource "local_file" "file" {
  content =  "${data.azurerm_storage_account.example.account_tier}"
  filename = "account_tier.txt"
}