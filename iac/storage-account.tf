resource "azurerm_storage_account" "storage_account" {
  name                     = "spotifyprojectsa${random_integer.number.result}"
  account_tier             = "Standard"
  location                 = var.location
  resource_group_name      = azurerm_resource_group.storage_rg.name
  account_replication_type = "LRS"
}


resource "azurerm_storage_share" "upload_dbt" {
  name               = "data"
  storage_account_id = azurerm_storage_account.storage_account.id
  quota              = 100
}

resource "azurerm_storage_share_directory" "dbt_folder" {
  name             = ".dbt"
  storage_share_id = azurerm_storage_share.upload_dbt.url
}

resource "azurerm_storage_share_file" "upload_dbt_profiles" {
  name             = "profiles.yml"
  source           = "assets/profiles.yml"
  storage_share_id = azurerm_storage_share.upload_dbt.url
  path             = ".dbt"

  depends_on = [azurerm_storage_share_directory.dbt_folder]

}
