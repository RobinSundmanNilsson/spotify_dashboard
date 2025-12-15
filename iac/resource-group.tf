resource "azurerm_resource_group" "storage_rg" {
  name     = "${var.prefix_app_name}-rg-${random_integer.number.result}"
  location = var.location
}