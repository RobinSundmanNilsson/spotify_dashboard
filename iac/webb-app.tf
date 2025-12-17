resource "null_resource" "build_and_push_dashboard" {
  triggers = {
    image_tag = local.dashboard_image_tag
  }

  provisioner "local-exec" {
    command = <<EOT
      export DOCKER_CONFIG="$(mktemp -d)"
      trap 'rm -rf "$DOCKER_CONFIG"' EXIT
      # When DOCKER_CONFIG is overridden, Docker can't see Desktop's CLI plugins (e.g. buildx).
      # Copy them so `docker buildx ...` continues to work.
      if [ -d "$HOME/.docker/cli-plugins" ]; then
        mkdir -p "$DOCKER_CONFIG/cli-plugins"
        cp -R "$HOME/.docker/cli-plugins/." "$DOCKER_CONFIG/cli-plugins/" || true
      fi
      az acr login --name ${azurerm_container_registry.acr.name}
      docker buildx build --platform linux/amd64 \
        -f ../dockerfile.dashboard \
        --no-cache \
        -t ${azurerm_container_registry.acr.name}.azurecr.io/spotifyproject-dashboard:${local.dashboard_image_tag} \
        -t ${azurerm_container_registry.acr.name}.azurecr.io/spotifyproject-dashboard:latest \
        ../ --push
    EOT
    interpreter = var.is_windows ? ["bash.exe", "-c"] : ["/bin/sh", "-c"]
  }

  depends_on = [azurerm_container_registry.acr]
}

resource "azurerm_service_plan" "asp" {
  name                = "${var.prefix_app_name}-asp"
  location            = azurerm_resource_group.storage_rg.location
  resource_group_name = azurerm_resource_group.storage_rg.name
  os_type             = "Linux"
  sku_name            = "P0v3"

}

resource "azurerm_linux_web_app" "app" {
  name                = "${var.prefix_app_name}-app-${random_integer.number.result}"
  location            = azurerm_resource_group.storage_rg.location
  resource_group_name = azurerm_resource_group.storage_rg.name
  service_plan_id     = azurerm_service_plan.asp.id

  site_config {
    application_stack {
      # Web App prepends docker_registry_url, so docker_image_name should be repo:tag only.
      # Use :latest so the app always pulls the most recent build.
      docker_image_name   = "spotifyproject-dashboard:latest"
      docker_registry_url = "https://${azurerm_container_registry.acr.login_server}"
      docker_registry_username = azurerm_container_registry.acr.admin_username
      docker_registry_password = azurerm_container_registry.acr.admin_password
    }
  }

  storage_account {
    name         = "duckdbdata"
    type         = "AzureFiles"
    account_name = azurerm_storage_account.storage_account.name
    share_name   = azurerm_storage_share.upload_dbt.name
    access_key   = azurerm_storage_account.storage_account.primary_access_key
    mount_path   = "/mnt/data"
  }

  app_settings = {
    WEBSITES_PORT                     = "8501"
    WEBSITES_ENABLE_APP_SERVICE_STORAGE = "true"
    DBT_PROFILES_DIR                  = "/mnt/data/.dbt"
    DUCKDB_PATH                       = "/mnt/data/spotify.duckdb"
  }

  depends_on = [
    null_resource.build_and_push_dashboard,
    azurerm_container_group.acg,
  azurerm_storage_share.upload_dbt]
}