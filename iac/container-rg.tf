resource "azurerm_container_registry" "acr" {
  name                = "spotifyprojectcr${random_integer.number.result}"
  location            = var.location
  resource_group_name = azurerm_resource_group.storage_rg.name
  sku                 = "Basic"
  admin_enabled       = true
}

resource "null_resource" "build_and_push_pipeline" {
  triggers = {
    image_tag = local.pipeline_image_tag
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
        -f ../dockerfile.dwh \
        --no-cache \
        -t ${azurerm_container_registry.acr.name}.azurecr.io/spotifyproject-pipeline:${local.pipeline_image_tag} \
        -t ${azurerm_container_registry.acr.name}.azurecr.io/spotifyproject-pipeline:latest \
        ../ --push
    EOT
  }

  depends_on = [azurerm_container_registry.acr]
}

resource "azurerm_container_group" "acg" {
  name                = "${var.prefix_app_name}-continst${random_integer.number.result}"
  location            = azurerm_resource_group.storage_rg.location
  resource_group_name = azurerm_resource_group.storage_rg.name
  ip_address_type     = "Public"
  dns_name_label      = "${var.prefix_app_name}-continst${random_integer.number.result}"
  os_type             = "Linux"

  depends_on = [
    null_resource.build_and_push_pipeline,
    azurerm_storage_share.upload_dbt
  ]

  image_registry_credential {
    server   = "${azurerm_container_registry.acr.name}.azurecr.io"
    username = azurerm_container_registry.acr.admin_username
    password = azurerm_container_registry.acr.admin_password
  }

  container {
    name   = "spotifyproject-pipeline"
    # Always pull the latest pipeline image
    image  = "${azurerm_container_registry.acr.name}.azurecr.io/spotifyproject-pipeline:latest"
    cpu    = "1"
    memory = "4"

    ports {
      port     = 80
      protocol = "TCP"
    }

    ports {
      port     = 3000
      protocol = "TCP"
    }

    environment_variables = {
      DBT_PROFILES_DIR = "/mnt/data/.dbt"
      DUCKDB_PATH      = "/mnt/data/spotify.duckdb"
    }

    secure_environment_variables = {
      SPOTIPY_CLIENT_ID     = var.spotipy_client_id
      SPOTIPY_CLIENT_SECRET = var.spotipy_client_secret
    }

    volume {
      name       = "mnt"
      mount_path = "/mnt/data"
      read_only  = false
      share_name = azurerm_storage_share.upload_dbt.name

      storage_account_name = azurerm_storage_account.storage_account.name
      storage_account_key  = azurerm_storage_account.storage_account.primary_access_key
    }
  }
}
