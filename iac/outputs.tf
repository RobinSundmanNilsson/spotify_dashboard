output "dagster_url" {
  value       = "http://${azurerm_container_group.acg.fqdn}:3000"
  description = "Public Dagster UI URL (Azure Container Instances)."
}

output "pipeline_resource_group" {
  value       = azurerm_resource_group.storage_rg.name
  description = "Resource group containing the pipeline container group."
}

output "pipeline_container_group_name" {
  value       = azurerm_container_group.acg.name
  description = "Azure Container Instances container group name for the pipeline."
}

output "dashboard_url" {
  value       = "https://${azurerm_linux_web_app.app.default_hostname}"
  description = "Public Streamlit dashboard URL (Azure Web App)."
}
