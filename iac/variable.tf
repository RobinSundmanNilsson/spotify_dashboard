variable "location" {
  description = "The location where resources will be deployed."
  default     = "swedencentral"
}

variable "prefix_app_name" {
  description = "app name before each resource name"
  default     = "spotifyproject"
}

variable "subscription_id" {
  type        = string
  description = "Azure subscription ID used by the provider. Leave unset to read the value from env_variable.sh."
  default     = null
}

variable "spotipy_client_id" {
  type        = string
  description = "Spotify Client ID for Spotipy."
}

variable "spotipy_client_secret" {
  type        = string
  description = "Spotify Client Secret for Spotipy."
}

variable "is_windows" {
  description = "Set to true if running on Windows"
  type        = bool
  default     = false
}
