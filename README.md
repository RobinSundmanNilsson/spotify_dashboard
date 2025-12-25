# Spotify Data Platform (Azure)

Modern end‑to‑end pipeline for Spotify analytics: extract with Spotipy + DLT, orchestrate with Dagster, transform with dbt into DuckDB, and serve insights with a Streamlit dashboard. Everything is containerized and provisioned to Azure via Terraform.

## Project overview
This repository is a full-stack data platform demo focused on reproducibility and clarity. It shows how to go from a public API to curated analytics and a live dashboard using modern data tooling, all packaged in containers and deployed with infrastructure as code. The result is a single, repeatable workflow that runs locally or in Azure.

## Data flow
1) Ingest Spotify data with Spotipy + DLT into a shared DuckDB file.
2) Transform and model tables with dbt inside the same DuckDB file.
3) Serve the curated tables through a Streamlit dashboard.
4) Dagster schedules and coordinates the ingest and dbt runs.

## Azure architecture
- **ACR** stores the pipeline and dashboard images built on apply.
- **ACI** runs the Dagster pipeline container.
- **Azure Files** hosts the DuckDB file and dbt profiles and is mounted into both services.
- **Azure Web App** runs the Streamlit dashboard container.

## What’s inside
- **Extraction**: Spotipy + DLT fetch tracks/artists, cached into DuckDB.
- **Orchestration**: Dagster (container) runs jobs and exposes the Dagster UI.
- **Transform**: dbt models (DuckDB target) materialize curated tables.
- **Serve**: Streamlit dashboard (container) reads `/mnt/data/spotify.duckdb`.
- **Infra as Code**: Terraform builds ACR, Azure Files share, ACI for Dagster, and Azure Web App for the dashboard. Docker images are built and pushed as part of apply.

## Prerequisites
- Docker Desktop (buildx enabled) and logged in locally.
- Python 3.11+ (for Terraform wrappers and any local tooling).
- Azure CLI (`az login` already done).
- Terraform CLI.
- Spotify API creds available as environment variables:
  - `SPOTIPY_CLIENT_ID`, `SPOTIPY_CLIENT_SECRET`, `SPOTIPY_REDIRECT_URI`

## Local quickstart
1) Create a `.env` in repo root (KEY=VALUE):
```
SPOTIPY_CLIENT_ID=...
SPOTIPY_CLIENT_SECRET=...
SPOTIPY_REDIRECT_URI=https://localhost:8501
```
2) (Optional) Run locally with Docker Compose:
```
docker-compose up --build
```
Dagster UI will be on localhost:3000, Streamlit on localhost:8501, DuckDB lives under `./mnt/data/spotify.duckdb`.

## Deploy to Azure with Terraform
From `iac/`:
1) Create `iac/terraform.tfvars` (gitignored) with the required values:
```
spotipy_client_id     = "..."
spotipy_client_secret = "..."
subscription_id       = "..."           # recommended to avoid the wrong subscription
location              = "swedencentral" # change region if needed; changing it replaces resources
prefix_app_name       = "spotifyproject" # optional
is_windows            = false           # set true on Windows to use bash.exe for local-exec
```
2) Initialize and apply:
```
terraform init
terraform plan
terraform apply
```

If you prefer to keep secrets in `.env`, you can still export them and use TF_VAR:
```
set -a
source ../.env   # exports SPOTIPY_* locally
set +a
TF_VAR_spotipy_client_id="$SPOTIPY_CLIENT_ID" \
TF_VAR_spotipy_client_secret="$SPOTIPY_CLIENT_SECRET" \
terraform apply
```

What apply does:
- Builds/pushes two images to ACR (`spotifyprojectcr<rand>.azurecr.io`): `spotifyproject-pipeline` and `spotifyproject-dashboard`.
- Provisions Azure File share and mounts it to `/mnt/data` in both containers.
- Spins up Dagster in ACI and Streamlit in Azure Web App pointing at the pushed images.

Outputs to note after apply:
- `dagster_url` – Dagster UI (run/monitor jobs)
- `dashboard_url` – Streamlit app
- `pipeline_container_group_name` – ACI name for troubleshooting

## Dagster usage
- Open `dagster_url` and enable the two automations (ingest_spotify_schedule & trigger_dbt_after_ingest) defined for the project.
- Manually materialize `load_spotify_to_duckdb` once.
- After that run, the remaining assets should materialize automatically.

## Project layout
- `dockerfile.dwh` – Dagster/DLT/dbt image
- `dockerfile.dashboard` – Streamlit image
- `data_extract_load/` – Spotify ingestion logic (lazy Spotipy init)
- `dbt_spotify_duckdb/` – dbt models
- `orchestration/` – Dagster definitions and assets
- `dashboard/` – Streamlit app and data connector
- `iac/` – Terraform definitions

## Operating notes
- DuckDB path is fixed to `/mnt/data/spotify.duckdb` (backed by Azure Files).
- Spotipy credentials are injected via Terraform `TF_VAR_spotipy_*`; they never live in code or images.
- If you change code, re‑apply Terraform (images rebuild thanks to triggers on source dirs).
- Ensure Docker Desktop is running before `terraform apply` (buildx step).
