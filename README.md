# Spotify Data Platform (Azure)

Modern end‑to‑end pipeline for Spotify analytics: extract with Spotipy + DLT, orchestrate with Dagster, transform with dbt into DuckDB, and serve insights with a Streamlit dashboard. Everything is containerized and provisioned to Azure via Terraform.

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

## Troubleshooting
- **App Service “Application Error”**: Verify `docker_image_name` in Terraform uses just `repo:tag` and `docker_registry_url` is set (already configured). A bad registry prefix causes pull failures.
- **Dagster import errors**: Confirm Spotipy env vars are set; Spotipy client now lazy‑loads, but missing envs will fail at run time.
- **Image rebuild not happening**: `terraform taint null_resource.build_and_push_* && terraform apply` forces rebuild, but normal code changes should trigger rebuild via hashes.
- **Missing .env**: `source ../.env` must contain KEY=VALUE lines only; no URLs on their own.

## What to do next
- Trigger a full materialization in Dagster; confirm dbt models are fresh.
- Explore the dashboard via `dashboard_url`; filters are powered by the DuckDB file in Azure Files.
- Add CI/CD (GitHub Actions) to run `terraform plan` and build images on PRs.
