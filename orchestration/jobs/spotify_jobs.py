import dagster as dg
from orchestration.assets import load_spotify_to_duckdb, dbt_spotify_models

# Ingestion job
job_ingest = dg.define_asset_job(
    "ingest_spotify",
    selection=dg.AssetSelection.assets(load_spotify_to_duckdb),
)

jobs = [job_ingest]

# Job for dbt (if dbt assets are available)
if dbt_spotify_models is not None:
    job_dbt = dg.define_asset_job(
        "run_dbt",
        selection=dg.AssetSelection.assets(dbt_spotify_models),
    )
    jobs.append(job_dbt)
else:
    job_dbt = None
