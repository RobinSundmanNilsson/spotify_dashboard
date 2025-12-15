from pathlib import Path
import os
import dagster as dg

from orchestration.assets import (
    load_spotify_to_duckdb,
    dbt_spotify_models,
    dbt_resource,
)
from orchestration.jobs import jobs, job_ingest
from orchestration.sensors import sensors

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "data_warehouse" / "spotify.duckdb"
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH)))

# Make DuckDB path available to dbt (profiles.yml)
os.environ.setdefault("DUCKDB_PATH", str(DUCKDB_PATH))

# Assets
assets = [load_spotify_to_duckdb]
if dbt_spotify_models is not None:
    assets.append(dbt_spotify_models)

# Resources
resources = {}
if dbt_resource is not None:
    resources["dbt"] = dbt_resource

# Schema (daglig ingest)
schedule_ingest = dg.ScheduleDefinition(
    job=job_ingest,
    cron_schedule="0 6 * * *",
)

defs = dg.Definitions(
    assets=assets,
    resources=resources,
    jobs=jobs,
    schedules=[schedule_ingest],
    sensors=sensors,
)
