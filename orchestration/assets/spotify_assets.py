# orchestration/assets/spotify_assets.py
from pathlib import Path
import os
import sys
import dagster as dg

# Add project root to PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from data_extract_load.load_spotify_data import run_pipeline, run_artist_enrichment

# Paths
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "data_warehouse" / "spotify.duckdb"
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH)))
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_spotify_duckdb"
DBT_PROFILES_DIR = Path(os.getenv("DBT_PROFILES_DIR", str(Path.home() / ".dbt")))

@dg.asset
def load_spotify_to_duckdb(context: dg.AssetExecutionContext):
    """
    Run the DLT pipeline that loads Spotify data into DuckDB.
    """
    context.log.info(f"Running Spotify ETL, duckdb_path={DUCKDB_PATH}")

    queries = [
        "",  # broad catch-all
        "genre:pop",
        "genre:rock",
        "genre:hip-hop",
        "genre:electronic",
        "genre:indie",
        "genre:metal",
        "genre:jazz",
        "genre:r&b",
        "genre:house",
        "genre:techno",
        "genre:latin",
        "genre:afrobeat",
        "genre:classical",
        "genre:punk",
        "genre:k-pop",
        "genre:lofi",
        "genre:trap",
        "genre:drum and bass",
        "genre:reggae",
        "genre:country",
        "genre:ambient",
        "genre:soul",
        "genre:disco",
        "genre:funk",
        "genre:blues",
        "genre:folk",
        "genre:deep house",
        "genre:tech house",
        "genre:progressive house",
        "genre:trance",
        "genre:swedish pop",
        "genre:electro pop",
        "genre:edm",
    ]
    years = list(range(2015, 2026))
    min_popularity = 0  # include everything; dbt dedup handles duplicates
    markets = ["SE"]  # Swedish market only

    try:
        for m in markets:
            context.log.info(f"Running Spotify ETL, duckdb_path={DUCKDB_PATH}, market={m}")
            run_pipeline(
                queries=queries,
                years=years,
                duckdb_path=DUCKDB_PATH,
                market=m,
                limit=50,
                min_popularity=min_popularity,
            )
            context.log.info(f"Finished market={m}")
        run_artist_enrichment(duckdb_path=DUCKDB_PATH, max_artists=None)
    except Exception as exc:
        context.log.error(f"ETL failed: {exc}")
        raise

    context.log.info("ETL run complete.")
    return {"duckdb_path": str(DUCKDB_PATH), "status": "success"}


# dbt-integration
try:
    from dagster_dbt import DbtCliResource, dbt_assets

    dbt_resource = DbtCliResource(project_dir=str(DBT_PROJECT_DIR), profiles_dir=str(DBT_PROFILES_DIR))

    @dbt_assets(
        manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    )
    def dbt_spotify_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        """Run dbt build (run + test)."""
        yield from dbt.cli(["build"], context=context).stream()

except Exception as e:
    dg.get_dagster_logger().warning(f"DBT integration disabled: {e}")
    dbt_spotify_models = None
    dbt_resource = None
