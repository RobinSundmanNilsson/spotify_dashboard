# orchestration/assets/spotify_assets.py
from pathlib import Path
import sys
import dagster as dg

# Lägg till projektroten i PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from data_extract_load.load_spotify_data import run_pipeline

# Paths
DUCKDB_PATH = PROJECT_ROOT / "data_warehouse" / "spotify.duckdb"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_spotify_duckdb"

@dg.asset
def load_spotify_to_duckdb(context: dg.AssetExecutionContext):
    """
    Kör DLT-pipelinen som laddar Spotify-data till DuckDB.
    """
    context.log.info(f"Kör Spotify ETL, duckdb_path={DUCKDB_PATH}")

    queries = [
        "",
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
    ]
    years = list(range(2015, 2026))
    min_popularity = 30

    try:
        run_pipeline(
            queries=queries,
            years=years,
            duckdb_path=DUCKDB_PATH,
            market="SE",
            limit=50,
            min_popularity=min_popularity,
        )
    except Exception as exc:
        context.log.error(f"ETL misslyckades: {exc}")
        raise

    context.log.info("ETL-körning klar.")
    return {"duckdb_path": str(DUCKDB_PATH), "status": "success"}


# dbt-integration
try:
    from dagster_dbt import DbtCliResource, dbt_assets

    dbt_resource = DbtCliResource(project_dir=str(DBT_PROJECT_DIR))

    @dbt_assets(
        manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    )
    def dbt_spotify_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        """Kör dbt build (run + test)"""
        yield from dbt.cli(["build"], context=context).stream()

except Exception as e:
    dg.get_dagster_logger().warning(f"DBT-integration inaktiverad: {e}")
    dbt_spotify_models = None
    dbt_resource = None