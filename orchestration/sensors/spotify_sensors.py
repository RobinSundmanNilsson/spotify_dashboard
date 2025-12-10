import dagster as dg
from orchestration.jobs import job_dbt

sensors: list[dg.SensorDefinition] = []

# Bara skapa sensor om dbt-jobbet finns
if job_dbt is not None:
    @dg.asset_sensor(
        asset_key=dg.AssetKey("load_spotify_to_duckdb"),
        job=job_dbt,
        minimum_interval_seconds=60,  # polla var minut
    )
    def trigger_dbt_after_ingest(
        context: dg.SensorEvaluationContext,
        asset_event: dg.EventLogEntry,
    ):
        """Triggar dbt-jobbet när load_spotify_to_duckdb är klar."""
        yield dg.RunRequest(run_key=context.cursor)

    sensors = [trigger_dbt_after_ingest]
