import dagster as dg
from orchestration.jobs import job_dbt

sensors: list[dg.SensorDefinition] = []

# Only create sensor if the dbt job exists
if job_dbt is not None:
    @dg.asset_sensor(
        asset_key=dg.AssetKey("load_spotify_to_duckdb"),
        job=job_dbt,
        minimum_interval_seconds=60,  # poll every minute
    )
    def trigger_dbt_after_ingest(
        context: dg.SensorEvaluationContext,
        asset_event: dg.EventLogEntry,
    ):
        """Trigger the dbt job when load_spotify_to_duckdb finishes."""
        yield dg.RunRequest(run_key=context.cursor)

    sensors = [trigger_dbt_after_ingest]
