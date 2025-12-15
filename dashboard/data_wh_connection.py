import os
from pathlib import Path

import duckdb

# Prefer env var for containers; default to mount path used in Azure
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", "/mnt/data/spotify.duckdb"))


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection to the warehouse."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def query_table(table_name: str):
    """Return the full table as a DataFrame."""
    with get_connection(read_only=True) as conn:
        return conn.execute(f"SELECT * FROM {table_name}").df()
