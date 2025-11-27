import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import dlt
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


# ============================================================
#  Paths
# ============================================================

CURRENT_FILE = Path(__file__).resolve()
DLT_DIR = CURRENT_FILE.parent
PROJECT_ROOT = DLT_DIR.parent
DATA_WAREHOUSE_DIR = PROJECT_ROOT / "data_warehouse"
DUCKDB_PATH = DATA_WAREHOUSE_DIR / "spotify.duckdb"


# ============================================================
#  Spotify-klient (Client Credentials)
# ============================================================

def make_spotify_client() -> spotipy.Spotify:
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "SPOTIPY_CLIENT_ID eller SPOTIPY_CLIENT_SECRET saknas i miljön. "
            "Lägg dem i en .env i projektroten eller exportera dem innan du kör skriptet."
        )

    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    return spotipy.Spotify(auth_manager=auth_manager)


sp = make_spotify_client()


# ============================================================
#  dlt-resource: Spotify search, per år, med popularity-filter
# ============================================================

@dlt.resource(
    table_name="raw_spotify_tracks",
    write_disposition="append",
    columns={
        "preview_url": {"data_type": "text"},
    }
)
def spotify_search_tracks(
    query: str,
    year: int,
    limit: int = 50,
    market: str | None = None,
    min_popularity: int | None = None,
):
    """
    Hämtar tracks från Spotify Search API för ENT år, med pagination.

    - query:          fri text, t.ex. "" eller "genre:pop"
    - year:           t.ex. 2020
    - limit:          1-50 (Spotify max 50)
    - market:         t.ex. "SE" eller None
    - min_popularity: 0-100, t.ex. 70 = bara ganska/populära låtar
    """

    limit = max(1, min(limit, 50))

    if query:
        base_q = f"{query} year:{year}"
    else:
        base_q = f"year:{year}"

    offset = 0
    max_total = 10_000  # Spotify search hårdgräns per query

    while True:
        search_kwargs = {
            "q": base_q,
            "type": "track",
            "limit": limit,
            "offset": offset,
        }
        if market:
            search_kwargs["market"] = market

        results = sp.search(**search_kwargs)

        tracks_obj = results.get("tracks", {})
        items = tracks_obj.get("items", [])
        total = tracks_obj.get("total", 0)

        if not items:
            break

        for t in items:
            pop = t.get("popularity", 0)
            if (min_popularity is None) or (pop >= min_popularity):
                # bara yielda låtar som uppfyller popularity-kravet
                yield t

        offset += limit

        if offset >= total:
            break
        if offset >= max_total:
            break


# ============================================================
#  dlt-source: loopa över år och queries
# ============================================================

@dlt.source
def spotify_source(
    queries: list[str],
    years: list[int],
    limit: int = 50,
    market: str | None = None,
    min_popularity: int | None = None,
):
    """
    Bygger flera resources:
    - en per (år, query) kombination.
    """
    for year in years:
        for q in queries:
            safe_name = q if q else "all"
            safe_name = safe_name.replace(" ", "_").replace(":", "_")

            yield spotify_search_tracks(
                query=q,
                year=year,
                limit=limit,
                market=market,
                min_popularity=min_popularity,
            ).with_name(f"tracks_{year}_{safe_name}")


# ============================================================
#  Pipeline-runner
# ============================================================

def run_pipeline(
    queries: list[str],
    years: list[int],
    duckdb_path: Path = DUCKDB_PATH,
    market: str | None = "SE",
    limit: int = 50,
    min_popularity: int | None = None,
):
    """
    Kör dlt-pipelinen och laddar resultatet till DuckDB i projektroten.
    """
    DATA_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="spotify_tracks",
        destination=dlt.destinations.duckdb(str(duckdb_path)),
        dataset_name="staging",
    )

    src = spotify_source(
        queries=queries,
        years=years,
        limit=limit,
        market=market,
        min_popularity=min_popularity,
    )

    load_info = pipeline.run(src)

    print("Pipeline körd.")
    print(f"DuckDB-path: {duckdb_path}")
    print(load_info)


# ============================================================
#  Main
# ============================================================

if __name__ == "__main__":
    # Queries:
    # Kör flera genrer + en helt öppen sökning för att få större urval (upp till 10k/träff)
    queries = [
        "",  # bred sökning (SE-marknaden)
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

    # År 2015–2025 för bredare spektrum
    years = list(range(2015, 2026))

    # Popularity-filter: 0–100.
    # 30 ger större volym men fortfarande bort de allra minst populära.
    min_popularity = 30

    run_pipeline(
        queries=queries,
        years=years,
        duckdb_path=DUCKDB_PATH,
        market="SE",
        limit=50,
        min_popularity=min_popularity,
    )
