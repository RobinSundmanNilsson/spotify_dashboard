import os
from pathlib import Path
from datetime import datetime
from typing import Iterable, List
from functools import lru_cache

from dotenv import load_dotenv
load_dotenv()

import dlt
import duckdb
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
ARTIST_ENRICH_TABLE = "spotify_artists_enriched"
PLAYLIST_TRACKS_TABLE = "spotify_playlist_tracks"


# ============================================================
#  Spotify client (Client Credentials)
# ============================================================

def make_spotify_client() -> spotipy.Spotify:
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "SPOTIPY_CLIENT_ID or SPOTIPY_CLIENT_SECRET missing in the environment. "
            "Add them to a .env in the project root or export them before running the script."
        )

    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    return spotipy.Spotify(auth_manager=auth_manager)


@lru_cache(maxsize=1)
def _spotify_client() -> spotipy.Spotify:
    """
    Lazily create and cache the Spotify client so Dagster can import this module
    even when the secrets are not present at process start. Secrets are required
    when the asset actually executes.
    """
    return make_spotify_client()


# ============================================================
#  dlt-resource: Spotify search per year with a popularity filter
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
    Fetch tracks from the Spotify Search API for a single year, with pagination.

    - query:          free text, e.g., "" or "genre:pop"
    - year:           e.g., 2020
    - limit:          1-50 (Spotify max 50)
    - market:         e.g., "SE" or None
    - min_popularity: 0-100, e.g., 70 to include only fairly popular tracks
    """

    limit = max(1, min(limit, 50))

    if query:
        base_q = f"{query} year:{year}"
    else:
        base_q = f"year:{year}"

    offset = 0
    max_total = 10_000
    processed = 0
    print(f"[DLT] Start search q='{base_q}' market={market} min_popularity={min_popularity}")

    sp = _spotify_client()

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
                yield t

        offset += limit

        if offset >= total:
            break
        if offset >= max_total:
            break

        processed += len(items)
        if processed % (limit * 5) == 0:
            print(f"[DLT] q='{base_q}' market={market} processed ~{processed}/{total} (max 10k)")


# ============================================================
#  dlt-source: loop over years and queries
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
    Build multiple resources:
    - one per (year, query) combination.
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
    Run the dlt pipeline and load results into DuckDB in the project root.
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

    print("Pipeline finished.")
    print(f"DuckDB path: {duckdb_path}")
    print(load_info)


# ============================================================
#  Artist enrichment (genres/followers/popularity)
# ============================================================

def _batch(iterable: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def fetch_artist_genres(artist_ids: list[str]):
    """
    Fetch genres/popularity/followers for a list of artist_ids using Spotify Artists API.
    Returns generator of dicts ready for loading.
    """
    sp = _spotify_client()
    fetched_at = datetime.now().isoformat()
    for chunk in _batch(artist_ids, 50):
        res = sp.artists(chunk) or {}
        artists = res.get("artists", [])
        for a in artists:
            if not a:
                continue
            yield {
                "artist_id": a.get("id"),
                "artist_name": a.get("name"),
                "genres": a.get("genres", []),
                "popularity": a.get("popularity"),
                "followers": (a.get("followers") or {}).get("total"),
                "fetched_at": fetched_at,
            }


@dlt.resource(
    table_name=ARTIST_ENRICH_TABLE,
    write_disposition="append",
    columns={
        "artist_id": {"data_type": "text", "primary_key": True},
        "genres": {"data_type": "json"},
        "artist_name": {"data_type": "text"},
        "popularity": {"data_type": "bigint"},
        "followers": {"data_type": "bigint"},
        "fetched_at": {"data_type": "text"},
    }
)
def spotify_artists_resource(artist_ids: list[str]):
    yield from fetch_artist_genres(artist_ids)


def run_artist_enrichment(duckdb_path: Path = DUCKDB_PATH, max_artists: int | None = None):
    """
    Look at staging.raw_spotify_tracks__artists, find unseen artist_ids,
    fetch genres via Spotify Artists API, and load into staging.spotify_artists_enriched.
    """
    if not duckdb_path.exists():
        raise RuntimeError(f"DuckDB file missing at {duckdb_path}. Run track pipeline first.")

    con = duckdb.connect(str(duckdb_path))

    # Gather distinct artist IDs from staging
    distinct_artist_ids = con.execute("SELECT DISTINCT id AS artist_id FROM staging.raw_spotify_tracks__artists").fetchall()
    all_ids = [row[0] for row in distinct_artist_ids if row[0]]

    # Exclude already enriched artists
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = ?", [ARTIST_ENRICH_TABLE]
    ).fetchone()[0] > 0
    existing_ids: set[str] = set()
    if table_exists:
        existing_ids = {row[0] for row in con.execute(f"SELECT artist_id FROM staging.{ARTIST_ENRICH_TABLE}").fetchall()}

    missing_ids = [aid for aid in all_ids if aid not in existing_ids]
    if max_artists:
        missing_ids = missing_ids[:max_artists]

    if not missing_ids:
        print("No new artists to enrich.")
        return

    pipeline = dlt.pipeline(
        pipeline_name="spotify_tracks",
        destination=dlt.destinations.duckdb(str(duckdb_path)),
        dataset_name="staging",
    )
    load_info = pipeline.run(spotify_artists_resource(missing_ids))
    print(f"Enriched {len(missing_ids)} artists.")
    print(load_info)


# ============================================================
#  Main
# ============================================================

if __name__ == "__main__":
    # Queries:
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

    min_popularity = 0

    run_pipeline(
        queries=queries,
        years=years,
        duckdb_path=DUCKDB_PATH,
        market="SE",
        limit=50,
        min_popularity=min_popularity,
    )
