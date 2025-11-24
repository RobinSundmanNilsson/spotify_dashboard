-- mart_spotify_top_artist_tracks.sql
-- All tracks for (top) artists, enriched with both global rank and per-year rank.
-- Materialized as a table.

{{ config(
    materialized = "table"
) }}

----------------------------------------------------------------
-- 1) Per-year artist aggregates & rank (derived from mart_spotify_tracks)
----------------------------------------------------------------
with artist_year as (

    select
        release_year,
        main_artist_id,
        count(*) as total_tracks_year,
        avg(popularity) as artist_avg_popularity_year
    from {{ ref('mart_spotify_tracks') }}
    where release_year is not null
    group by release_year, main_artist_id

),

artist_year_ranked as (

    select
        ay.*,
        row_number() over (
            partition by ay.release_year
            order by ay.total_tracks_year desc, ay.artist_avg_popularity_year desc
        ) as artist_rank_year
    from artist_year ay

),

----------------------------------------------------------------
-- 2) Global artist info (from existing mart_spotify_top_artists)
----------------------------------------------------------------
global_artists as (

    select
        main_artist_id,
        artist_rank            as artist_rank_global,
        avg_popularity         as artist_avg_popularity_global,
        total_tracks           as total_tracks_global,
        artist_score
    from {{ ref('mart_spotify_top_artists') }}

)

----------------------------------------------------------------
-- 3) Final: join tracks to both global and per-year artist metrics
----------------------------------------------------------------
select
    -- Global artist metrics (if exists in top_artists)
    ga.artist_rank_global,
    ga.artist_avg_popularity_global,
    ga.total_tracks_global,
    ga.artist_score,

    -- Per-year artist metrics (may be null if track has no release_year)
    ayr.release_year,
    ayr.artist_rank_year,
    ayr.total_tracks_year,
    ayr.artist_avg_popularity_year,

    -- Track-level fields from mart_spotify_tracks
    t.track_id,
    t.track_name,
    t.main_artist_id,
    t.main_artist_name,
    t.main_artist_spotify_url,
    t.album_name,
    t.album_type,
    t.album__release_date,
    t.release_year,
    t.release_decade,
    t.popularity,
    t.popularity_bucket,
    t.is_recent_release,
    t.preview_url,
    t.cover_image_url,
    t.cover_height,
    t.cover_width

from {{ ref('mart_spotify_tracks') }} t

-- join per-year artist aggregates (left join so we keep tracks without year too)
left join artist_year_ranked ayr
    on t.main_artist_id = ayr.main_artist_id
    and t.release_year = ayr.release_year

-- join global top artist info (inner join if you only want top artists, else left join)
left join global_artists ga
    on t.main_artist_id = ga.main_artist_id

-- If you want to keep only the top artists globally, change the ga join to INNER JOIN
-- and add "where ga.main_artist_id is not null"
order by
    -- Prefer to order by year rank when available, else fallback to global rank/popularity
    coalesce(ayr.artist_rank_year, ga.artist_rank_global, 9999),
    t.popularity desc