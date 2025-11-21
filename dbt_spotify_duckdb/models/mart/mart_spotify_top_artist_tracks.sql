-- mart_spotify_top_artist_tracks.sql
-- All tracks from the top 50 artists.
-- Enriched with artist rank for easy filtering and sorting.

{{ config(
    materialized = "table"
) }}

with top_artists as (

    select 
        main_artist_id,
        artist_rank,
        avg_popularity as artist_avg_popularity
    from {{ ref('mart_spotify_top_artists') }}

)

select
    -- Artist info with rank
    ta.artist_rank,
    ta.artist_avg_popularity,
    
    -- All track details
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
inner join top_artists ta
    on t.main_artist_id = ta.main_artist_id

order by 
    ta.artist_rank,
    t.popularity desc