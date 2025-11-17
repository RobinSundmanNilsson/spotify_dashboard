-- mart_spotify_tracks.sql
-- General-purpose mart for Spotify tracks.
-- One row per track, enriched with main artist and a single cover image.
-- Filters (year, popularity, etc.) are meant to be applied in the dashboard.

{{ config(
    materialized = "view"
) }}

with tracks as (

    select
        track_id,
        track_name,
        album_name,
        album_type,
        album__release_date,
        release_year,
        release_decade,
        popularity,
        preview_url,

        -- Useful derived flags for dashboard filters
        case
            when release_year >= 2020 then true
            else false
        end as is_recent_release,

        case
            when popularity >= 80 then 'high'
            when popularity >= 50 then 'medium'
            when popularity is null then 'unknown'
            else 'low'
        end as popularity_bucket

    from {{ ref('dim_spotify_tracks') }}

),

main_artist as (

    -- Pick one "main" artist per track (first by artist_id just to have a stable rule)
    select
        track_id,
        artist_id       as main_artist_id,
        artist_name     as main_artist_name,
        artist_spotify_url as main_artist_spotify_url
    from (
        select
            track_id,
            artist_id,
            artist_name,
            artist_spotify_url,
            row_number() over (
                partition by track_id
                order by artist_id
            ) as rn
        from {{ ref('dim_spotify_track_artists') }}
    )
    where rn = 1

),

cover_image as (

    -- Pick the largest image per track (highest resolution)
    select
        track_id,
        image_url as cover_image_url,
        height    as cover_height,
        width     as cover_width
    from (
        select
            track_id,
            image_url,
            height,
            width,
            row_number() over (
                partition by track_id
                order by height desc, width desc
            ) as rn
        from {{ ref('dim_spotify_album_images') }}
    )
    where rn = 1

)

select
    t.track_id,
    t.track_name,

    ma.main_artist_id,
    ma.main_artist_name,
    ma.main_artist_spotify_url,

    t.album_name,
    t.album_type,
    t.album__release_date,
    t.release_year,
    t.release_decade,

    t.popularity,
    t.popularity_bucket,
    t.is_recent_release,

    t.preview_url,

    ci.cover_image_url,
    ci.cover_height,
    ci.cover_width

from tracks t
left join main_artist ma
    on t.track_id = ma.track_id
left join cover_image ci
    on t.track_id = ci.track_id