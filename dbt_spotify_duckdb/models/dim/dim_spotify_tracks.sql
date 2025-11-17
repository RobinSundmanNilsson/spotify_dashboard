-- dim_spotify_tracks.sql
-- Simple dimension for Spotify tracks.
-- Reads from the DLT-created staging table staging.raw_spotify_tracks.

with raw as (

    select *
    from staging.raw_spotify_tracks

)

select
    -- Business key: stable Spotify track ID
    id                              as track_id,

    -- Descriptive attributes
    name                            as track_name,
    album__name                     as album_name,
    album__album_type               as album_type,
    album__release_date,
    cast(substr(album__release_date, 1, 4) as integer) as release_year,
    popularity,
    preview_url,

    -- Simple derived attributes
    case
        when lower(album__album_type) = 'single' then true
        else false
    end                             as is_single,

    (cast(substr(album__release_date, 1, 4) as integer) / 10) * 10 as release_decade

from raw