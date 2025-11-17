-- dim_spotify_track_artists.sql
-- Bridge-like dimension between tracks and their contributing artists.
-- One row per (track_id, artist_id) pair.

with artists_raw as (

    select *
    from staging.raw_spotify_tracks__artists

),
tracks as (

    select
        _dlt_id,
        id as track_id
    from staging.raw_spotify_tracks

)

select
    t.track_id,
    artists_raw.id                     as artist_id,
    artists_raw.name                   as artist_name,
    artists_raw.external_urls__spotify as artist_spotify_url
from artists_raw
join tracks t
    on artists_raw._dlt_parent_id = t._dlt_id