-- dim_spotify_album_artist.sql
-- Dimension connecting tracks to the artists listed on the album.
-- One row per (track_id, album_artist_id) pair.

with album_artists_raw as (

    select *
    from staging.raw_spotify_tracks__album__artists

),
tracks as (

    select
        _dlt_id,
        id as track_id
    from staging.raw_spotify_tracks

)

select
    t.track_id,
    album_artists_raw.id                     as album_artist_id,
    album_artists_raw.name                   as album_artist_name,
    album_artists_raw.external_urls__spotify as album_artist_spotify_url
from album_artists_raw
join tracks t
    on album_artists_raw._dlt_parent_id = t._dlt_id