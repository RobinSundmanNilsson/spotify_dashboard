-- dim_spotify_album_images.sql
-- Dimension for album artwork images connected to tracks.
-- One row per (track_id, image_url, height, width).

with album_images_raw as (

    select *
    from staging.raw_spotify_tracks__album__images

),
tracks as (

    select
        _dlt_id,
        id as track_id
    from staging.raw_spotify_tracks

)

select
    t.track_id,
    album_images_raw.height,
    album_images_raw.width,
    album_images_raw.url as image_url
from album_images_raw
join tracks t
    on album_images_raw._dlt_parent_id = t._dlt_id