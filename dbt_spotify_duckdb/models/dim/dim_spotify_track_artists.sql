-- dim_spotify_track_artists.sql
-- Bridge-like dimension between tracks and their contributing artists.
-- One row per (track_id, artist_id) pair. Includes genres from enrichment table.

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
    artists_raw.external_urls__spotify as artist_spotify_url,
    list_transform(
      string_split(
        regexp_replace(cast(e.genres as varchar), '\\\\[|\\\\]|\\"', ''),
        ','
      ),
      x -> trim(x)
    ) as genres
from artists_raw
join tracks t
    on artists_raw._dlt_parent_id = t._dlt_id
left join staging.spotify_artists_enriched e
    on artists_raw.id = e.artist_id
