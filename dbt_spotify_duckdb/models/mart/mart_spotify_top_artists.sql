-- mart_spotify_top_artists.sql
-- Top 50 artists based on average popularity and track count.
-- One row per artist with aggregated statistics.

{{ config(
    materialized = "table"
) }}

with artist_stats as (

    select
        main_artist_id,
        main_artist_name,
        main_artist_spotify_url,
        
        count(distinct track_id) as total_tracks,
        avg(popularity) as avg_popularity,
        max(popularity) as max_popularity,
        min(popularity) as min_popularity,
        
        -- Count tracks by decade
        count(distinct case when release_decade = 2020 then track_id end) as tracks_2020s,
        
        -- Count by album type
        count(distinct case when album_type = 'album' then track_id end) as album_tracks,
        count(distinct case when album_type = 'single' then track_id end) as single_tracks,
        
        -- Weighted score: prioritize avg popularity but also reward variety
        (avg(popularity) * 0.8) + (count(distinct track_id) * 0.2) as artist_score
        
    from {{ ref('mart_spotify_tracks') }}
    where main_artist_id is not null
      and popularity is not null
    group by 
        main_artist_id,
        main_artist_name,
        main_artist_spotify_url

),

ranked_artists as (

    select
        *,
        row_number() over (order by artist_score desc) as artist_rank
    from artist_stats

)

select
    artist_rank,
    main_artist_id,
    main_artist_name,
    main_artist_spotify_url,
    
    total_tracks,
    round(avg_popularity, 1) as avg_popularity,
    max_popularity,
    min_popularity,
    
    tracks_2020s,
    album_tracks,
    single_tracks,
    
    round(artist_score, 1) as artist_score
    
from ranked_artists
where artist_rank <= 50
order by artist_rank