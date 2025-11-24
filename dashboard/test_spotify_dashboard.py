# spotify_dashboard.py
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

# ============================================================
# Configuration
# ============================================================
st.set_page_config(
    page_title="Spotify Dashboard - Svenska Marknaden",
    page_icon="🎵",
    initial_sidebar_state="expanded"
)

# Minimal CSS (behålls från din version)
st.markdown("""
<style>
    .main-header { font-size: 2.2rem; font-weight: bold; color: #1DB954; text-align: center; margin-bottom: 1rem; }
    .metric-card { background: linear-gradient(135deg, #1DB954, #1ed760); padding: 1rem; border-radius: 10px; color: white; text-align: center; }
    .track-card { background: #f8f9fa; padding: 1rem; border-radius: 10px; margin: 0.5rem 0; border-left: 4px solid #1DB954; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# Database Connection
# ============================================================
@st.cache_resource
def get_db_connection():
    db_path = Path("data_warehouse/spotify.duckdb")
    if not db_path.exists():
        st.error(f"Database not found at {db_path}. Please run the data pipeline first!")
        st.stop()
    return duckdb.connect(str(db_path))

# ============================================================
# Data Loaders
# ============================================================
@st.cache_data
def load_all_tracks():
    conn = get_db_connection()
    query = """
    SELECT 
        track_id,
        track_name,
        main_artist_id,
        main_artist_name,
        main_artist_spotify_url,
        album_name,
        album_type,
        album__release_date,
        release_year,
        release_decade,
        popularity,
        popularity_bucket,
        is_recent_release,
        preview_url,
        cover_image_url,
        cover_height,
        cover_width
    FROM analytics_mart.mart_spotify_tracks
    WHERE track_name IS NOT NULL
    ORDER BY popularity DESC
    """
    try:
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Error loading all tracks: {e}")
        return pd.DataFrame()

# ============================================================
# Helper
# ============================================================
def create_cover_html(image_url, size=60):
    if pd.isna(image_url) or image_url == "":
        return f'<div style="width:{size}px;height:{size}px;background:#ddd;border-radius:5px;display:flex;align-items:center;justify-content:center;">🎵</div>'
    return f'<img src="{image_url}" width="{size}" height="{size}" style="border-radius:5px;">'

# ============================================================
# Main
# ============================================================
def main():
    # Page header (kept emoji only inside page, not in page_title to avoid duplicate tab icons)
    st.markdown('<h1 class="main-header">🎵 Spotify Dashboard - Svenska Marknaden</h1>', unsafe_allow_html=True)

    # Sidebar: view + filters
    st.sidebar.markdown("## 🎛️ Controls")
    view_mode = st.sidebar.radio("Data view", ["All tracks", "Top artists"], index=0)
    # NOTE: removed the "Use precomputed yearly mart" checkbox as requested

    # Load all tracks once (we'll use this when computing dynamic top artists)
    df_all = load_all_tracks()
    if df_all.empty:
        st.warning("No data available. Kör data-pipelinen först (dbt/dlt).")
        return

    # Year slider (supports ranges)
    min_year = int(df_all['release_year'].min()) if not df_all['release_year'].isna().all() else 2020
    max_year = int(df_all['release_year'].max()) if not df_all['release_year'].isna().all() else datetime.now().year
    year_range = st.sidebar.slider("Release Year", min_value=min_year, max_value=max_year, value=(min_year, max_year))

    # Additional filters
    popularity_filter = st.sidebar.selectbox("Popularity Level", ["All", "High (80+)", "Medium (50-79)", "Low (<50)"], index=0)
    album_types = ["All"] + sorted(df_all['album_type'].dropna().unique().tolist())
    album_type_filter = st.sidebar.selectbox("Album Type", options=album_types, index=0)
    search_term = st.sidebar.text_input("Search artist or track", placeholder="Type artist or track to filter...")

    # Narrow down base dataframe by year range & album type & search term & popularity
    filtered_df = df_all[
        (df_all['release_year'] >= year_range[0]) &
        (df_all['release_year'] <= year_range[1])
    ].copy()

    if album_type_filter != "All":
        filtered_df = filtered_df[filtered_df['album_type'] == album_type_filter]

    if popularity_filter != "All":
        if popularity_filter == "High (80+)":
            filtered_df = filtered_df[filtered_df['popularity'] >= 80]
        elif popularity_filter == "Medium (50-79)":
            filtered_df = filtered_df[(filtered_df['popularity'] >= 50) & (filtered_df['popularity'] < 80)]
        else:
            filtered_df = filtered_df[filtered_df['popularity'] < 50]

    if search_term and search_term.strip() != "":
        s = search_term.strip().lower()
        filtered_df = filtered_df[
            filtered_df['track_name'].str.lower().str.contains(s) |
            filtered_df['main_artist_name'].str.lower().str.contains(s)
        ]

    if filtered_df.empty:
        st.info("Inga resultat för de valda filtren.")
        return

    # KPI section
    st.markdown("## 📊 Key Metrics")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Tracks", f"{len(filtered_df):,}")
    c2.metric("Avg Popularity", f"{filtered_df['popularity'].mean():.1f}")
    c3.metric("Unique Artists", f"{filtered_df['main_artist_name'].nunique():,}")
    c4.metric("Recent Releases (2020+)", f"{len(filtered_df[filtered_df['is_recent_release'] == True]):,}")

    # Top Artists: ALWAYS compute dynamically from the filtered dataset (single year or interval)
    if view_mode == "Top artists":
        st.markdown("## 🎤 Top Artists")

        # Aggregate per artist over the filtered range (avoid duplicate artist rows)
        top_artists = (
            filtered_df.groupby(['main_artist_id', 'main_artist_name'], dropna=False)
            .agg(total_tracks=('track_id', 'count'),
                 avg_popularity=('popularity', 'mean'))
            .round(1)
            .sort_values(['total_tracks', 'avg_popularity'], ascending=[False, False])
            .reset_index()
        )
        top_artists['computed_rank'] = range(1, len(top_artists) + 1)

        # Display a concise table (artist-level, no duplicates)
        st.dataframe(top_artists[['computed_rank', 'main_artist_name', 'total_tracks', 'avg_popularity']].head(50), use_container_width=True)

    # Charts & Top tracks
    st.markdown("## 📈 Analytics & Top Tracks")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Popularity Trends by Year")
        yearly_stats = filtered_df.groupby('release_year')['popularity'].agg(['mean', 'count']).reset_index()
        fig_line = px.line(yearly_stats, x='release_year', y='mean', title="Average Popularity by Release Year", color_discrete_sequence=['#1DB954'])
        fig_line.update_layout(xaxis_title="Release Year", yaxis_title="Average Popularity", plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_line, use_container_width=True)

    with col2:
        st.markdown("### Album Type Distribution")
        album_dist = filtered_df['album_type'].value_counts()
        if len(album_dist) > 0:
            fig_pie = px.pie(values=album_dist.values, names=album_dist.index, title="Distribution by Album Type")
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.write("No album type distribution to show.")

    st.markdown("## 🎵 Top Tracks")
    display_count = st.selectbox("Number of tracks to display:", [10, 25, 50, 100], index=0)
    top_tracks = filtered_df.sort_values('popularity', ascending=False).head(display_count)

    for _, track in top_tracks.iterrows():
        c1, c2, c3 = st.columns([1, 6, 2])
        with c1:
            st.markdown(create_cover_html(track.get('cover_image_url', None), 80), unsafe_allow_html=True)
        with c2:
            st.markdown(f"**{track.get('track_name', '')}**")
            st.markdown(f"*by {track.get('main_artist_name', '')}*")
            st.markdown(f"Album: {track.get('album_name', '')} ({track.get('release_year', '')})")
            popularity = track.get('popularity', 0) if not pd.isna(track.get('popularity', None)) else 0
            st.progress(min(max(popularity / 100, 0), 1))
            st.caption(f"Popularity: {popularity}/100")
        with c3:
            if track.get('main_artist_spotify_url'):
                st.markdown(f'<a href="{track["main_artist_spotify_url"]}" target="_blank">🎵 Open in Spotify</a>', unsafe_allow_html=True)
            if track.get('preview_url'):
                st.markdown(f'<a href="{track["preview_url"]}" target="_blank">🎧 Preview</a>', unsafe_allow_html=True)
        st.divider()

    # Raw data viewer
    with st.expander("📋 Raw Data (Filtered)"):
        st.dataframe(filtered_df, use_container_width=True)

    # Footer
    st.markdown("---")
    st.markdown("*Data from Spotify API via DLT pipeline | Built with Streamlit & DuckDB*")

if __name__ == "__main__":
    main()