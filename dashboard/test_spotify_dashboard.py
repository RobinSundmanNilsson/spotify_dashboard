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
    # Resolve path relative to repo root (dashboard/.. = project root)
    repo_root = Path(__file__).resolve().parent.parent
    db_path = repo_root / "data_warehouse" / "spotify.duckdb"
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
    FROM main_mart.mart_spotify_tracks
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
    # Page header
    st.markdown('<h1 class="main-header">🎵 Spotify Dashboard - Svenska Marknaden</h1>', unsafe_allow_html=True)

    # Sidebar: view + filters
    st.sidebar.markdown("## 🎛️ Controls")
    view_mode = st.sidebar.radio("Data view", ["All tracks", "Top artists"], index=0)

    # Load all tracks once
    df_all = load_all_tracks()
    if df_all.empty:
        st.warning("No data available. Kör data-pipelinen först (dbt/dlt).")
        return

    # ============================================================
    # Year Range Selector (2 dropdowns instead of checkboxes)
    # ============================================================
    st.sidebar.markdown("### 📅 Filter by Year Range")
    
    min_year_available = int(df_all['release_year'].min())
    max_year_available = int(df_all['release_year'].max())
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        year_from = st.selectbox(
            "From year",
            options=range(min_year_available, max_year_available + 1),
            index=0,
            key="year_from"
        )
    with col2:
        year_to = st.selectbox(
            "To year",
            options=range(min_year_available, max_year_available + 1),
            index=max_year_available - min_year_available,
            key="year_to"
        )

    # Validera range
    if year_from > year_to:
        st.sidebar.error("'From year' kan inte vara större än 'To year'")
        return

    # Additional filters
    popularity_filter = st.sidebar.selectbox("Popularity Level", ["All", "High (80+)", "Medium (50-79)", "Low (<50)"], index=0)
    album_types = ["All"] + sorted(df_all['album_type'].dropna().unique().tolist())
    album_type_filter = st.sidebar.selectbox("Album Type", options=album_types, index=0)
    search_term = st.sidebar.text_input("Search artist or track", placeholder="Type artist or track to filter...")

    # ============================================================
    # Apply all filters
    # ============================================================
    filtered_df = df_all[
        (df_all['release_year'] >= year_from) & 
        (df_all['release_year'] <= year_to)
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

    # ============================================================
    # Year Range Overview Card
    # ============================================================
    st.markdown("## 📊 Year Range Overview")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Period", f"{year_from}–{year_to}")
    with col2:
        st.metric("Total Tracks", f"{len(filtered_df):,}")
    with col3:
        st.metric("Avg Popularity", f"{filtered_df['popularity'].mean():.1f}")
    with col4:
        st.metric("Unique Artists", f"{filtered_df['main_artist_name'].nunique():,}")
    with col5:
        year_count = year_to - year_from + 1
        st.metric("Years Covered", f"{year_count}")

    # ============================================================
    # Trends Across Selected Period
    # ============================================================
    st.markdown("### 📈 Trends Across Selected Period")
    yearly_stats = filtered_df.groupby('release_year').agg({
        'track_id': 'count',
        'popularity': 'mean'
    }).reset_index()
    yearly_stats.columns = ['Year', 'Track Count', 'Avg Popularity']

    tab1, tab2 = st.tabs(["Track Count per Year", "Avg Popularity Trend"])
    
    with tab1:
        fig_count = px.bar(
            yearly_stats, 
            x='Year', 
            y='Track Count',
            title="Tracks Released per Year (in selected range)",
            color_discrete_sequence=['#1DB954']
        )
        fig_count.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title="Release Year",
            yaxis_title="Track Count"
        )
        st.plotly_chart(fig_count, width='stretch')

    with tab2:
        fig_pop = px.line(
            yearly_stats, 
            x='Year', 
            y='Avg Popularity',
            title="Average Popularity Trend (in selected range)",
            color_discrete_sequence=['#1DB954'],
            markers=True
        )
        fig_pop.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title="Release Year",
            yaxis_title="Average Popularity"
        )
        st.plotly_chart(fig_pop, width='stretch')

    # ============================================================
    # Top Artists or All Tracks View
    # ============================================================
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

        # Display ranking count
        st.subheader(f"📊 Found {len(top_artists)} unique artists")

        # Top 15 Artists - Two column visualization
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🏆 Top Artists by Track Count")
            top_by_tracks = top_artists.nlargest(15, 'total_tracks')
            fig_tracks = px.bar(
                top_by_tracks,
                x='total_tracks',
                y='main_artist_name',
                orientation='h',
                title="Top 15 Artists (by number of tracks)",
                color='total_tracks',
                color_continuous_scale='Greens',
                labels={'total_tracks': 'Track Count', 'main_artist_name': 'Artist'}
            )
            fig_tracks.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
                height=500
            )
            st.plotly_chart(fig_tracks, width='stretch')

        with col2:
            st.markdown("### ⭐ Top Artists by Avg Popularity")
            top_by_pop = top_artists.nlargest(15, 'avg_popularity')
            fig_pop = px.bar(
                top_by_pop,
                x='avg_popularity',
                y='main_artist_name',
                orientation='h',
                title="Top 15 Artists (by avg popularity)",
                color='avg_popularity',
                color_continuous_scale='Greens',
                labels={'avg_popularity': 'Avg Popularity', 'main_artist_name': 'Artist'}
            )
            fig_pop.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
                height=500
            )
            st.plotly_chart(fig_pop, width='stretch')

        # Scatter plot: Track Count vs Avg Popularity
        st.markdown("### 📈 Artist Performance Matrix")
        fig_scatter = px.scatter(
            top_artists,
            x='total_tracks',
            y='avg_popularity',
            size='total_tracks',
            hover_name='main_artist_name',
            title="Artists: Track Count vs Average Popularity",
            color='avg_popularity',
            color_continuous_scale='Greens',
            labels={'total_tracks': 'Number of Tracks', 'avg_popularity': 'Avg Popularity'}
        )
        fig_scatter.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            height=500
        )
        st.plotly_chart(fig_scatter, width='stretch')

        # Full artist table
        st.markdown("### 📋 All Artists (Ranked)")
        st.dataframe(
            top_artists[['computed_rank', 'main_artist_name', 'total_tracks', 'avg_popularity']].head(100),
            width='stretch'
        )

    else:
        # ============================================================
        # Analytics & Distribution (shown for "All tracks" view)
        # ============================================================
        st.markdown("## 📊 Analytics")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Album Type Distribution")
            album_dist = filtered_df['album_type'].value_counts()
            if len(album_dist) > 0:
                fig_pie = px.pie(
                    values=album_dist.values, 
                    names=album_dist.index, 
                    title="Distribution by Album Type",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig_pie, width='stretch')
            else:
                st.write("No album type distribution to show.")

        with col2:
            st.markdown("### Popularity Distribution")
            fig_hist = px.histogram(
                filtered_df,
                x='popularity',
                nbins=20,
                title="Popularity Distribution",
                color_discrete_sequence=['#1DB954']
            )
            fig_hist.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="Popularity",
                yaxis_title="Number of Tracks"
            )
            st.plotly_chart(fig_hist, width='stretch')

        # ============================================================
        # Top Tracks
        # ============================================================
        st.markdown("## 🎵 Top Tracks")
        display_count = st.selectbox("Number of tracks to display:", [10, 25, 50, 100], index=0)
        top_tracks = filtered_df.sort_values('popularity', ascending=False).head(display_count)

        for idx, (_, track) in enumerate(top_tracks.iterrows(), 1):
            c1, c2, c3 = st.columns([1, 6, 2])
            with c1:
                st.markdown(create_cover_html(track.get('cover_image_url', None), 80), unsafe_allow_html=True)
            with c2:
                st.markdown(f"**{idx}. {track.get('track_name', '')}**")
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

    # ============================================================
    # Raw data viewer
    # ============================================================
    with st.expander("📋 Raw Data (Filtered)"):
        st.dataframe(filtered_df, width='stretch')

    # Footer
    st.markdown("---")
    st.markdown("*Data from Spotify API via DLT pipeline | Built with Streamlit & DuckDB*")

if __name__ == "__main__":
    main()