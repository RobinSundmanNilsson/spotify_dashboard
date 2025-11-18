import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import os
from datetime import datetime

# ============================================================
# Configuration
# ============================================================

st.set_page_config(
    page_title="🎵 Spotify Dashboard",
    page_icon="🎵",

    initial_sidebar_state="expanded"
)

# Custom CSS for Spotify-like styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1DB954;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1DB954, #1ed760);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .track-card {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        border-left: 4px solid #1DB954;
    }
    .sidebar .sidebar-content {
        background: #191414;
    }
    .stSelectbox > div > div {
        background-color: #282828;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# Database Connection
# ============================================================

@st.cache_resource
def get_db_connection():
    """Connect to DuckDB database"""
    db_path = Path("data_warehouse/spotify.duckdb")
    if not db_path.exists():
        st.error(f"Database not found at {db_path}. Please run the data pipeline first!")
        st.stop()
    return duckdb.connect(str(db_path))

@st.cache_data
def load_data():
    """Load data from the mart table"""
    conn = get_db_connection()
    
    query = """
    SELECT 
        track_id,
        track_name,
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
        df = conn.execute(query).df()
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Make sure you've run `dbt run` to create the mart tables!")
        return pd.DataFrame()

# ============================================================
# Helper Functions
# ============================================================

def format_spotify_url(url):
    """Create clickable Spotify link"""
    if pd.isna(url) or url == "":
        return "N/A"
    return f'<a href="{url}" target="_blank">🎵 Spotify</a>'

def create_cover_html(image_url, size=60):
    """Create HTML for album cover"""
    if pd.isna(image_url) or image_url == "":
        return f'<div style="width:{size}px;height:{size}px;background:#ddd;border-radius:5px;display:flex;align-items:center;justify-content:center;">🎵</div>'
    return f'<img src="{image_url}" width="{size}" height="{size}" style="border-radius:5px;">'

# ============================================================
# Main Dashboard
# ============================================================

def main():
    # Header
    st.markdown('<h1 class="main-header">🎵 Spotify Dashboard - Svenska Marknaden</h1>', unsafe_allow_html=True)
    
    # Load data
    df = load_data()
    
    if df.empty:
        st.warning("No data available. Please run the data pipeline first!")
        return
    
    # Sidebar filters
    st.sidebar.markdown("## 🎛️ Filters")
    
    # Year filter
    min_year = int(df['release_year'].min()) if not df['release_year'].isna().all() else 2020
    max_year = int(df['release_year'].max()) if not df['release_year'].isna().all() else datetime.now().year
    
    year_range = st.sidebar.slider(
        "Release Year",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year)
    )
    
    # Popularity filter
    popularity_filter = st.sidebar.selectbox(
        "Popularity Level",
        options=["All", "High (80+)", "Medium (50-79)", "Low (<50)"],
        index=0
    )
    
    # Album type filter
    album_types = ["All"] + sorted(df['album_type'].dropna().unique().tolist())
    album_type_filter = st.sidebar.selectbox(
        "Album Type",
        options=album_types,
        index=0
    )
    
    # Apply filters
    filtered_df = df[
        (df['release_year'] >= year_range[0]) & 
        (df['release_year'] <= year_range[1])
    ].copy()
    
    if popularity_filter != "All":
        if popularity_filter == "High (80+)":
            filtered_df = filtered_df[filtered_df['popularity'] >= 80]
        elif popularity_filter == "Medium (50-79)":
            filtered_df = filtered_df[(filtered_df['popularity'] >= 50) & (filtered_df['popularity'] < 80)]
        elif popularity_filter == "Low (<50)":
            filtered_df = filtered_df[filtered_df['popularity'] < 50]
    
    if album_type_filter != "All":
        filtered_df = filtered_df[filtered_df['album_type'] == album_type_filter]
    
    # KPI Section
    st.markdown("## 📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Tracks", f"{len(filtered_df):,}")
    
    with col2:
        avg_popularity = filtered_df['popularity'].mean()
        st.metric("Avg Popularity", f"{avg_popularity:.1f}")
    
    with col3:
        unique_artists = filtered_df['main_artist_name'].nunique()
        st.metric("Unique Artists", f"{unique_artists:,}")
    
    with col4:
        recent_tracks = len(filtered_df[filtered_df['is_recent_release'] == True])
        st.metric("Recent Releases (2020+)", f"{recent_tracks:,}")
    
    # Charts Section
    st.markdown("## 📈 Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Popularity over time
        st.markdown("### Popularity Trends by Year")
        yearly_stats = filtered_df.groupby('release_year')['popularity'].agg(['mean', 'count']).reset_index()
        
        fig_line = px.line(
            yearly_stats, 
            x='release_year', 
            y='mean',
            title="Average Popularity by Release Year",
            color_discrete_sequence=['#1DB954']
        )
        fig_line.update_layout(
            xaxis_title="Release Year",
            yaxis_title="Average Popularity",
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_line, use_container_width=True)
    
    with col2:
        # Album type distribution
        st.markdown("### Album Type Distribution")
        album_dist = filtered_df['album_type'].value_counts()
        
        fig_pie = px.pie(
            values=album_dist.values,
            names=album_dist.index,
            title="Distribution by Album Type",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    # Top Artists Section
    st.markdown("## 🎤 Top Artists")
    
    top_artists = filtered_df.groupby('main_artist_name').agg({
        'track_id': 'count',
        'popularity': 'mean'
    }).round(1).sort_values('track_id', ascending=False).head(10)
    
    top_artists.columns = ['Number of Tracks', 'Avg Popularity']
    st.dataframe(top_artists, use_container_width=True)
    
    # Top Tracks Section
    st.markdown("## 🎵 Top Tracks")
    
    # Display options
    display_count = st.selectbox("Number of tracks to display:", [10, 25, 50, 100], index=0)
    
    top_tracks = filtered_df.head(display_count)
    
    # Create a nice table with covers
    for idx, track in top_tracks.iterrows():
        col1, col2, col3 = st.columns([1, 6, 2])
        
        with col1:
            st.markdown(create_cover_html(track['cover_image_url'], 80), unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"**{track['track_name']}**")
            st.markdown(f"*by {track['main_artist_name']}*")
            st.markdown(f"Album: {track['album_name']} ({track['release_year']})")
            
            # Popularity bar
            popularity = track['popularity'] if not pd.isna(track['popularity']) else 0
            st.progress(popularity / 100)
            st.caption(f"Popularity: {popularity}/100")
        
        with col3:
            if not pd.isna(track['main_artist_spotify_url']):
                st.markdown(f'<a href="{track["main_artist_spotify_url"]}" target="_blank">🎵 Open in Spotify</a>', unsafe_allow_html=True)
            
            if not pd.isna(track['preview_url']):
                st.markdown(f'<a href="{track["preview_url"]}" target="_blank">🎧 Preview</a>', unsafe_allow_html=True)
        
        st.divider()
    
    # Raw Data Section (collapsible)
    with st.expander("📋 Raw Data (Filtered)"):
        st.dataframe(filtered_df, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("*Data from Spotify API via DLT pipeline | Built with Streamlit & DuckDB*")

if __name__ == "__main__":
    main()