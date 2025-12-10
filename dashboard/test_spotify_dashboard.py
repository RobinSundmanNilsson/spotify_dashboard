import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import re
from pathlib import Path
from datetime import datetime
import math

# ============================================================
# Configuration
# ============================================================
st.set_page_config(
    page_title="Spotify Dashboard - Sweden Market",
    page_icon="🎵",
    initial_sidebar_state="expanded",
    layout="wide"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Manrope:wght@400;500;600&display=swap');
    :root {
        --bg: #0e1116;
        --panel: rgba(20, 27, 36, 0.9);
        --accent: #1DB954;
        --accent-2: #33e28c;
        --text: #e7f6ee;
        --muted: #9fb2c5;
        --border: rgba(255, 255, 255, 0.08);
    }
    html, body, [class*="css"]  {
        font-family: 'Space Grotesk', 'Manrope', sans-serif;
        background: var(--bg);
        color: var(--text);
    }
    .main { background: radial-gradient(circle at 10% 20%, rgba(51,226,140,0.08), transparent 25%), radial-gradient(circle at 80% 0%, rgba(29,185,84,0.08), transparent 30%), var(--bg); }
    .hero {
        background: linear-gradient(120deg, rgba(29,185,84,0.18), rgba(9,12,17,0.9));
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 24px 26px;
        position: relative;
        overflow: hidden;
        box-shadow: 0 25px 45px rgba(0,0,0,0.35);
    }
    .hero:before {
        content: "";
        position: absolute;
        inset: -60% 60% 20% -30%;
        background: radial-gradient(circle, rgba(29,185,84,0.15) 0%, transparent 60%);
        transform: rotate(-8deg);
    }
    .hero h1 { font-size: 2.4rem; margin: 0 0 8px; color: #e9fff3; }
    .hero p { color: var(--muted); margin: 0; }
    .eyebrow { letter-spacing: 0.12em; text-transform: uppercase; font-size: 0.82rem; color: var(--accent-2); }
    .pill {
        display: inline-flex; align-items: center; gap: 6px;
        background: rgba(29,185,84,0.12); color: var(--accent);
        padding: 6px 10px; border-radius: 999px; font-weight: 600;
        border: 1px solid rgba(29,185,84,0.3);
    }
    .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 10px; }
    .metric-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 14px;
        box-shadow: 0 15px 30px rgba(0,0,0,0.3);
    }
    .metric-label { color: var(--muted); font-size: 0.9rem; }
    .metric-value { font-size: 1.6rem; font-weight: 700; color: #eafff5; }
    .metric-sub { color: var(--muted); font-size: 0.85rem; }
    .section-title { font-size: 1.2rem; font-weight: 700; margin: 4px 0 8px; }
    .panel {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 16px;
        box-shadow: 0 15px 30px rgba(0,0,0,0.28);
    }
    .track-card { background: rgba(255,255,255,0.03); padding: 12px; border-radius: 12px; margin: 0.35rem 0; border: 1px solid var(--border); }
    .divider { border-top: 1px solid var(--border); margin: 14px 0; }
    .muted { color: var(--muted); }
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
    # Detect available optional columns (like genre) to avoid failing if absent
    col_rows = conn.execute("PRAGMA table_info(main_mart.mart_spotify_tracks)").fetchall()
    col_names = [c[1] for c in col_rows]
    optional_genre = next((c for c in ["genre", "genres", "artist_genres", "main_artist_genres", "primary_genre"] if c in col_names), None)

    select_cols = [
        "track_id",
        "track_name",
        "main_artist_id",
        "main_artist_name",
        "main_artist_spotify_url",
        "album_name",
        "album_type",
        "album__release_date",
        "release_year",
        "release_decade",
        "popularity",
        "popularity_bucket",
        "is_recent_release",
        "preview_url",
        "cover_image_url",
        "cover_height",
        "cover_width",
    ]
    if optional_genre:
        select_cols.append(optional_genre)

    query = """
    SELECT {cols}
    FROM main_mart.mart_spotify_tracks
    WHERE track_name IS NOT NULL
    ORDER BY popularity DESC
    """.format(cols=", ".join(select_cols))
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


def normalize_genres(series: pd.Series) -> pd.Series:
    """Normalize any genre-like column (list or comma-delimited string) into a flat lowercase series."""
    if series is None or series.empty:
        return pd.Series(dtype=object)
    values = []
    for val in series.dropna():
        if isinstance(val, (list, tuple)):
            values.extend(val)
        elif isinstance(val, str):
            cleaned = val.replace(";", ",")
            cleaned = cleaned.replace("{", "").replace("}", "").replace("[", "").replace("]", "")
            parts = [p.strip().strip("'\"") for p in cleaned.split(",") if p.strip()]
            values.extend(parts)
        else:
            try:
                # DuckDB arrays may come through as numpy arrays / Arrow lists
                values.extend(list(val))
            except Exception:
                continue
    return pd.Series(values, dtype="object").str.lower().str.strip()


def to_genre_list(val) -> list[str]:
    """Return a normalized genre list for a single row."""
    # Handle common sequence types first (avoid pd.isna on arrays)
    if isinstance(val, (list, tuple, set)):
        return [str(x).lower().strip() for x in val if str(x).strip()]
    try:
        import numpy as np  # type: ignore
        if isinstance(val, np.ndarray):
            return [str(x).lower().strip() for x in val.tolist() if str(x).strip()]
    except Exception:
        pass
    if isinstance(val, str):
        cleaned = val.replace(";", ",").replace("{", "").replace("}", "").replace("[", "").replace("]", "")
        return [p.strip().strip("'\"").lower() for p in cleaned.split(",") if p.strip()]
    try:
        if pd.isna(val):
            return []
    except Exception:
        pass
    try:
        return [str(x).lower().strip() for x in list(val) if str(x).strip()]
    except Exception:
        # Fallback to scalar
        return [str(val).lower().strip()] if str(val).strip() else []


def clean_genre_label(label) -> str | None:
    """Normalize a single genre label to a clean string; return None if empty."""
    if isinstance(label, (list, tuple, set)):
        if len(label) == 0:
            return None
        label = ",".join([str(x) for x in label])
    label = str(label)
    cleaned = re.sub(r'[\[\]\{\}\"]', "", label)
    cleaned = cleaned.replace("'", "").strip().lower()
    return cleaned or None


def compute_genre_stats(df: pd.DataFrame, genre_col: str) -> pd.DataFrame:
    """Return genre stats with diminishing volume weight + popularity."""
    work = df[[genre_col, "popularity", "track_id", "main_artist_name"]].copy()
    work["genre_list"] = work[genre_col].apply(to_genre_list)
    exploded = work.explode("genre_list")
    exploded = exploded[exploded["genre_list"].notna() & (exploded["genre_list"] != "")]
    exploded["genre_clean"] = exploded["genre_list"].apply(clean_genre_label)
    exploded = exploded[exploded["genre_clean"].notna() & (exploded["genre_clean"] != "")]
    if exploded.empty:
        return pd.DataFrame()
    exploded["pop_weight"] = exploded["popularity"].fillna(0) / 100.0
    genre_stats = (
        exploded.groupby("genre_clean")
        .agg(
            tracks=("track_id", "nunique"),
            artists=("main_artist_name", "nunique"),
            popularity_score=("pop_weight", "sum"),
            pop_mean=("popularity", "mean"),
        )
        .reset_index()
    )
    # Diminishing returns on volume, still reward bigger pools
    genre_stats["volume_weight"] = genre_stats["tracks"].apply(lambda x: math.sqrt(x) * 25)
    genre_stats["score"] = genre_stats["volume_weight"] + genre_stats["pop_mean"]
    return genre_stats.sort_values("score", ascending=False)

# ============================================================
# Main
# ============================================================
def main():
    df_all = load_all_tracks()
    if df_all.empty:
        st.warning("No data available. Run the data pipeline first (dbt/dlt).")
        return

    # Sidebar: view + filters
    st.sidebar.markdown("## 🎛️ Filters & Navigation")
    st.sidebar.markdown("Smarter slicers to focus on eras, moods, and visibility.")
    view_mode = st.sidebar.radio("View", ["Overview", "Top artists", "All tracks"], index=0)

    st.sidebar.markdown("### 📅 Year range")
    min_year_available = int(df_all['release_year'].min())
    max_year_available = int(df_all['release_year'].max())

    year_from, year_to = st.sidebar.slider(
        "Span",
        min_value=min_year_available,
        max_value=max_year_available,
        value=(min_year_available, max_year_available),
        step=1,
    )

    # Validate range
    if year_from > year_to:
        st.sidebar.error("'From year' cannot be greater than 'To year'")
        return

    # Additional filters (grouped)
    st.sidebar.markdown("### ⭐ Popularity")
    popularity_filter = st.sidebar.selectbox("Level", ["All", "High (80+)", "Medium (50-79)", "Low (<50)"], index=0)

    st.sidebar.markdown("### 🎧 Visibility")
    recent_only = st.sidebar.checkbox("Recent releases (last 3 years)", value=False)
    previews_only = st.sidebar.checkbox("Has preview available", value=False)

    st.sidebar.markdown("### 🔍 Search")
    search_term = st.sidebar.text_input("Artist or track", placeholder="Type to filter...")
    st.sidebar.caption("Sweden market sample. Filters apply everywhere.")

    # ============================================================
    # Apply all filters
    # ============================================================
    filtered_df = df_all[
        (df_all['release_year'] >= year_from) & 
        (df_all['release_year'] <= year_to)
    ].copy()

    if popularity_filter != "All":
        if popularity_filter == "High (80+)":
            filtered_df = filtered_df[filtered_df['popularity'] >= 80]
        elif popularity_filter == "Medium (50-79)":
            filtered_df = filtered_df[(filtered_df['popularity'] >= 50) & (filtered_df['popularity'] < 80)]
        else:
            filtered_df = filtered_df[filtered_df['popularity'] < 50]

    if recent_only:
        cutoff = max_year_available - 2
        filtered_df = filtered_df[filtered_df['release_year'] >= cutoff]

    if previews_only:
        filtered_df = filtered_df[filtered_df['preview_url'].notna() & (filtered_df['preview_url'] != "")]

    if search_term and search_term.strip() != "":
        s = search_term.strip().lower()
        filtered_df = filtered_df[
            filtered_df['track_name'].str.lower().str.contains(s) |
            filtered_df['main_artist_name'].str.lower().str.contains(s)
        ]

    if filtered_df.empty:
        st.info("No results for the selected filters.")
        return

    # ============================================================
    # Hero & Snapshot
    # ============================================================
    latest_year = int(filtered_df['release_year'].max())
    year_count = year_to - year_from + 1
    hero_html = f"""
    <div class="hero">
        <div style="position:relative; z-index:1; display:flex; justify-content:space-between; gap:20px; align-items:flex-start; flex-wrap:wrap;">
            <div>
                <div class="eyebrow">Spotify • Sweden</div>
                <h1>Tracks streaming in Sweden {year_from}–{year_to}</h1>
                <p>See how each era performs, surface the real movers, and let filters steer the view.</p>
                <div style="margin-top:10px;" class="pill">🎧 {len(filtered_df):,} tracks in view</div>
            </div>
            <div class="panel" style="min-width:240px; background:rgba(0,0,0,0.25);">
                <div class="metric-label">Latest release year</div>
                <div class="metric-value">{latest_year}</div>
                <div class="metric-sub">Years covered: {year_count}</div>
                <div class="divider"></div>
                <div class="metric-label">Updated</div>
                <div class="metric-sub">{datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
            </div>
        </div>
    </div>
    """
    st.markdown(hero_html, unsafe_allow_html=True)

    # Snapshot metrics
    stats = [
        ("Tracks", f"{len(filtered_df):,}", "Within selected period"),
        ("Avg popularity", f"{filtered_df['popularity'].mean():.1f}", "0–100 scale"),
        ("Unique artists", f"{filtered_df['main_artist_name'].nunique():,}", "Primary artists"),
    ]
    st.markdown('<div class="metric-grid">', unsafe_allow_html=True)
    for label, value, sub in stats:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
                <div class="metric-sub">{sub}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    st.markdown('</div>', unsafe_allow_html=True)

    # Highlight cards
    highlight_col1, highlight_col2 = st.columns([2, 1])
    with highlight_col1:
        st.markdown("### 🔥 Most popular track right now")
        top_track = filtered_df.sort_values('popularity', ascending=False).iloc[0]
        top_card = f"""
        <div class="panel" style="display:flex; gap:16px; align-items:center;">
            {create_cover_html(top_track.get('cover_image_url', None), 90)}
            <div>
                <div class="pill">Popularity {int(top_track.get('popularity', 0))}/100</div>
                <h3 style="margin:6px 0;">{top_track.get('track_name', '')}</h3>
                <div class="muted">By {top_track.get('main_artist_name', '')}</div>
                <div class="muted">Album: {top_track.get('album_name', '')} • {top_track.get('release_year', '')}</div>
                <div style="margin-top:6px;">
                    {"<a href='" + top_track['main_artist_spotify_url'] + "' target='_blank' style='color:var(--accent);text-decoration:none;'>Open in Spotify</a>" if top_track.get('main_artist_spotify_url') else ""}
                </div>
            </div>
        </div>
        """
        st.markdown(top_card, unsafe_allow_html=True)

    with highlight_col2:
        st.markdown("### 🆕 Fresh releases")
        latest_releases = (
            filtered_df.dropna(subset=['release_year'])
            .sort_values(['release_year', 'popularity'], ascending=[False, False])
            .head(4)
        )
        for _, row in latest_releases.iterrows():
            st.markdown(
                f"""
                <div class="track-card">
                    <div style="display:flex; align-items:center; gap:10px;">
                        {create_cover_html(row.get('cover_image_url', None), 46)}
                        <div>
                            <div><strong>{row.get('track_name', '')}</strong></div>
                            <div class="muted">{row.get('main_artist_name', '')} • {row.get('release_year', '')}</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

    # Quick insights to add value with limited data
    st.markdown("### 💡 Quick insights")
    insight_cols = st.columns(3)

    # Highest avg popularity
    with insight_cols[0]:
        top_artist_avg = (
            filtered_df.groupby('main_artist_name')['popularity']
            .mean()
            .sort_values(ascending=False)
        )
        if not top_artist_avg.empty:
            artist_name = top_artist_avg.index[0]
            artist_avg = top_artist_avg.iloc[0]
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">Highest avg popularity</div>
                    <div class="metric-value">{artist_name}</div>
                    <div class="metric-sub">{artist_avg:.1f} / 100</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="metric-card"><div class="metric-label">Highest avg popularity</div><div class="metric-sub">No artist data</div></div>',
                unsafe_allow_html=True,
            )

    # Dominant format
    with insight_cols[1]:
        album_mode = filtered_df['album_type'].mode(dropna=True)
        if len(album_mode) > 0:
            share = (filtered_df['album_type'] == album_mode.iloc[0]).mean() * 100
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">Dominant format</div>
                    <div class="metric-value">{album_mode.iloc[0].title()}</div>
                    <div class="metric-sub">{share:.0f}% of filtered tracks</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="metric-card"><div class="metric-label">Dominant format</div><div class="metric-sub">No data</div></div>',
                unsafe_allow_html=True,
            )

    # Recent share
    with insight_cols[2]:
        recent_share = (filtered_df['release_year'] >= max_year_available - 2).mean() * 100
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Recent share</div>
                <div class="metric-value">{recent_share:.0f}%</div>
                <div class="metric-sub">Released in last 3 years</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ============================================================
    # Genre spotlight (best effort; only if genre columns exist)
    # ============================================================
    genre_col = next((c for c in ["genre", "genres", "artist_genres", "main_artist_genres", "primary_genre"] if c in filtered_df.columns), None)
    st.markdown("## 🎶 Genres & mood")
    if genre_col:
        genre_stats = compute_genre_stats(filtered_df, genre_col)
        if genre_stats.empty:
            st.info("No genre values available in the current selection.")
        else:
            top_genres = genre_stats.head(12)
            g1, g2 = st.columns([2, 1])
            with g1:
                fig_genre = px.bar(
                    top_genres,
                    x="score",
                    y="genre_clean",
                    orientation="h",
                    title="Top genres (diminishing volume + popularity)",
                    color="score",
                    color_continuous_scale="Greens",
                    labels={"genre_clean": "Genre", "score": "Score"},
                )
                fig_genre.update_layout(plot_bgcolor="rgba(0,0,0,0)", height=420, showlegend=False)
                st.plotly_chart(fig_genre)
            with g2:
                lead = top_genres.iloc[0]
                top5 = top_genres.head(5)
                lead_html = f"""
                <div class="panel" style="background:rgba(0,0,0,0.15); border:1px solid var(--border);">
                    <div class="metric-label">Leading genre</div>
                    <div class="metric-value" style="margin:4px 0;">{lead['genre_clean'].title()}</div>
                    <div class="metric-sub">Tracks: {int(lead['tracks'])} • Artists: {int(lead['artists'])}</div>
                    <div class="metric-sub">Avg popularity: {lead['pop_mean']:.1f} • Score: {lead['score']:.1f}</div>
                    <div class="divider"></div>
                    <div class="metric-label">Top 5</div>
                    <ul style="padding-left:18px; margin:4px 0;">
                """
                for _, row in top5.iterrows():
                    lead_html += f"<li>{row['genre_clean'].title()} • {int(row['tracks'])} tracks • pop {row['pop_mean']:.1f}</li>"
                lead_html += "</ul></div>"
                st.markdown(lead_html, unsafe_allow_html=True)
    else:
        st.info("No genre column found in the mart. Add a genre field to `main_mart.mart_spotify_tracks` (e.g., main artist genre) to enable this view.")

    # ============================================================
    # Trends Across Selected Period
    # ============================================================
    st.markdown("## 📈 Trends")
    yearly_stats = filtered_df.groupby('release_year').agg({
        'track_id': 'count',
        'popularity': 'mean'
    }).reset_index()
    yearly_stats.columns = ['Year', 'Track Count', 'Avg Popularity']

    tab1, tab2 = st.tabs(["Release trend", "Distributions"])
    
    with tab1:
        fig_count = px.line(
            yearly_stats, 
            x='Year', 
            y='Track Count',
            title="Tracks Released per Year (in selected range)",
            color_discrete_sequence=['#1DB954'],
            markers=True
        )
        fig_count.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title="Release Year",
            yaxis_title="Track Count"
        )
        st.plotly_chart(fig_count)

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
        st.plotly_chart(fig_pop)

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
            st.plotly_chart(fig_tracks)

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
            st.plotly_chart(fig_pop)

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
        st.plotly_chart(fig_scatter)

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
        if view_mode == "All tracks":
            st.markdown("## 🎵 Tracks & Analytics")
        else:
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
                st.plotly_chart(fig_pie)
            else:
                st.write("No album type distribution to show.")

        with col2:
            st.markdown("### Popularity Distribution")
            pop_series = filtered_df['popularity'].dropna()
            pop_series = pop_series[pop_series > 0]
            if pop_series.empty:
                st.write("No popularity data to show.")
            else:
                pop_df = (
                    pop_series.value_counts()
                    .sort_index()
                    .reset_index()
                )
                pop_df.columns = ["Popularity", "Count"]
                fig_hist = px.line(
                    pop_df,
                    x="Popularity",
                    y="Count",
                    markers=True,
                    title="Popularity Distribution (pop > 0)",
                    color_discrete_sequence=['#1DB954']
                )
                fig_hist.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    xaxis_title="Popularity",
                    yaxis_title="Number of Tracks"
                )
                st.plotly_chart(fig_hist)

        # ============================================================
        # Top Tracks
        # ============================================================
        st.markdown("## 🎵 Top Tracks")
        display_count = st.selectbox("Number of tracks to display:", [10, 25, 50, 100], index=0)
        top_tracks = filtered_df.sort_values('popularity', ascending=False).head(display_count)

        # Render cards in two columns, numbered
        cards_per_row = 2
        cols = st.columns(cards_per_row)
        for idx, track in enumerate(top_tracks.itertuples(index=False), start=1):
            col = cols[(idx - 1) % cards_per_row]
            if (idx - 1) % cards_per_row == 0 and idx != 1:
                cols = st.columns(cards_per_row)
                col = cols[0]
            popularity = track.popularity if not pd.isna(track.popularity) else 0
            rank_badge = f"<span class='pill'>#{idx}</span>"
            pop_badge = f"<span class='pill'>Pop {int(popularity)}/100</span>"
            links = []
            if track.main_artist_spotify_url:
                links.append(f'<a href="{track.main_artist_spotify_url}" target="_blank" style="color:var(--accent);text-decoration:none;">🎵 Spotify</a>')
            if track.preview_url:
                links.append(f'<a href="{track.preview_url}" target="_blank" style="color:var(--accent);text-decoration:none;">🎧 Preview</a>')
            links_html = " • ".join(links)
            card_html = f"""
            <div class="panel" style="background:rgba(0,0,0,0.12); border:1px solid var(--border); padding:12px;">
                <div style="display:flex; gap:12px; align-items:center;">
                    {create_cover_html(track.cover_image_url, 70)}
                    <div>
                        <div style="display:flex; gap:8px; align-items:center;">{rank_badge}{pop_badge}</div>
                        <div style="font-weight:700; margin-top:4px;">{track.track_name}</div>
                        <div class="muted">by {track.main_artist_name}</div>
                        <div class="muted">{track.album_name} • {track.release_year}</div>
                    </div>
                </div>
                <div style="margin-top:8px; display:flex; gap:10px; flex-wrap:wrap;">
                    {links_html}
                </div>
            </div>
            """
            with col:
                st.markdown(card_html, unsafe_allow_html=True)

    # ============================================================
    # Raw data viewer
    # ============================================================
    st.markdown("### 📥 Export filtered data")
    st.download_button(
        label="Download CSV",
        data=filtered_df.to_csv(index=False).encode("utf-8"),
        file_name=f"spotify_filtered_{year_from}_{year_to}.csv",
        mime="text/csv",
    )
    with st.expander("📋 Raw Data (Filtered)"):
        st.dataframe(filtered_df, width='stretch')

    # Footer
    st.markdown("---")
    st.markdown("*Data from Spotify API via DLT pipeline | Built with Streamlit & DuckDB*")

if __name__ == "__main__":
    main()
