# Spotify Dashboard -- Data Pipeline (DLT + DuckDB)

**Status:** Work In Progress\
Projektet är just nu på datainsamlings-steget. Vi hämtar populära
Spotify-låtar från marknaden **SE**, filtrerar på **år ≥ 2020**, och
lagrar datan i en **DuckDB**-databas via **DLT**. Modellering (dbt) och
dashboard (Streamlit) implementeras senare.

## 1. Krav

-   Python 3.11+
-   Spotify Developer-konto
-   DuckDB (CLI valfritt)

## 2. Installation

### Klona repository och skapa virtuellt environment

    git clone <REPO_URL> spotify_dashboard
    cd spotify_dashboard

    python3 -m venv .venv
    source .venv/bin/activate

### Installera beroenden

    uv pip install -r requirements.txt


## 3. Lägg till Spotify API-nycklar

Skapa en `.env` i projektroten:

    SPOTIPY_CLIENT_ID=DIN_CLIENT_ID_HÄR
    SPOTIPY_CLIENT_SECRET=DITT_CLIENT_SECRET_HÄR

## 4. Kör DLT-pipelinen

    python dlt/load_spotify_data.py

## 5. Inspektera databasen

    duckdb data_warehouse/spotify.duckdb