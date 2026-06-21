"""
ingest.py

Step 1 of the NBA ETL pipeline: pull teams, players, and recent game stats
from the BallDontLie API, transform with pandas, and load into Postgres
using the star schema defined in schema.sql.

Run manually for now:
    python ingest.py
"""

import os
import time
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BALLDONTLIE_API_KEY")
BASE_URL = "https://api.balldontlie.io/v1"
HEADERS = {"Authorization": API_KEY}

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_all_pages(endpoint, params=None, max_pages=5):
    """
    Free tier is rate-limited (~5 req/min), so we sleep between calls
    and cap pages so a first run doesn't immediately exhaust the quota.
    """
    params = params or {}
    results = []
    cursor = None

    for _ in range(max_pages):
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params=params)

        if resp.status_code == 429:
            print("Rate limited, sleeping 60s...")
            time.sleep(60)
            continue

        resp.raise_for_status()
        payload = resp.json()
        results.extend(payload.get("data", []))

        cursor = payload.get("meta", {}).get("next_cursor")
        if not cursor:
            break

        time.sleep(12)  # stay under 5 req/min

    return results


def extract_teams():
    print("Fetching teams...")
    raw = fetch_all_pages("teams", max_pages=1)
    df = pd.DataFrame(raw)
    return df[["id", "full_name", "abbreviation", "city", "conference"]].rename(
        columns={"id": "team_id"}
    )


def extract_players(max_pages=3):
    print("Fetching players...")
    raw = fetch_all_pages("players", max_pages=max_pages)
    rows = []
    for p in raw:
        rows.append(
            {
                "player_id": p["id"],
                "first_name": p.get("first_name"),
                "last_name": p.get("last_name"),
                "position": p.get("position") or None,
                "team_id": p.get("team", {}).get("id") if p.get("team") else None,
            }
        )
    return pd.DataFrame(rows)


def extract_stats(max_pages=2):
    print("Fetching recent stats...")
    raw = fetch_all_pages("stats", params={"per_page": 25}, max_pages=max_pages)
    rows = []
    for s in raw:
        # Defensive: skip records missing required identifiers instead of
        # crashing the whole pipeline on one bad row.
        if not s.get("player") or not s.get("game") or not s.get("team"):
            continue

        rows.append(
            {
                "player_id": s["player"]["id"],
                "team_id": s["team"]["id"],
                "game_id": s["game"]["id"],
                "game_date": s["game"]["date"][:10],
                "points": s.get("pts") or 0,
                "rebounds": s.get("reb") or 0,
                "assists": s.get("ast") or 0,
                "steals": s.get("stl") or 0,
                "blocks": s.get("blk") or 0,
                "turnovers": s.get("turnover") or 0,
                "minutes": s.get("min") or None,
            }
        )
    return pd.DataFrame(rows)


def load_teams(df):
    if df.empty:
        return
    conn = get_connection()
    cur = conn.cursor()
    rows = list(df.itertuples(index=False, name=None))
    execute_values(
        cur,
        """
        INSERT INTO dim_teams (team_id, full_name, abbreviation, city, conference)
        VALUES %s
        ON CONFLICT (team_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            abbreviation = EXCLUDED.abbreviation,
            city = EXCLUDED.city,
            conference = EXCLUDED.conference
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} teams.")


def load_players(df):
    if df.empty:
        return
    conn = get_connection()
    cur = conn.cursor()
    rows = list(df.itertuples(index=False, name=None))
    execute_values(
        cur,
        """
        INSERT INTO dim_players (player_id, first_name, last_name, position, team_id)
        VALUES %s
        ON CONFLICT (player_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            position = EXCLUDED.position,
            team_id = EXCLUDED.team_id
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} players.")


def load_stats(df):
    if df.empty:
        return
    conn = get_connection()
    cur = conn.cursor()
    rows = list(
        df[
            [
                "player_id",
                "team_id",
                "game_id",
                "game_date",
                "points",
                "rebounds",
                "assists",
                "steals",
                "blocks",
                "turnovers",
                "minutes",
            ]
        ].itertuples(index=False, name=None)
    )
    execute_values(
        cur,
        """
        INSERT INTO fact_game_stats
            (player_id, team_id, game_id, game_date, points, rebounds,
             assists, steals, blocks, turnovers, minutes)
        VALUES %s
        ON CONFLICT (player_id, game_id) DO NOTHING
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} stat rows (duplicates skipped).")


def run():
    teams_df = extract_teams()
    load_teams(teams_df)

    players_df = extract_players()
    load_players(players_df)

    stats_df = extract_stats()
    load_stats(stats_df)

    print("Ingestion complete.")


if __name__ == "__main__":
    run()
