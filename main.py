"""
main.py

Step 2: FastAPI read layer on top of the Postgres star schema built in
ingest.py. Run with:

    uvicorn main:app --reload

Swagger UI auto-generates at http://localhost:8000/docs
"""

import os
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

app = FastAPI(
    title="NBA Stats API",
    description="Read-only REST API over an NBA games star schema, "
    "loaded nightly from the BallDontLie API.",
    version="1.0.0",
)


def get_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


@app.get("/health")
def health_check():
    """Simple liveness check, useful for CI/CD and container orchestration."""
    return {"status": "ok"}


@app.get("/teams")
def list_teams(conference: Optional[str] = None):
    """List all teams, optionally filtered by conference (East/West)."""
    conn = get_connection()
    cur = conn.cursor()

    if conference:
        cur.execute(
            "SELECT * FROM dim_teams WHERE conference = %s ORDER BY full_name",
            (conference,),
        )
    else:
        cur.execute("SELECT * FROM dim_teams ORDER BY full_name")

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/teams/{team_id}")
def get_team(team_id: int):
    """Get a single team by ID."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dim_teams WHERE team_id = %s", (team_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")
    return row


@app.get("/players")
def list_players(
    team_id: Optional[int] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    """List players, optionally filtered by team. Paginated."""
    conn = get_connection()
    cur = conn.cursor()
    offset = (page - 1) * page_size

    if team_id:
        cur.execute(
            "SELECT * FROM dim_players WHERE team_id = %s "
            "ORDER BY last_name LIMIT %s OFFSET %s",
            (team_id, page_size, offset),
        )
    else:
        cur.execute(
            "SELECT * FROM dim_players ORDER BY last_name LIMIT %s OFFSET %s",
            (page_size, offset),
        )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"page": page, "page_size": page_size, "results": rows}


@app.get("/players/{player_id}")
def get_player(player_id: int):
    """Get a single player by ID."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dim_players WHERE player_id = %s", (player_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Player {player_id} not found")
    return row


@app.get("/games")
def list_games(
    team_id: Optional[int] = None,
    season: Optional[int] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    """
    List games, optionally filtered by team (home or away) and/or season.
    Paginated to avoid dumping the whole table in one response.
    """
    conn = get_connection()
    cur = conn.cursor()
    offset = (page - 1) * page_size

    filters = []
    params = []

    if team_id:
        filters.append("(home_team_id = %s OR visitor_team_id = %s)")
        params.extend([team_id, team_id])
    if season:
        filters.append("season = %s")
        params.append(season)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    query = (
        f"SELECT * FROM fact_games {where_clause} "
        f"ORDER BY game_date DESC LIMIT %s OFFSET %s"
    )
    params.extend([page_size, offset])

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"page": page, "page_size": page_size, "results": rows}


@app.get("/games/{game_id}")
def get_game(game_id: int):
    """Get a single game by ID."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM fact_games WHERE game_id = %s", (game_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Game {game_id} not found")
    return row


@app.get("/teams/{team_id}/record")
def get_team_record(team_id: int, season: Optional[int] = None):
    """
    Compute a team's win/loss record from final games, optionally
    scoped to a season. Demonstrates a derived/aggregated metric on
    top of the raw fact table rather than just passthrough reads.
    """
    conn = get_connection()
    cur = conn.cursor()

    season_filter = "AND season = %s" if season else ""

    query = f"""
        SELECT
            COUNT(*) FILTER (
                WHERE (home_team_id = %s AND home_team_score > visitor_team_score)
                   OR (visitor_team_id = %s AND visitor_team_score > home_team_score)
            ) AS wins,
            COUNT(*) FILTER (
                WHERE (home_team_id = %s AND home_team_score < visitor_team_score)
                   OR (visitor_team_id = %s AND visitor_team_score < home_team_score)
            ) AS losses
        FROM fact_games
        WHERE (home_team_id = %s OR visitor_team_id = %s)
          AND status = 'Final'
          {season_filter}
    """

    params = [team_id, team_id, team_id, team_id, team_id, team_id]
    if season:
        params.append(season)

    cur.execute(query, params)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return {"team_id": team_id, "season": season, **row}


@app.post("/ingest")
def trigger_ingestion():
    """
    Trigger an ETL ingestion run as a background Celery task.
    Returns immediately with a task ID — the actual work happens
    asynchronously in the Celery worker container.
    """
    from celery_app import run_ingestion
    task = run_ingestion.delay()
    return {"task_id": task.id, "status": "queued"}
