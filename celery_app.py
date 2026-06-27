"""
celery_app.py

Celery application and task definitions for the NBA ETL pipeline.
The `run_ingestion` task wraps the existing ingest.py logic so it can be
triggered asynchronously via a queue (Redis) rather than run manually.

Send a task manually from Python:
    from celery_app import run_ingestion
    run_ingestion.delay()

Or trigger via the API endpoint POST /ingest (defined in main.py).
"""

import os
from celery import Celery
from dotenv import load_dotenv
from ingest import extract_teams, load_teams
from ingest import extract_players, load_players
from ingest import extract_games, load_games

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = Celery(
    "nba_etl",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)

app.conf.beat_schedule = {
    "ingest-every-day": {
        "task": "celery_app.run_ingestion",
        "schedule": 86400.0,  # seconds — 24 hours
    },
}


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def run_ingestion(self):
    """
    ETL ingestion task: pulls teams, players, and games from BallDontLie,
    transforms with pandas, and upserts into Postgres.

    bind=True gives access to `self` so we can call self.retry() on failure.
    max_retries=3 means Celery will retry up to 3 times before marking failed.
    default_retry_delay=60 means it waits 60s between retries.
    """
    try:
        print("Starting ingestion task...")

        teams_df = extract_teams()
        load_teams(teams_df)

        players_df = extract_players()
        load_players(players_df)

        games_df = extract_games(seasons=[2024])
        load_games(games_df)

        print("Ingestion task complete.")
        return {"status": "success"}

    except Exception as exc:
        print(f"Ingestion task failed: {exc}")
        raise self.retry(exc=exc)
