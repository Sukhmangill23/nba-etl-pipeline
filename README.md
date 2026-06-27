# NBA ETL Pipeline

A production-style ETL pipeline that ingests NBA game data, stores it in a star schema, and exposes it through a REST API. Built to demonstrate backend and data engineering fundamentals: pipeline orchestration, background task processing, containerization, and CI/CD.

## Architecture

```
BallDontLie API
      │
      ▼
 Celery Worker  ◄──  Celery Beat (daily schedule)
      │                     │
      │               Redis (broker)
      │
      ▼
  PostgreSQL (star schema)
      │
      ▼
  FastAPI (REST API + Swagger UI)
```

**Stack:** Python · FastAPI · PostgreSQL · Redis · Celery · Docker Compose · GitHub Actions

## Data Model

Star schema with two dimension tables and one fact table:

- `dim_teams` — NBA franchises (id, name, city, conference)
- `dim_players` — Player roster (id, name, position, team)
- `fact_games` — Game results (scores, date, season, home/away teams)

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Liveness check |
| GET | `/teams` | List teams, filter by `?conference=East\|West` |
| GET | `/teams/{id}` | Single team |
| GET | `/teams/{id}/record` | Win/loss record (computed from fact table) |
| GET | `/players` | Paginated player list, filter by `?team_id=` |
| GET | `/players/{id}` | Single player |
| GET | `/games` | Paginated games, filter by `?team_id=` and `?season=` |
| GET | `/games/{id}` | Single game |
| POST | `/ingest` | Trigger ETL ingestion as a background task |

Interactive docs available at `/docs` (Swagger UI) when running.

## Running Locally

**Prerequisites:** Docker, Docker Compose

```bash
git clone https://github.com/Sukhmangill23/nba-etl-pipeline.git
cd nba-etl-pipeline

# Add your BallDontLie API key (free at balldontlie.io)
cp .env.example .env
# Edit .env and set BALLDONTLIE_API_KEY

# Start all services
docker compose up --build -d

# Trigger initial data load
curl -X POST http://localhost:8000/ingest

# View API docs
open http://localhost:8000/docs
```

## Services

| Container | Role |
|-----------|------|
| `nba_postgres` | PostgreSQL — stores the star schema |
| `nba_redis` | Redis — Celery message broker |
| `nba_api` | FastAPI — REST API + Swagger UI |
| `nba_worker` | Celery worker — executes ETL tasks |
| `nba_beat` | Celery Beat — schedules daily ingestion |

## CI/CD

GitHub Actions runs the test suite on every push to `main`. Tests use mocking to validate API behavior without requiring a live database connection.

```bash
pytest test_api.py -v
```
