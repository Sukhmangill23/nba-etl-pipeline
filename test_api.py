"""
test_api.py

Basic tests for the NBA ETL pipeline.
These run in CI without a live database by testing logic and response
shapes rather than real DB queries — keeps CI fast and dependency-free.
"""

from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


def get_app():
    """Import app fresh each test to avoid module-level side effects."""
    from main import app
    return app


def test_health_check():
    """Health endpoint should always return 200 with status ok."""
    client = TestClient(get_app())
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_teams_returns_list():
    """
    /teams should return a list. Mock the DB so CI doesn't need Postgres.
    This tests that the endpoint wires up correctly and returns the right shape.
    """
    mock_rows = [
        {"team_id": 1, "full_name": "Atlanta Hawks", "abbreviation": "ATL",
         "city": "Atlanta", "conference": "East"},
        {"team_id": 2, "full_name": "Boston Celtics", "abbreviation": "BOS",
         "city": "Boston", "conference": "East"},
    ]

    with patch("main.get_connection") as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = mock_rows
        mock_conn.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.return_value.cursor.return_value = mock_cursor

        client = TestClient(get_app())
        response = client.get("/teams")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_players_returns_paginated():
    """
    /players should return a paginated response with page, page_size, results keys.
    """
    mock_rows = [
        {"player_id": 1, "first_name": "Alex", "last_name": "Abrines",
         "position": "G", "team_id": 21},
    ]

    with patch("main.get_connection") as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = mock_rows
        mock_conn.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.return_value.cursor.return_value = mock_cursor

        client = TestClient(get_app())
        response = client.get("/players")

    assert response.status_code == 200
    body = response.json()
    assert "page" in body
    assert "page_size" in body
    assert "results" in body


def test_team_not_found_returns_404():
    """
    /teams/{id} with a nonexistent ID should return 404, not a 500 crash.
    This is the key error-handling test — confirms we raise HTTPException
    rather than letting psycopg2 errors bubble up raw.
    """
    with patch("main.get_connection") as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # simulate no row found
        mock_conn.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.return_value.cursor.return_value = mock_cursor

        client = TestClient(get_app())
        response = client.get("/teams/99999")

    assert response.status_code == 404


def test_games_pagination_defaults():
    """Page and page_size should default to 1 and 25 respectively."""
    with patch("main.get_connection") as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.return_value.cursor.return_value = mock_cursor

        client = TestClient(get_app())
        response = client.get("/games")

    assert response.status_code == 200
    body = response.json()
    assert body["page"] == 1
    assert body["page_size"] == 25
