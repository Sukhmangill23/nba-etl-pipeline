CREATE TABLE dim_teams (
    team_id     INTEGER PRIMARY KEY,
    full_name   VARCHAR(100) NOT NULL,
    abbreviation VARCHAR(10) NOT NULL,
    city        VARCHAR(50),
    conference  VARCHAR(10)
);

CREATE TABLE dim_players (
    player_id   INTEGER PRIMARY KEY,
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    position    VARCHAR(10),
    team_id     INTEGER REFERENCES dim_teams(team_id)
);

CREATE TABLE fact_games (
    game_id             INTEGER PRIMARY KEY,
    game_date           DATE NOT NULL,
    season              INTEGER,
    status              VARCHAR(20),
    home_team_id        INTEGER REFERENCES dim_teams(team_id),
    visitor_team_id     INTEGER REFERENCES dim_teams(team_id),
    home_team_score     INTEGER,
    visitor_team_score  INTEGER,
    period              INTEGER,
    postseason          BOOLEAN
);
