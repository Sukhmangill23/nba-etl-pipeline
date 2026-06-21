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

CREATE TABLE fact_game_stats (
    stat_id     SERIAL PRIMARY KEY,
    player_id   INTEGER REFERENCES dim_players(player_id),
    team_id     INTEGER REFERENCES dim_teams(team_id),
    game_id     INTEGER NOT NULL,
    game_date   DATE NOT NULL,
    points      INTEGER,
    rebounds    INTEGER,
    assists     INTEGER,
    steals      INTEGER,
    blocks      INTEGER,
    turnovers   INTEGER,
    minutes     VARCHAR(10),
    UNIQUE (player_id, game_id)
);
