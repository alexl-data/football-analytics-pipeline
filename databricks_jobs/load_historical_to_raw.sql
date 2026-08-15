-- Run by Airflow's load_historical_to_raw task (DatabricksSubmitRunOperator,
-- sql_task, pulled directly from this repo via git_source — no manual
-- deployment into the Databricks workspace).
--
-- PROVISIONAL: column names/types below are a best guess and have not yet
-- been verified against a real openfootball export. Adjust once a sample
-- file is available (see docs/architecture.md).

CREATE TABLE IF NOT EXISTS football_analytics_dev.football_analytics.raw_openfootball_matches (
    match_date DATE,
    season STRING,
    matchday INT,
    home_team STRING,
    away_team STRING,
    home_score INT,
    away_score INT,
    ingested_at TIMESTAMP
) USING DELTA;

MERGE INTO football_analytics_dev.football_analytics.raw_openfootball_matches AS target
USING (
    SELECT
        Date AS match_date,
        Season AS season,
        Matchday AS matchday,
        HomeTeam AS home_team,
        AwayTeam AS away_team,
        HomeScore AS home_score,
        AwayScore AS away_score,
        current_timestamp() AS ingested_at
    FROM read_files(
        '/Volumes/football_analytics_dev/football_analytics/landing/historical_matches.csv',
        format => 'csv',
        header => true
    )
) AS source
ON  target.match_date = source.match_date
AND target.home_team = source.home_team
AND target.away_team = source.away_team
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
