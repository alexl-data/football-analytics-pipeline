-- Run by Airflow's load_historical_to_raw task (DatabricksSubmitRunOperator,
-- sql_task, pulled directly from this repo via git_source — no manual
-- deployment into the Databricks workspace).
--
-- Source: footballcsv/deutschland mirror of openfootball, columns
-- Round,Date,Team 1,FT,Team 2 (e.g. "1,Fri Sep 18 2020,Bayern München,8-0,FC
-- Schalke 04") — FT is a combined "home-away" score, no separate season
-- column (derived from match_date in staging instead, not raw).

CREATE TABLE IF NOT EXISTS football_analytics_dev.football_analytics.raw_openfootball_matches (
    matchday INT,
    match_date DATE,
    home_team STRING,
    away_team STRING,
    home_score INT,
    away_score INT,
    ingested_at TIMESTAMP
) USING DELTA;

MERGE INTO football_analytics_dev.football_analytics.raw_openfootball_matches AS target
USING (
    SELECT
        Round AS matchday,
        -- Date is "Fri Sep 18 2020" — drop the weekday token, Databricks'
        -- date parser rejects 'EEE' combined with other fields as an
        -- unsafe cross-version pattern (INCONSISTENT_BEHAVIOR_CROSS_VERSION).
        to_date(substring_index(Date, ' ', -3), 'MMM d yyyy') AS match_date,
        `Team 1` AS home_team,
        `Team 2` AS away_team,
        CAST(split(FT, '-')[0] AS INT) AS home_score,
        CAST(split(FT, '-')[1] AS INT) AS away_score,
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
