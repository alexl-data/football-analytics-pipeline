-- Run by Airflow's load_historical_to_raw task (DatabricksSubmitRunOperator,
-- sql_task, pulled directly from this repo via git_source — no manual
-- deployment into the Databricks workspace).
--
-- Raw layer principle: land the source shape as closely as possible — only
-- mechanical renames (spaces -> underscores, lowercase) so columns are valid
-- SQL identifiers, no type interpretation beyond a trivial int cast, no
-- derived columns. Date parsing and splitting FT ("8-0") into home/away
-- score ints is staging-layer work (stg_openfootball__matches, Step 5), not
-- ingestion work — see the "raw vs staging" discussion in project memory.
--
-- Source: footballcsv/deutschland mirror of openfootball, columns
-- Round,Date,Team 1,FT,Team 2 (e.g. "1,Fri Sep 18 2020,Bayern München,8-0,FC
-- Schalke 04").

CREATE TABLE IF NOT EXISTS football_analytics_dev.football_analytics.raw_openfootball_matches (
    round INT,
    date STRING,
    team_1 STRING,
    ft STRING,
    team_2 STRING,
    ingested_at TIMESTAMP
) USING DELTA;

MERGE INTO football_analytics_dev.football_analytics.raw_openfootball_matches AS target
USING (
    SELECT
        Round AS round,
        Date AS date,
        `Team 1` AS team_1,
        FT AS ft,
        `Team 2` AS team_2,
        current_timestamp() AS ingested_at
    FROM read_files(
        '/Volumes/football_analytics_dev/football_analytics/landing/historical_matches.csv',
        format => 'csv',
        header => true,
        -- Explicit schema, not inferred: schema inference on this file was
        -- silently nulling ~80% of the FT column (inconsistent inference
        -- across the sampled rows), a real production-grade footgun.
        schema => 'Round INT, Date STRING, `Team 1` STRING, FT STRING, `Team 2` STRING'
    )
) AS source
ON  target.date = source.date
AND target.team_1 = source.team_1
AND target.team_2 = source.team_2
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
