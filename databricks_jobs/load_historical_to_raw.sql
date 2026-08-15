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
-- Schalke 04"). upload_historical_file uploads every dropped season file
-- under its own name (not a fixed filename), so this reads the whole landing
-- directory and unions all of them — supports loading multiple seasons.
-- The date string already encodes the year, so the MERGE key below stays
-- unique across seasons without needing a separate season column.

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
        '/Volumes/football_analytics_dev/football_analytics/landing/',
        format => 'csv',
        header => true,
        -- Explicit schema instead of relying on inference: not because
        -- inference caused the null-FT bug we hit earlier (that turned out
        -- to be a genuine gap in one source file, see project history) but
        -- because explicit types are deterministic regardless of which rows
        -- Databricks happens to sample. mode => FAILFAST is the part that
        -- actually guards against a shape change (column added/removed/
        -- reordered) in a future season file: without it, a mismatch would
        -- silently null/misalign data (PERMISSIVE, the default) instead of
        -- failing the job loudly.
        schema => 'Round INT, Date STRING, `Team 1` STRING, FT STRING, `Team 2` STRING',
        mode => 'FAILFAST'
    )
) AS source
ON  target.date = source.date
AND target.team_1 = source.team_1
AND target.team_2 = source.team_2
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
