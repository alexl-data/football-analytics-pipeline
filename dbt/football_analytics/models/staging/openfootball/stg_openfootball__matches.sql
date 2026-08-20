-- Parsing/renaming lives here on purpose, not in the raw ingestion job:
-- raw_openfootball_matches lands the CSV source-shaped (see
-- load_historical_to_raw.sql). date parsing works around a Databricks
-- quirk: to_date(..., 'EEE MMM d yyyy') is rejected as an unsafe
-- cross-version pattern because of the weekday token, so the weekday is
-- dropped before parsing.

select
    round as matchday,
    team_1 as home_team,
    team_2 as away_team,
    ingested_at,
    cast(split(ft, '-')[0] as int) as home_score,
    cast(split(ft, '-')[1] as int) as away_score,
    md5(concat_ws('|', date, team_1, team_2)) as match_id,
    to_date(substring_index(date, ' ', -3), 'MMM d yyyy') as match_date
from {{ source('openfootball', 'raw_openfootball_matches') }}
