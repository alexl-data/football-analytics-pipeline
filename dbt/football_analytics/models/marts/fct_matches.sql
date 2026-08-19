-- Incremental, not full-refresh table: on every dbt run, a full-refresh
-- table model rescans and rewrites its entire result set, even for rows
-- that haven't changed. That cost grows with the fact table's size and
-- repeats every single run. Incremental materialization only processes
-- rows touched since the last run (watermarked on ingested_at, which the
-- raw MERGE steps bump whenever a row is inserted or updated), so the
-- per-run cost stays proportional to what's new, not to the whole table.

{{
    config(
        materialized='incremental',
        unique_key='match_id',
        incremental_strategy='merge'
    )
}}

select
    match_id,
    source,
    matchday,
    match_date,
    home_team_id,
    away_team_id,
    home_score,
    away_score,
    result_type,
    ingested_at
from {{ ref('int_matches__unioned') }}

{% if is_incremental() %}
where ingested_at > (select coalesce(max(ingested_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}
