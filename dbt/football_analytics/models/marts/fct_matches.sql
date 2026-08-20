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
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

select
    unioned_matches.match_id,
    unioned_matches.data_source,
    unioned_matches.matchday,
    unioned_matches.match_date,
    unioned_matches.season_start_year,
    unioned_matches.home_team_id,
    unioned_matches.away_team_id,
    unioned_matches.home_score,
    unioned_matches.away_score,
    unioned_matches.result_type,
    unioned_matches.ingested_at
from {{ ref('int_matches__unioned') }} as unioned_matches

{% if is_incremental() %}
    where unioned_matches.ingested_at > (
        select coalesce(max(t.ingested_at), timestamp('1900-01-01'))
        from {{ this }} as t
    )
{% endif %}
