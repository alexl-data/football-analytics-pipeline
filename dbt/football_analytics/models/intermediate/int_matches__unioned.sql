-- Reusable business-logic building block (per the project's dbt layering
-- convention): combines both sources into one shape, resolves team names to
-- canonical team_ids via the alias seed, and computes result_type. Not meant
-- to be queried directly — marts/fct_matches builds on top of this.

with football_api as (
    select
        'football_api' as source,
        cast(match_id as string) as source_match_id,
        matchday,
        cast(utc_date as date) as match_date,
        home_team,
        away_team,
        home_score,
        away_score,
        ingested_at
    from {{ ref('stg_football_api__matches') }}
),

openfootball as (
    select
        'openfootball' as source,
        match_id as source_match_id,
        matchday,
        match_date,
        home_team,
        away_team,
        home_score,
        away_score,
        ingested_at
    from {{ ref('stg_openfootball__matches') }}
),

unioned as (
    select * from football_api
    union all
    select * from openfootball
)

select
    md5(concat_ws('|', unioned.source, unioned.source_match_id)) as match_id,
    unioned.source,
    unioned.matchday,
    unioned.match_date,
    home_alias.team_id as home_team_id,
    away_alias.team_id as away_team_id,
    unioned.home_score,
    unioned.away_score,
    case
        when unioned.home_score is null or unioned.away_score is null then null
        when unioned.home_score > unioned.away_score then 'home_win'
        when unioned.home_score < unioned.away_score then 'away_win'
        else 'draw'
    end as result_type,
    -- German football seasons run Aug-May, crossing a calendar year
    -- boundary: the season is identified by the year it starts in.
    case
        when month(unioned.match_date) >= 7 then year(unioned.match_date)
        else year(unioned.match_date) - 1
    end as season_start_year,
    unioned.ingested_at
from unioned
left join {{ ref('team_name_aliases') }} as home_alias
    on unioned.home_team = home_alias.source_team_name
left join {{ ref('team_name_aliases') }} as away_alias
    on unioned.away_team = away_alias.source_team_name
