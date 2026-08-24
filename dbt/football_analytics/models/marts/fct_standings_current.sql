-- A "marts on marts" model: the current-season league table, built from
-- fct_matches. This is deliberately in marts, not intermediate — it
-- consumes another marts table, and layering an intermediate model on top
-- of a fact table would invert the usual staging -> intermediate -> marts
-- dependency direction. It's also why this must run after fct_matches, not
-- before it. Feeds snapshot_standings, which turns successive runs of this
-- query into SCD Type 2 history.
--
-- Filtered to the current season only (max season_start_year for the live
-- source) so results don't blend together once a new Bundesliga season
-- starts alongside this one.

with current_season as (
    select max(season_start_year) as season_start_year
    from {{ ref('fct_matches') }}
    where data_source = 'football_api'
),

played_matches as (
    select fct_matches.*
    from {{ ref('fct_matches') }} as fct_matches
    cross join current_season
    where
        fct_matches.data_source = 'football_api'
        and fct_matches.season_start_year = current_season.season_start_year
        and fct_matches.home_score is not null
        and fct_matches.away_score is not null
),

team_perspective as (
    select
        home_team_id as team_id,
        home_score as goals_for,
        away_score as goals_against,
        case when home_score > away_score then 1 else 0 end as win,
        case when home_score = away_score then 1 else 0 end as draw,
        case when home_score < away_score then 1 else 0 end as loss
    from played_matches

    union all

    select
        away_team_id as team_id,
        away_score as goals_for,
        home_score as goals_against,
        case when away_score > home_score then 1 else 0 end as win,
        case when away_score = home_score then 1 else 0 end as draw,
        case when away_score < home_score then 1 else 0 end as loss
    from played_matches
),

aggregated as (
    select
        team_id,
        count(*) as played,
        sum(win) as won,
        sum(draw) as drawn,
        sum(loss) as lost,
        sum(goals_for) as goals_for,
        sum(goals_against) as goals_against,
        sum(goals_for) - sum(goals_against) as goal_difference,
        sum(win) * 3 + sum(draw) as points
    from team_perspective
    group by team_id
)

select
    teams.team_id,
    coalesce(aggregated.played, 0) as played,
    coalesce(aggregated.won, 0) as won,
    coalesce(aggregated.drawn, 0) as drawn,
    coalesce(aggregated.lost, 0) as lost,
    coalesce(aggregated.goals_for, 0) as goals_for,
    coalesce(aggregated.goals_against, 0) as goals_against,
    coalesce(aggregated.goal_difference, 0) as goal_difference,
    coalesce(aggregated.points, 0) as points,
    -- Standard Bundesliga tie-break order: points, then goal difference,
    -- then goals scored. Left-joined so teams with 0 played matches still
    -- appear in the table (e.g. before the season's first matchday).
    rank() over (
        order by
            coalesce(aggregated.points, 0) desc,
            coalesce(aggregated.goal_difference, 0) desc,
            coalesce(aggregated.goals_for, 0) desc
    ) as table_position
from {{ ref('teams') }} as teams
left join aggregated on teams.team_id = aggregated.team_id
