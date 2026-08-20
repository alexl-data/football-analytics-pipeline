-- Singular test: a team cannot play itself. Unlike the generic tests
-- (non_negative, one_active_scd_row), this checks one specific, one-off
-- business fact rather than a parameterized, reusable property.

select *
from {{ ref('fct_matches') }}
where home_team_id = away_team_id
