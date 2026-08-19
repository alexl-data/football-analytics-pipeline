select
    team_id,
    team_name,
    city,
    stadium,
    capacity,
    founded_year
from {{ ref('teams') }}
