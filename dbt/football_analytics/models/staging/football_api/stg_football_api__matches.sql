-- Source is already close to clean (raw_football_api_matches does a shallow
-- flatten of the API's JSON, not staging-layer business logic), so this is
-- mostly a passthrough + rename to the project's own column vocabulary.

select
    match_id,
    competition,
    matchday,
    utc_date,
    status,
    home_team,
    away_team,
    home_score,
    away_score,
    ingested_at
from {{ source('football_api', 'raw_football_api_matches') }}
