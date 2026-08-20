{% docs scd2_snapshot_standings %}
SCD Type 2 history of the current-season league table, built with dbt's
`check` snapshot strategy (compares the actual values of `check_cols`
between runs — `timestamp` wasn't an option here, since `fct_standings_current`
is a freshly computed aggregate each run with no `updated_at` of its own to
compare against).

A new row-version is opened for a team whenever any of `played`, `won`,
`drawn`, `lost`, `goals_for`, `goals_against`, `goal_difference`, `points`,
or `table_position` changes since its last version — the old version gets
`dbt_valid_to` set to the run's timestamp, and a new version is inserted
with `dbt_valid_to = null`. Note `table_position` is relative to every other
team's results, so a team's version can change even on a day it didn't
play, if other results shifted its ranking.

Verified during development with synthetic test data: a simulated matchday
produced new versions for both playing and non-playing teams as expected,
and `dbt_valid_from`/`dbt_valid_to` correctly bracketed the transition. See
`test_one_active_scd_row` for the automated version of that check — every
team must have exactly one currently-active row (`dbt_valid_to is null`) at
all times.
{% enddocs %}
