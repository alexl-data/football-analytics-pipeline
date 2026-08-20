{% snapshot snapshot_standings %}

{{
    config(
        target_database='football_analytics_dev',
        target_schema='football_analytics',
        unique_key='team_id',
        strategy='check',
        check_cols=['played', 'won', 'drawn', 'lost', 'goals_for', 'goals_against', 'goal_difference', 'points', 'table_position'],
    )
}}

    select *
    from {{ ref('fct_standings_current') }}

{% endsnapshot %}
