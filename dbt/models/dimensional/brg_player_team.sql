{{ config(
    unique_key=['player_sk', 'team_sk']
) }}

with player_teams as (
    select
        pl.player_sk,
        tm.team_sk,
        min(tm.created_at) as created_at,
        max(tm.updated_at) as updated_at

    from {{ ref('scorecard_entries') }} se
    join {{ ref('dim_player') }} pl on se.player_id = pl.player_id
    join {{ ref('dim_team') }} tm on contains(tm.team_id, se.player_id)
    group by all
)

select
    pt.player_sk,
    pt.team_sk,
    pt.created_at,
    pt.updated_at

from player_teams pt

{% if is_incremental() %}
  where pt.updated_at > (select max(updated_at) from {{ this }})
{% endif %}