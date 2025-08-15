{{ config(
    unique_key=['player_sk', 'team_sk']
) }}

with player_teams as (
    select
        pl.player_sk,
        tm.team_sk,
        min(tm.created_at) as created_at,
        max(tm.updated_at) as updated_at

    from {{ ref('scorecard_entries') }} sce
    join {{ ref('dim_player') }} pl on sce.player_id = pl.player_id
    join {{ ref('dim_team') }} tm on contains(tm.team_id, sce.player_id)
    group by all
)

select
    player_sk,
    team_sk,
    created_at,
    updated_at

from player_teams

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}