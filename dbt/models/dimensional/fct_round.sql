{{
  config(
    unique_key='round_sk'
  )
}}

with player_or_team_id as (
    select
        entry_id,
        listagg(distinct player_id, '_') within group (order by player_id) as player_or_team_id

    from {{ ref('scorecard_entries') }}
    group by entry_id
), 

final as (
  select
      se.entry_id as round_id,
      {{ dbt_utils.generate_surrogate_key(["round_id"]) }} as round_sk,
      fsc.scorecard_sk,
      dp.player_sk,
      dt.team_sk,
      sum(se.hole_strokes) as strokes,
      se.starting_score,
      se.entry_created_at as created_at,
      se.entry_updated_at as updated_at

  from {{ ref('scorecard_entries') }} se
  join player_or_team_id ptid on se.entry_id = ptid.entry_id
  join {{ ref('fct_scorecard') }} fsc on se.scorecard_id = fsc.scorecard_id
  left join {{ ref('dim_player') }} dp on ptid.player_or_team_id = dp.player_id
  left join {{ ref('dim_team') }} dt on ptid.player_or_team_id = dt.team_id

  group by all
  qualify row_number() over (partition by round_sk order by se.entry_updated_at desc) = 1
)

select
    round_sk,
    round_id,
    scorecard_sk,
    player_sk,
    team_sk,
    strokes,
    starting_score,
    created_at,
    updated_at

from final

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}