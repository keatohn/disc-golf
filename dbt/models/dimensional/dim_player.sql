{{
  config(
    unique_key='player_sk'
  )
}}

with players as (
  select
      {{ dbt_utils.generate_surrogate_key(["player_id"]) }} as player_sk,
      player_id,
      player_full_name as full_name,
      player_first_name as first_name,
      nullif(player_last_name, '') as last_name,
      player_display_name as display_name,
      player_username as username,
      player_is_udisc_user as is_udisc_user,
      player_is_deleted as is_deleted,
      case
        when is_udisc_user then null
        else min_by(created_by_user_id, created_at)
      end as created_by_player_id,
      player_created_at as created_at,
      player_updated_at as updated_at

  from {{ ref('scorecard_entries') }}
  group by all
  qualify row_number() over (partition by player_sk order by player_created_at desc) = 1
)

select
  p.player_sk,
  p.player_id,
  p.full_name,
  p.first_name,
  p.last_name,
  p.display_name,
  p.username,
  p.is_udisc_user,
  p.is_deleted,
  cbp.player_sk as created_by_player_sk,
  p.created_at,
  p.updated_at

from players p
left join players cbp on p.created_by_player_id = cbp.player_id

{% if is_incremental() %}
  where p.updated_at > (select max(updated_at) from {{ this }})
{% endif %}