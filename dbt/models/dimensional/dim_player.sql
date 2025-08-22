{{
  config(
    unique_key='player_sk'
  )
}}

with players as (
    select
        {{ dbt_utils.generate_surrogate_key(["se.player_id"]) }} as player_sk,
        se.player_id,
        se.player_full_name as full_name,
        se.player_first_name as first_name,
        nullif(se.player_last_name, '') as last_name,
        se.player_display_name as display_name,
        se.player_username as username,
        se.player_is_udisc_user as is_udisc_user,
        se.player_is_deleted as is_deleted,
        case
          when is_udisc_user then null
          else min_by(se.created_by_user_id, se.created_at)
        end as created_by_player_id,
        se.player_created_at as created_at,
        se.player_updated_at as updated_at

    from {{ ref('scorecard_entries') }} se
    group by all
    qualify row_number() over (partition by player_sk order by count(distinct se.scorecard_id) desc, max(se.start_date) desc) = 1
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