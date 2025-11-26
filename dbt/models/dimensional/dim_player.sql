{{
  config(
    unique_key='player_sk'
  )
}}

with players_grouped as (
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
        min_by(se.created_by_user_id, se.created_at) as created_by_user_id_raw,
        se.player_created_at as created_at,
        se.player_updated_at as updated_at,
        count(distinct se.scorecard_id) as scorecard_count,
        max(se.start_date) as latest_start_date

    from {{ ref('scorecard_entries') }} se
    group by all
),
players_with_created_by as (
    select
        *,
        case
          when is_udisc_user then null
          else created_by_user_id_raw
        end as created_by_player_id
    from players_grouped
),
players as (
    select *
    from players_with_created_by
    qualify row_number() over (partition by player_sk order by scorecard_count desc, latest_start_date desc) = 1
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