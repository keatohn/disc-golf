{{ config(
    unique_key='team_sk'
) }}

with teams as (
    select
        entry_id,
        listagg(distinct player_id, '_') within group (order by player_id) as team_id,
        {{ dbt_utils.generate_surrogate_key(["team_id"]) }} as team_sk,
        listagg(distinct coalesce(player_first_name, player_display_name), ' + ')
            within group (order by coalesce(player_first_name, player_display_name)) as team_name,
        case
            when count(distinct player_id) = 2 then 'Doubles'
            when count(distinct player_id) = 3 then 'Triples'
            else 'Team'
        end as team_type,
        min(created_at) as created_at,
        max(updated_at) as updated_at
    
    from {{ ref('scorecard_entries') }}
    group by entry_id
    having count(distinct player_id) > 1
    qualify row_number() over (partition by team_sk order by min(created_at), max(updated_at) desc) = 1
)

select
    team_sk,
    team_id,
    team_name,
    team_type,
    created_at,
    updated_at

from teams

{% if is_incremental() %}
  where updated_at > (select max(updated_at ) from {{ this }})
{% endif %}