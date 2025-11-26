{{ config(
    unique_key='team_sk'
) }}

with teams_grouped as (
    select
        se.entry_id,
        string_agg(distinct se.player_id, '_' order by se.player_id) as team_id,
        {{ dbt_utils.generate_surrogate_key(["string_agg(distinct se.player_id, '_' order by se.player_id)"]) }} as team_sk,
        string_agg(distinct coalesce(se.player_first_name, se.player_display_name), ' + ' 
            order by coalesce(se.player_first_name, se.player_display_name)) as team_name,
        case
            when count(distinct se.player_id) = 2 then 'Doubles'
            when count(distinct se.player_id) = 3 then 'Triples'
            else 'Team'
        end as team_type,
        min(se.created_at) as created_at,
        max(se.updated_at) as updated_at,
        count(distinct se.scorecard_id) as scorecard_count,
        max(se.start_date) as latest_start_date
    
    from {{ ref('scorecard_entries') }} se
    group by se.entry_id
    having count(distinct se.player_id) > 1
),
teams as (
    select *
    from teams_grouped
    qualify row_number() over (partition by team_sk order by scorecard_count desc, latest_start_date desc) = 1
)

select
    tm.team_sk,
    tm.team_id,
    tm.team_name,
    tm.team_type,
    tm.created_at,
    tm.updated_at

from teams tm

{% if is_incremental() %}
  where tm.updated_at > (select max(updated_at ) from {{ this }})
{% endif %}