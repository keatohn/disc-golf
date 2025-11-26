{{ config(
    unique_key='scorecard_sk'
) }}

with scorecards as (
    select
        sc.scorecard_id,
        {{ dbt_utils.generate_surrogate_key(["sc.scorecard_id"]) }} as scorecard_sk,
        dl.layout_sk,
        timezone('America/New_York', sc.start_date) as start_date,
        timezone('America/New_York', sc.end_date) as end_date,
        date_diff('minute', sc.start_date, sc.end_date) as duration_minutes,
        sc.starting_hole_index + 1 as starting_hole,
        sc.custom_name as scorecard_name,
        case when round({{ meters_to_miles('sc.total_distance') }}, 2) = 0 
            then null 
            else round({{ meters_to_miles('sc.total_distance') }}, 2) 
        end as miles_travelled,
        case when sc.step_count = 0 then null else sc.step_count end as step_count,
        sc.floors_ascended,
        sc.floors_descended,
        round(
            {{ kelvin_to_fahrenheit("cast(json_extract(sc.weather, '$.temperature') as double)") }}
        , 0) as temperature,
        cast(json_extract(sc.weather, '$.wind.direction') as double) as wind_direction_degrees,
        {{ bearing_degrees_to_cardinal_direction(
            "cast(json_extract(sc.weather, '$.wind.direction') as double)",
            8
        ) }} as wind_direction,
        round(
            {{ meters_per_second_to_miles_per_hour("cast(json_extract(sc.weather, '$.wind.speed') as double)") }}
        , 0) as wind_speed,
        cast(json_extract(sc.weather, '$.humidity') as double) as humidity_percent,
        cast(json_extract(sc.weather, '$.cloudCoverPercent') as double) as cloud_cover_percent,
        sc.notes,
        sc.uses_valid_smart_layout,
        sc.is_finished,
        sc.is_public,
        sc.is_deleted,
        sc.version,
        dp.player_sk as created_by_player_sk,
        sc.created_at,
        sc.updated_at

    from {{ ref('scorecards') }} sc
    join {{ ref('dim_layout') }} dl on coalesce(sc.layout_id, sc.layout_name) = coalesce(dl.layout_id, dl.layout_name)
    left join {{ ref('dim_player') }} dp on sc.created_by_user_id = dp.player_id
    qualify row_number() over (partition by scorecard_sk order by sc.updated_at desc) = 1
)

select
    sc.scorecard_sk,
    sc.scorecard_id,
    sc.layout_sk,
    sc.start_date,
    sc.end_date,
    sc.duration_minutes,
    sc.starting_hole,
    sc.scorecard_name,
    sc.miles_travelled,
    sc.step_count,
    sc.floors_ascended,
    sc.floors_descended,
    sc.temperature,
    sc.wind_direction,
    sc.wind_speed,
    sc.humidity_percent,
    sc.cloud_cover_percent,
    sc.notes,
    sc.uses_valid_smart_layout,
    sc.is_finished,
    sc.is_public,
    sc.is_deleted,
    sc.version,
    sc.created_by_player_sk,
    sc.created_at,
    sc.updated_at

from scorecards sc

{% if is_incremental() %}
  where sc.updated_at > (select max(updated_at) from {{ this }})
{% endif %}