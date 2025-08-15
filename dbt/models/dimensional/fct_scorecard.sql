{{ config(
    unique_key='scorecard_sk'
) }}

with scorecards as (
    select
        sc.scorecard_id,
        {{ dbt_utils.generate_surrogate_key(["sc.scorecard_id"]) }} as scorecard_sk,
        dl.layout_sk,
        convert_timezone('UTC', 'America/New_York', sc.start_date) as start_date,
        convert_timezone('UTC', 'America/New_York', sc.end_date) as end_date,
        datediff(minute, sc.start_date, sc.end_date) as duration_minutes,
        sc.starting_hole_index + 1 as starting_hole,
        nullifzero(
            round(
                utils.meters_to_miles(sc.total_distance)
            , 2)
        ) as miles_travelled,
        nullifzero(sc.step_count) as step_count,
        sc.floors_ascended,
        sc.floors_descended,
        round(
            utils.kelvin_to_fahrenheit(
                sc.weather:temperature::number
            )
        , 0) as temperature,
        sc.weather:wind:direction::number as wind_direction_degrees,
        case
            when wind_direction_degrees between 337.5 and 360
                or wind_direction_degrees between 0 and 22.5 then 'N'
            when wind_direction_degrees between 22.5 and 67.5 then 'NE'
            when wind_direction_degrees between 67.5 and 112.5 then 'E'
            when wind_direction_degrees between 112.5 and 157.5 then 'SE'
            when wind_direction_degrees between 157.5 and 202.5 then 'S'
            when wind_direction_degrees between 202.5 and 247.5 then 'SW'
            when wind_direction_degrees between 247.5 and 292.5 then 'W'
            when wind_direction_degrees between 292.5 and 337.5 then 'NW'
        end as wind_direction,
        round(
            utils.meters_per_second_to_miles_per_hour(
                sc.weather:wind:speed::number
            )
        , 0) as wind_speed,
        sc.weather:humidity::number as humidity_percent,
        sc.weather:cloudCoverPercent::number as cloud_cover_percent,
        sc.notes,
        sc.is_finished,
        sc.is_public,
        sc.is_deleted,
        sc.version,
        sc.holes_updated_at,
        dp.player_sk as created_by_player_sk,
        sc.created_at,
        sc.updated_at

    from {{ ref('scorecards') }} sc
    join {{ ref('dim_layout') }} dl on coalesce(sc.layout_id, sc.layout_name) = coalesce(dl.layout_id, dl.layout_name)
    join {{ ref('dim_player') }} dp on sc.created_by_user_id = dp.player_id

    qualify row_number() over (partition by scorecard_sk order by sc.updated_at desc) = 1
)

select
    scorecard_sk,
    scorecard_id,
    layout_sk,
    start_date,
    end_date,
    duration_minutes,
    starting_hole,
    miles_travelled,
    step_count,
    floors_ascended,
    floors_descended,
    temperature,
    wind_direction,
    wind_speed,
    humidity_percent,
    cloud_cover_percent,
    notes,
    is_finished,
    is_public,
    is_deleted,
    version,
    holes_updated_at,
    created_by_player_sk,
    created_at,
    updated_at

from scorecards

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}