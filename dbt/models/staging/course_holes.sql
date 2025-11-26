{{
  config(
    schema='staging'
  )
}}

select
    scorecard_id,
    course_id,
    course_name,
    layout_id,
    layout_name,

    -- Calculate hole number from array position (1-indexed)
    ordinality as hole_number,

    -- Hole level data
    json_extract_string(hole, '$.holeId') as hole_id,
    json_extract_string(hole, '$.name') as hole_name,
    cast(json_extract(hole, '$.par') as integer) as hole_par,
    cast(json_extract(hole, '$.distance') as double) as hole_distance,
    cast(json_extract(hole, '$.customDistance') as double) as hole_distance_custom,
    
    -- Tee and basket information
    json_extract_string(hole, '$.teePosition.teePositionId') as tee_position_id,
    json_extract_string(hole, '$.teePosition.status') as tee_position_status,
    coalesce(
      cast(json_extract(hole, '$.teePosition.latitude') as double),
      cast(json_extract(hole, '$.teePad.latitude') as double))
    as tee_latitude,
    coalesce(
      cast(json_extract(hole, '$.teePosition.longitude') as double),
      cast(json_extract(hole, '$.teePad.longitude') as double)
    ) as tee_longitude,
    json_extract_string(hole, '$.teePosition.teeType.teeType') as tee_type,
    json_extract_string(hole, '$.targetPosition.targetPositionId') as target_position_id,
    json_extract_string(hole, '$.targetPosition.status') as target_position_status,
    coalesce(
      cast(json_extract(hole, '$.targetPosition.latitude') as double),
      cast(json_extract(hole, '$.basket.latitude') as double)
    ) as target_latitude,
    coalesce(
      cast(json_extract(hole, '$.targetPosition.longitude') as double),
      cast(json_extract(hole, '$.basket.longitude') as double)
    ) as target_longitude,
    json_extract_string(hole, '$.targetPosition.targetType.type') as target_type,
    json_extract_string(hole, '$.targetPosition.targetType.basketModel.name') as basket_type,
    json_extract_string(hole, '$.targetPosition.targetType.basketModel.manufacturer') as basket_manufacturer,

    {{ bearing_degrees_to_cardinal_direction(
        coordinates_to_bearing_degrees(
            'coalesce(cast(json_extract(hole, \'$.teePosition.latitude\') as double), cast(json_extract(hole, \'$.teePad.latitude\') as double))',
            'coalesce(cast(json_extract(hole, \'$.teePosition.longitude\') as double), cast(json_extract(hole, \'$.teePad.longitude\') as double))',
            'coalesce(cast(json_extract(hole, \'$.targetPosition.latitude\') as double), cast(json_extract(hole, \'$.basket.latitude\') as double))',
            'coalesce(cast(json_extract(hole, \'$.targetPosition.longitude\') as double), cast(json_extract(hole, \'$.basket.longitude\') as double))'
        ),
        8
    ) }} as hole_direction,

    json_extract(hole, '$.doglegs') as doglegs,
    json_array_length(json_extract(hole, '$.doglegs')) as dogleg_count,

    md5(
        concat(
            coalesce(course_id, 'null'),
            coalesce(course_name, 'null'),
            coalesce(hole_id, 'null'),
            hole_name,
            cast(hole_number as varchar),
            cast(hole_par as varchar),
            coalesce(cast(round(hole_distance, 0) as varchar), 'null'),
            coalesce(tee_position_id, 'null'),
            coalesce(tee_position_status, 'null'),
            coalesce(cast(tee_longitude as varchar), 'null'),
            coalesce(cast(tee_latitude as varchar), 'null'),
            coalesce(tee_type, 'null'),
            coalesce(target_position_id, 'null'),
            coalesce(target_position_status, 'null'),
            coalesce(cast(target_longitude as varchar), 'null'),
            coalesce(cast(target_latitude as varchar), 'null'),
            coalesce(target_type, 'null'),
            coalesce(basket_type, 'null'),
            coalesce(basket_manufacturer, 'null'),
            coalesce(hole_direction, 'null'),
            cast(dogleg_count as varchar)
        )
    ) as hole_version_hash,

    md5(
      concat(
        course_id,
        coalesce(tee_position_id, 'null'),
        coalesce(tee_position_status, 'null'),
        cast(tee_latitude as varchar),
        cast(tee_longitude as varchar),
        coalesce(tee_type, 'null')
      )
    ) as tee_version_hash,

    md5(
      concat(
        course_id,
        coalesce(target_position_id, 'null'),
        coalesce(target_position_status, 'null'),
        cast(target_latitude as varchar),
        cast(target_longitude as varchar),
        coalesce(target_type, 'null'),
        coalesce(basket_type, 'null'),
        coalesce(basket_manufacturer, 'null')
      )
    ) as target_version_hash,

    start_date,
    created_at,
    updated_at
    
from {{ ref('scorecards') }},
      unnest(json_extract(holes, '$')::json[]) with ordinality as t(hole, ordinality)
where holes is not null