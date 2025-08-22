{{
  config(
    schema='STAGING'
  )
}}

select
    scorecard_id,
    course_id,
    course_name,
    layout_id,
    layout_name,

    -- Calculate hole number from array position (1-indexed)
    row_number() over (partition by scorecard_id order by hole.index) as hole_number,

    -- Hole level data
    hole.value:holeId::string as hole_id,
    hole.value:name::string as hole_name,
    hole.value:par::number as hole_par,
    hole.value:distance::float as hole_distance,
    hole.value:customDistance::float as hole_distance_custom,
    
    -- Tee and basket information
    hole.value:teePosition.teePositionId::string as tee_position_id,
    hole.value:teePosition.status::string as tee_position_status,
    coalesce(
      hole.value:teePosition.latitude::float,
      hole.value:teePad.latitude::float)
    as tee_latitude,
    coalesce(
      hole.value:teePosition.longitude::float,
      hole.value:teePad.longitude::float
    ) as tee_longitude,
    hole.value:teePosition.teeType.teeType::string as tee_type,
    hole.value:targetPosition.targetPositionId::string as target_position_id,
    hole.value:targetPosition.status::string as target_position_status,
    coalesce(
      hole.value:targetPosition.latitude::float,
      hole.value:basket.latitude::float
    ) as target_latitude,
    coalesce(
      hole.value:targetPosition.longitude::float,
      hole.value:basket.longitude::float
    ) as target_longitude,
    hole.value:targetPosition.targetType.type::string as target_type,
    hole.value:targetPosition.targetType.basketModel.name::string as basket_type,
    hole.value:targetPosition.targetType.basketModel.manufacturer::string as basket_manufacturer,

    utils.bearing_degrees_to_cardinal_direction(
      utils.coordinates_to_bearing_degrees(
        tee_latitude,
        tee_longitude,
        target_latitude,
        target_longitude)
      , 8)
    as hole_direction,

    hole.value:doglegs as doglegs,
    array_size(to_array(doglegs)) as dogleg_count,

    md5(
        concat(
            ifnull(course_id, 'null'),
            ifnull(course_name, 'null'),
            ifnull(hole_id, 'null'),
            hole_name,
            to_varchar(hole_number),
            to_varchar(hole_par),
            ifnull(to_varchar(round(hole_distance, 0)), 'null'),
            ifnull(tee_position_id, 'null'),
            ifnull(tee_position_status, 'null'),
            ifnull(to_varchar(tee_longitude), 'null'),
            ifnull(to_varchar(tee_latitude), 'null'),
            ifnull(tee_type, 'null'),
            ifnull(target_position_id, 'null'),
            ifnull(target_position_status, 'null'),
            ifnull(to_varchar(target_longitude), 'null'),
            ifnull(to_varchar(target_latitude), 'null'),
            ifnull(target_type, 'null'),
            ifnull(basket_type, 'null'),
            ifnull(basket_manufacturer, 'null'),
            ifnull(hole_direction, 'null'),
            to_varchar(dogleg_count)
        )
    ) as hole_version_hash,

    md5(
      concat(
        course_id,
        ifnull(tee_position_id, 'null'),
        ifnull(tee_position_status, 'null'),
        to_varchar(tee_latitude),
        to_varchar(tee_longitude),
        ifnull(tee_type, 'null')
      )
    ) as tee_version_hash,

    md5(
      concat(
        course_id,
        ifnull(target_position_id, 'null'),
        ifnull(target_position_status, 'null'),
        to_varchar(target_latitude),
        to_varchar(target_longitude),
        ifnull(target_type, 'null'),
        ifnull(basket_type, 'null'),
        ifnull(basket_manufacturer, 'null')
      )
    ) as target_version_hash,

    start_date,
    created_at,
    updated_at
    
from {{ ref('scorecards') }},
      lateral flatten(input => holes) hole
where holes is not null