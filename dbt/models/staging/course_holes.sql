{{
  config(
    schema='STAGING'
  )
}}

with holes_flattened as (
    select
        scorecard_id,
        course_id,
        course_name,
        layout_id,
        layout_name,

        -- Calculate hole number from array position (1-indexed)
        row_number() over (partition by scorecard_id order by hole.index) as hole_number,

        -- Hole level data
        hole.value:name::string as hole_name,
        hole.value:par::number as hole_par,
        hole.value:distance::float as hole_distance,
        
        -- Tee pad coordinates
        hole.value:teePad.latitude::float as tee_latitude,
        hole.value:teePad.longitude::float as tee_longitude,
        
        -- Basket coordinates
        hole.value:basket.latitude::float as basket_latitude,
        hole.value:basket.longitude::float as basket_longitude,
        
        -- Additional hole features
        hole.value:doglegs as doglegs,

        holes_updated_at,
        created_at,
        updated_at
        
    from {{ ref('scorecards') }},
         lateral flatten(input => holes) hole
    where holes is not null
)

select * from holes_flattened