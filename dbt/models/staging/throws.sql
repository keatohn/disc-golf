{{
  config(
    schema='staging'
  )
}}

with throws_flattened as (
    select
        se.scorecard_id,
        se.entry_id,
        se.hole_number,
        json_extract_string(throw, '$.landingZone') as landing_zone,
        cast(json_extract(throw, '$.distance') as double) as throw_distance,
        ordinality as throw_number,
        se.created_at,
        se.updated_at
        
    from {{ ref('scorecard_entries') }} se,
         unnest(json_extract(se.hole_throws, '$')::json[]) with ordinality as t(throw, ordinality)
    where hole_throws is not null
)

select
    scorecard_id,
    entry_id,
    hole_number,
    throw_number,
    landing_zone,
    throw_distance,
    created_at,
    updated_at
    
from throws_flattened