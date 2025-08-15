{{
  config(
    schema='STAGING'
  )
}}

with throws_flattened as (
    select
        scorecard_id,
        entry_id,
        hole_number,
        
        -- Throw level data
        throw.value:landingZone::string as landing_zone,
        throw.value:distance::float as throw_distance,
        throw.index + 1 as throw_number,

        created_at,
        updated_at
        
    from {{ ref('scorecard_entries') }},
         lateral flatten(input => hole_throws) throw
    where hole_throws is not null
    group by all
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