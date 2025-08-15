{{
  config(
    schema='STAGING'
  )
}}

select
    sc.value:objectId::string as scorecard_id,
    
    sc.value:courseId::string as course_id,
    sc.value:courseName::string as course_name,
    sc.value:layoutId::string as layout_id,
    sc.value:layoutName::string as layout_name,

    sc.value:startDate.iso::timestamp_ntz as start_date,
    sc.value:endDate.iso::timestamp_ntz as end_date,
    sc.value:playFormat::string as play_format,
    sc.value:startingHoleIndex::number as starting_hole_index,

    max(sc.value:stepCount::number) as step_count,
    max(sc.value:floorsAscended::number) as floors_ascended,
    max(sc.value:floorsDescended::number) as floors_descended,
    max(sc.value:distance::float) as total_distance,

    sc.value:weather as weather,
    sc.value:entries as entries,
    sc.value:holes as holes,
    sc.value:holesUpdatedAt:iso::timestamp_ntz as holes_updated_at,

    sc.value:version::number as version,
    max(sc.value:notes::string) as notes,
    sc.value:isFinished::boolean as is_finished,
    sc.value:isSimpleScoring::boolean as is_simple_scoring,
    coalesce(sc.value:isPublic::boolean, true) as is_public,
    coalesce(sc.value:isDeleted::boolean, false) as is_deleted,
    sc.value:createdAt::timestamp_ntz as created_at,
    sc.value:updatedAt::timestamp_ntz as updated_at,
    sc.value:createdBy.objectId::string as created_by_user_id,
    min(r.loaded_at) as loaded_at
    
from {{ source('raw_udisc_scorecards', 'raw_udisc_scorecards') }} r,
  lateral flatten(input => raw_data) sc

group by all

qualify row_number() over (partition by scorecard_id order by updated_at desc) = 1