{{
  config(
    schema='STAGING'
  )
}}

select
    sc.value:objectId::string as scorecard_id,
    
    sc.value:courseId::string as course_id,
    trim(sc.value:courseName::string) as course_name,
    sc.value:layoutId::string as layout_id,
    trim(sc.value:layoutName::string) as layout_name,

    convert_timezone('America/Los_Angeles', 'America/New_York', sc.value:startDate.iso::timestamp_ntz) as start_date,
    convert_timezone('America/Los_Angeles', 'America/New_York', sc.value:endDate.iso::timestamp_ntz) as end_date,
    sc.value:playFormat::string as play_format,
    sc.value:startingHoleIndex::number as starting_hole_index,

    max(sc.value:stepCount::number) as step_count,
    max(sc.value:floorsAscended::number) as floors_ascended,
    max(sc.value:floorsDescended::number) as floors_descended,
    max(sc.value:distance::float) as total_distance,

    sc.value:difficulty::string as difficulty,
    sc.value:customName::string as custom_name,
    sc.value:usesValidSmartLayout::boolean as uses_valid_smart_layout,

    sc.value:weather as weather,
    sc.value:entries as entries,
    sc.value:holes as holes,

    sc.value:version::number as version,
    max(sc.value:notes::string) as notes,
    sc.value:isFinished::boolean as is_finished,
    sc.value:isSimpleScoring::boolean as is_simple_scoring,
    coalesce(sc.value:isPublic::boolean, true) as is_public,
    coalesce(sc.value:isDeleted::boolean, false) as is_deleted,
    convert_timezone('America/Los_Angeles', 'America/New_York', sc.value:createdAt::timestamp_ntz) as created_at,
    convert_timezone('America/Los_Angeles', 'America/New_York', sc.value:updatedAt::timestamp_ntz) as updated_at,
    sc.value:createdBy.objectId::string as created_by_user_id,
    min(r.loaded_at) as loaded_at
    
from {{ source('raw_udisc_scorecards', 'raw_udisc_scorecards') }} r,
  lateral flatten(input => raw_data) sc

group by all

qualify row_number() over (partition by scorecard_id order by min(r.loaded_at)) = 1