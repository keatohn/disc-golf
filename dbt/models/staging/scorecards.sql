{{
  config(
    schema='staging'
  )
}}

with grouped_scorecards as (
    select
        json_extract_string(raw_data, '$.objectId') as scorecard_id,
        
        json_extract_string(raw_data, '$.courseId') as course_id,
        trim(json_extract_string(raw_data, '$.courseName')) as course_name,
        json_extract_string(raw_data, '$.layoutId') as layout_id,
        trim(json_extract_string(raw_data, '$.layoutName')) as layout_name,
        json_extract_string(raw_data, '$.courseName') || ' - ' || trim(json_extract_string(raw_data, '$.layoutName')) as layout_full_name,

        timezone('America/New_York', cast(json_extract_string(raw_data, '$.startDate.iso') as timestamp)) as start_date,
        timezone('America/New_York', cast(json_extract_string(raw_data, '$.endDate.iso') as timestamp)) as end_date,
        json_extract_string(raw_data, '$.playFormat') as play_format,
        cast(json_extract(raw_data, '$.startingHoleIndex') as integer) as starting_hole_index,

        max(cast(json_extract(raw_data, '$.stepCount') as integer)) as step_count,
        max(cast(json_extract(raw_data, '$.floorsAscended') as integer)) as floors_ascended,
        max(cast(json_extract(raw_data, '$.floorsDescended') as integer)) as floors_descended,
        max(cast(json_extract(raw_data, '$.distance') as double)) as total_distance,

        json_extract_string(raw_data, '$.difficulty') as difficulty,
        json_extract_string(raw_data, '$.customName') as custom_name,
        cast(json_extract(raw_data, '$.usesValidSmartLayout') as boolean) as uses_valid_smart_layout,

        json_extract(raw_data, '$.weather') as weather,
        json_extract(raw_data, '$.entries') as entries,
        json_extract(raw_data, '$.holes') as holes,
        json_array_length(json_extract(raw_data, '$.holes')) as hole_count,

        cast(json_extract(raw_data, '$.version') as integer) as version,
        max(json_extract_string(raw_data, '$.notes')) as notes,
        cast(json_extract(raw_data, '$.isFinished') as boolean) as is_finished,
        cast(json_extract(raw_data, '$.isSimpleScoring') as boolean) as is_simple_scoring,
        coalesce(cast(json_extract(raw_data, '$.isPublic') as boolean), true) as is_public,
        coalesce(cast(json_extract(raw_data, '$.isDeleted') as boolean), false) as is_deleted,
        timezone('America/New_York', cast(json_extract_string(raw_data, '$.createdAt') as timestamp)) as created_at,
        timezone('America/New_York', cast(json_extract_string(raw_data, '$.updatedAt') as timestamp)) as updated_at,
        json_extract_string(raw_data, '$.createdBy.objectId') as created_by_user_id,
        min(r.loaded_at) as loaded_at
        
    from {{ source('raw_udisc_scorecards', 'raw_udisc_scorecards') }} r
    
    group by all
)

select *
from grouped_scorecards
qualify row_number() over (partition by scorecard_id order by loaded_at) = 1