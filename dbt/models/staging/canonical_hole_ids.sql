{{
  config(
    schema='STAGING'
  )
}}

with course_hole_data as (
    select
        ch.course_id,
        ch.hole_version_hash,
        ch.hole_id,
        ch.hole_name,
        ch.hole_number,
        ch.hole_par,
        round(ch.hole_distance, 0) as hole_distance,
        ch.hole_direction,
        coalesce(
            initcap(ch.target_type),
            case
                when lower(ch.layout_name) like '%object%'
                    or lower(ch.course_name) like '%object%'
                    then 'Object'
                else 'Basket'
            end
        ) as target_type,
        ch.tee_position_id,
        ch.tee_longitude,
        ch.tee_latitude,
        ch.target_position_id,
        ch.target_longitude,
        ch.target_latitude,
        min(ch.created_at) as min_scorecard_date,
        max(ch.created_at) as max_scorecard_date,
        count(distinct ch.scorecard_id) as scorecard_cnt,
        row_number() over (partition by ch.course_id order by
            case when ch.hole_id is not null then 1 else 0 end desc,
            min_scorecard_date,
            max_scorecard_date desc,
            case when max(ch.tee_position_status) = 'active' then 1 else 0 end desc,
            case when max(ch.target_position_status) = 'active' then 1 else 0 end desc,
            scorecard_cnt desc
            ) as preference
        
    from {{ ref('course_holes') }} ch

    group by all
    
    qualify row_number() over (partition by ch.hole_version_hash order by max_scorecard_date desc) = 1
),

hole_matching as (
    -- Base case
    select 
        ch.hole_version_hash,
        ch.hole_version_hash as group_id,
        0 as depth
        
    from course_hole_data ch
    
    union all
    
    -- Recursive case
    select
        ch2.hole_version_hash,
        hm.group_id,
        hm.depth + 1
    
    from hole_matching hm
    join course_hole_data ch1 on hm.hole_version_hash = ch1.hole_version_hash
    join course_hole_data ch2 on ch1.course_id = ch2.course_id
        and ch1.hole_version_hash != ch2.hole_version_hash
        and ch1.preference < ch2.preference
        and (ch1.hole_id = ch2.hole_id
            -- Holes share a direction and teepad id
            or (ch1.hole_direction = ch2.hole_direction and ch1.tee_position_id = ch2.tee_position_id)
            -- Holes share a direction and target id
            or (ch1.hole_direction = ch2.hole_direction and ch1.target_position_id = ch2.target_position_id)
            -- Holes share a direction and their tee coordinates within 5 meters apart
            or (ch1.hole_direction = ch2.hole_direction
                and utils.distance_meters(ch1.tee_latitude, ch1.tee_longitude, ch2.tee_latitude, ch2.tee_longitude) <= 5)
            -- Holes share a direction and their target coordinates within 5 meters apart
            or (ch1.hole_direction = ch2.hole_direction
                and utils.distance_meters(ch1.target_latitude, ch1.target_longitude, ch2.target_latitude, ch2.target_longitude) <= 5)
            -- Hole distance difference (meters) + Sum of squares of euclidean distance via lon-lat coordinates (meters) is within 10 meters
            --      and they do not have different tee ids and different target ids
            or (abs(ch1.hole_distance - ch2.hole_distance)
                + sqrt(
                    power(
                        utils.distance_meters(
                            ch1.tee_latitude,
                            ch1.tee_longitude,
                            ch2.tee_latitude,
                            ch2.tee_longitude
                        )
                    , 2)
                    + power(
                        utils.distance_meters(
                            ch1.target_latitude,
                            ch1.target_longitude,
                            ch2.target_latitude,
                            ch2.target_longitude
                        )
                    , 2)
                ) <= 10
                and (ch1.tee_position_id = ch2.tee_position_id
                    or ch1.target_position_id = ch2.target_position_id
                    or greatest(ch1.tee_position_id, ch1.target_position_id,
                        ch2.tee_position_id, ch2.target_position_id) is null)
                )
            -- Exception: Timmons hole 2
            or (ch1.course_id = '2006'
                and ch1.hole_number = 2
                and ch2.hole_number = 2
                )
            -- Exception: Tyger River hole 1
            or (ch1.course_id = '5519'
                and ch1.hole_number = 1
                and ch2.hole_number = 1
                and ch1.tee_position_id = ch2.tee_position_id
                )
            -- Exception: Tyger River hole 18
            or (ch1.course_id = '5519'
                and ch1.hole_number = 18
                and ch2.hole_number = 18
                and ch1.target_position_id = ch2.target_position_id
                )
            )

    where hm.depth < 3
),

hole_groups as (
    select
        hm.hole_version_hash,
        min_by(hm.group_id, ch.preference) as group_id,
        min(ch.preference) as group_preference,
        min_by(hm.depth, ch.preference) as depth
    
    from hole_matching hm
    join course_hole_data ch on hm.group_id = ch.hole_version_hash
    group by hm.hole_version_hash
),

assign_hole_id as (
    select 
        hg.hole_version_hash,
        coalesce(
            ch.hole_id,
            substring(md5(hg.hole_version_hash), 1, 8)
        )as canonical_hole_id,
        hg.depth
    
    from hole_groups hg
    join course_hole_data ch on hg.group_id = ch.hole_version_hash
)

select
    ahid.*

from assign_hole_id ahid

group by all