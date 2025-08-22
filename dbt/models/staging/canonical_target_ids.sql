{{
  config(
    schema='STAGING'
  )
}}

with target_data as (
    select
        ch.course_id,
        ch.target_version_hash,
        ch.target_position_id,
        ch.target_latitude,
        ch.target_longitude,
        ch.target_position_status,
        ch.target_type,
        ch.basket_type,
        ch.basket_manufacturer,
        min(ch.created_at) as min_scorecard_date,
        max(ch.created_at) as max_scorecard_date,
        count(distinct ch.scorecard_id) as scorecard_cnt,
        row_number() over (partition by ch.course_id order by
            case when ch.target_position_id is not null then 1 else 0 end desc,
            case when ch.target_position_status = 'active' then 1 else 0 end desc,
            case when ch.target_type is not null then 1 else 0 end desc,
            case when ch.basket_type is not null then 1 else 0 end desc,
            case when ch.basket_manufacturer is not null then 1 else 0 end desc,
            min_scorecard_date,
            max_scorecard_date desc,
            scorecard_cnt desc
            ) as preference
        
    from {{ ref('course_holes') }} ch

    where ch.target_version_hash is not null

    group by all

    qualify row_number() over (partition by ch.target_version_hash order by max_scorecard_date desc) = 1
),

target_matching as (
    -- Base case
    select 
        t.target_version_hash,
        t.target_version_hash as group_id,
        0 as depth
        
    from target_data t
    
    union all
    
    -- Recursive case
    select
        t2.target_version_hash,
        tm.group_id,
        tm.depth + 1
    
    from target_matching tm
    join target_data t1 on tm.target_version_hash = t1.target_version_hash
    join target_data t2 on t1.course_id = t2.course_id
        and t1.target_latitude = t2.target_latitude
        and t1.target_longitude = t2.target_longitude

    where tm.depth < 3
),

target_groups as (
    select
        tm.target_version_hash,
        min_by(tm.group_id, t.preference) as group_id,
        min(t.preference) as group_preference,
        min_by(tm.depth, t.preference) as depth
    
    from target_matching tm
    join target_data t on tm.group_id = t.target_version_hash
    group by tm.target_version_hash
),

assign_target_id as (
    select 
        tg.target_version_hash,
        coalesce(
            t.target_position_id,
            substring(md5(tg.target_version_hash), 1, 8)
        )as canonical_target_id,
        tg.depth
    
    from target_groups tg
    join target_data t on tg.group_id = t.target_version_hash
)

select
    atid.*

from assign_target_id atid

group by all