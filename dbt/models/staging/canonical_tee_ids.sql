{{
  config(
    schema='STAGING'
  )
}}

with tee_data as (
    select
        ch.course_id,
        ch.tee_version_hash,
        ch.tee_position_id,
        ch.tee_latitude,
        ch.tee_longitude,
        ch.tee_position_status,
        ch.tee_type,
        min(ch.created_at) as min_scorecard_date,
        max(ch.created_at) as max_scorecard_date,
        count(distinct ch.scorecard_id) as scorecard_cnt,
        row_number() over (partition by ch.course_id order by
            case when ch.tee_position_id is not null then 1 else 0 end desc,
            case when ch.tee_position_status = 'active' then 1 else 0 end desc,
            case when ch.tee_type is not null then 1 else 0 end desc,
            min_scorecard_date,
            max_scorecard_date desc,
            scorecard_cnt desc
            ) as preference
        
    from {{ ref('course_holes') }} ch

    where ch.tee_version_hash is not null

    group by all

    qualify row_number() over (partition by ch.tee_version_hash order by max_scorecard_date desc) = 1
),

tee_matching as (
    -- Base case
    select 
        t.tee_version_hash,
        t.tee_version_hash as group_id,
        0 as depth
        
    from tee_data t
    
    union all
    
    -- Recursive case
    select
        t2.tee_version_hash,
        tm.group_id,
        tm.depth + 1
    
    from tee_matching tm
    join tee_data t1 on tm.tee_version_hash = t1.tee_version_hash
    join tee_data t2 on t1.course_id = t2.course_id
        and t1.tee_latitude = t2.tee_latitude
        and t1.tee_longitude = t2.tee_longitude

    where tm.depth < 3
),

tee_groups as (
    select
        tm.tee_version_hash,
        min_by(tm.group_id, t.preference) as group_id,
        min(t.preference) as group_preference,
        min_by(tm.depth, t.preference) as depth
    
    from tee_matching tm
    join tee_data t on tm.group_id = t.tee_version_hash
    group by tm.tee_version_hash
),

assign_tee_id as (
    select 
        tg.tee_version_hash,
        coalesce(
            t.tee_position_id,
            substring(md5(tg.tee_version_hash), 1, 8)
        )as canonical_tee_id,
        tg.depth
    
    from tee_groups tg
    join tee_data t on tg.group_id = t.tee_version_hash
)

select
    atid.*

from assign_tee_id atid

group by all