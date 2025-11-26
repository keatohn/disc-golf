{{
  config(
    unique_key='tee_sk'
  )
}}

with tees_grouped as (
    select
        ch.tee_version_hash as tee_sk,
        cti.canonical_tee_id as tee_id,
        upper(substring(ch.tee_position_status, 1, 1)) || lower(substring(ch.tee_position_status, 2)) as tee_status,
        upper(substring(ch.tee_type, 1, 1)) || lower(substring(ch.tee_type, 2)) as tee_type,
        ch.tee_latitude as latitude,
        ch.tee_longitude as longitude,
        min(ch.created_at) as created_at,
        max(ch.updated_at) as updated_at,
        count(distinct ch.scorecard_id) as scorecard_count,
        max(ch.start_date) as latest_start_date
        
    from {{ ref('course_holes') }} ch
    join {{ ref('canonical_tee_ids') }} cti on ch.tee_version_hash = cti.tee_version_hash
    group by all
),
tees as (
    select *
    from tees_grouped
    qualify row_number() over (partition by tee_sk order by scorecard_count desc, latest_start_date desc) = 1
)

select
    t.tee_sk,
    t.tee_id,
    t.tee_status,
    t.tee_type,
    t.latitude,
    t.longitude,
    t.created_at,
    t.updated_at

from tees t

{% if is_incremental() %}
  where t.updated_at > (select max(updated_at) from {{ this }})
{% endif %}