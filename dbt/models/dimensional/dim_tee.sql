{{
  config(
    unique_key='tee_sk'
  )
}}

with tees as (
    select
        ch.tee_version_hash as tee_sk,
        cti.canonical_tee_id as tee_id,
        initcap(ch.tee_position_status) as tee_status,
        initcap(ch.tee_type) as tee_type,
        ch.tee_latitude as latitude,
        ch.tee_longitude as longitude,
        min(ch.created_at) as created_at,
        max(ch.updated_at) as updated_at
        
    from {{ ref('course_holes') }} ch
    join {{ ref('canonical_tee_ids') }} cti on ch.tee_version_hash = cti.tee_version_hash
    group by all
    qualify row_number() over (partition by tee_sk order by count(distinct ch.scorecard_id) desc, max(ch.start_date) desc) = 1
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