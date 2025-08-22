{{
  config(
    unique_key='target_sk'
  )
}}

with targets as (
    select
        ch.target_version_hash as target_sk,
        cti.canonical_target_id as target_id,
        initcap(ch.target_position_status) as target_status,
        initcap(ch.target_type) as target_type,
        ch.basket_type,
        ch.basket_manufacturer,
        ch.target_latitude as latitude,
        ch.target_longitude as longitude,
        min(ch.created_at) as created_at,
        max(ch.updated_at) as updated_at
        
    from {{ ref('course_holes') }} ch
    join {{ ref('canonical_target_ids') }} cti on ch.target_version_hash = cti.target_version_hash
    group by all
    qualify row_number() over (partition by target_sk order by count(distinct ch.scorecard_id) desc, max(ch.start_date) desc) = 1
)

select
    trg.target_sk,
    trg.target_id,
    trg.target_status,
    trg.target_type,
    trg.basket_type,
    trg.basket_manufacturer,
    trg.latitude,
    trg.longitude,
    trg.created_at,
    trg.updated_at

from targets trg

{% if is_incremental() %}
  where trg.updated_at > (select max(updated_at) from {{ this }})
{% endif %}