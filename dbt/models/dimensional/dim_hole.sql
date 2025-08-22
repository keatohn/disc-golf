{{
  config(
    unique_key='hole_sk'
  )
}}

with holes as (
    select
        ch.hole_version_hash as hole_sk,
        chi.canonical_hole_id as hole_id,
        ch.tee_version_hash as tee_sk,
        ch.target_version_hash as target_sk,
        ch.hole_name,
        ch.hole_number,
        ch.hole_par as par,
        round(utils.meters_to_feet(round(ch.hole_distance, 0)), 0) as distance,
        ch.hole_direction as direction,
        ch.dogleg_count,
        min(ch.created_at) as created_at,
        max(ch.updated_at) as updated_at

    from {{ ref('course_holes') }} ch
    join {{ ref('canonical_hole_ids') }} chi on ch.hole_version_hash = chi.hole_version_hash
    group by all
    qualify row_number() over (partition by hole_sk order by count(distinct ch.scorecard_id) desc, max(ch.start_date) desc) = 1
)

select
    h.hole_sk,
    h.hole_id,
    h.tee_sk,
    h.target_sk,
    h.hole_name,
    h.hole_number,
    h.par,
    h.distance,
    h.direction,
    h.dogleg_count,
    h.created_at,
    h.updated_at

from holes h

{% if is_incremental() %}
  where h.updated_at > (select max(updated_at) from {{ this }})
{% endif %}