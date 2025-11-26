{{
  config(
    unique_key='hole_sk'
  )
}}

with holes_grouped as (
    select
        ch.hole_version_hash as hole_sk,
        ch.hole_version_hash as hole_id,  -- Use hole_version_hash as hole_id if canonical_hole_ids doesn't exist
        ch.tee_version_hash as tee_sk,
        ch.target_version_hash as target_sk,
        ch.hole_name,
        ch.hole_number,
        ch.hole_par as par,
        round({{ meters_to_feet('round(ch.hole_distance, 0)') }}, 0) as distance,
        ch.hole_direction as direction,
        ch.dogleg_count,
        min(ch.created_at) as created_at,
        max(ch.updated_at) as updated_at,
        count(distinct ch.scorecard_id) as scorecard_count,
        max(ch.start_date) as latest_start_date

    from {{ ref('course_holes') }} ch
    group by all
),
holes as (
    select *
    from holes_grouped
    qualify row_number() over (partition by hole_sk order by scorecard_count desc, latest_start_date desc) = 1
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