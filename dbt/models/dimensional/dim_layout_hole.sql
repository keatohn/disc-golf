{{
  config(
    unique_key='layout_hole_sk'
  )
}}

with layout_holes as (
  select
      dl.layout_id || '_' || ch.hole_name as layout_hole_id,
      {{ dbt_utils.generate_surrogate_key(["layout_hole_id"]) }} as layout_hole_sk,
      dl.layout_sk,
      ch.hole_name,
      ch.hole_number,
      ch.hole_par as par,
      round(utils.meters_to_feet(ch.hole_distance), 0) as distance,
      array_size(to_array(ch.doglegs)) as dogleg_count,
      min(convert_timezone('America/Los_Angeles', 'America/New_York', ch.created_at)) as created_at_min,
      max(convert_timezone('America/Los_Angeles', 'America/New_York', ch.holes_updated_at)) as updated_at

  from {{ ref('course_holes') }} ch
  left join {{ ref('dim_layout') }} dl on ch.layout_id = dl.layout_id
  group by all
  qualify row_number() over (partition by layout_hole_sk order by created_at_min desc) = 1
)

select
  lh.layout_hole_sk,
  lh.layout_hole_id,
  lh.layout_sk,
  lh.hole_name,
  lh.hole_number,
  lh.par,
  lh.distance,
  lh.dogleg_count,
  lh.created_at_min as created_at,
  lh.updated_at

from layout_holes lh

{% if is_incremental() %}
  where lh.updated_at > (select max(updated_at) from {{ this }})
{% endif %}