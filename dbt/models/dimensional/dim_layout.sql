{{
  config(
    unique_key='layout_sk'
  )
}}

with hole_counts as (
    select
        layout_id,
        count(distinct hole_name) as hole_count,
      
    from {{ ref('course_holes') }}
    group by layout_id
),

layouts as (
    select
        layout_id,
        layout_name,
        course_id,
        layout_id is null as is_custom,
        min(convert_timezone('America/Los_Angeles', 'America/New_York', created_at)) as created_at,
        max(convert_timezone('America/Los_Angeles', 'America/New_York', updated_at)) as updated_at,
        {{ dbt_utils.generate_surrogate_key(["coalesce(layout_id, layout_name)"]) }} as layout_sk

    from {{ ref('scorecards') }}
    group by all
    qualify row_number() over (partition by layout_sk order by min(created_at) desc) = 1
)

select
    l.layout_sk,
    l.layout_id,
    l.layout_name,
    dc.course_sk,
    hc.hole_count,
    l.is_custom,
    l.created_at,
    l.updated_at

from layouts l
left join {{ ref('dim_course') }} dc on l.course_id = dc.course_id
left join hole_counts hc on l.layout_id = hc.layout_id

{% if is_incremental() %}
  where l.updated_at > (select max(updated_at) from {{ this }})
{% endif %}