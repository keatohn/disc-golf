{{
  config(
    unique_key='layout_sk'
  )
}}

with layouts_grouped as (
    select
        sc.layout_id,
        sc.layout_name,
        sc.layout_full_name,
        {{ dbt_utils.generate_surrogate_key(["coalesce(sc.layout_id, sc.layout_full_name)"]) }} as layout_sk,
        dc.course_sk,
        case
            when sc.layout_name like '% to %'
                then {{ standardize_layout_name("split_part(sc.layout_name, ' to ', 1)") }}
                    || ' to ' || {{ standardize_layout_name("split_part(sc.layout_name, ' to ', 2)") }}
            else {{ standardize_layout_name('sc.layout_name') }}
        end as layout_type,
        mode(sc.hole_count) as hole_count,
        sc.layout_id is null as is_custom,
        min(sc.created_at) as created_at,
        max(sc.updated_at) as updated_at,
        count(distinct sc.scorecard_id) as scorecard_count,
        max(sc.start_date) as latest_start_date

    from {{ ref('scorecards') }} sc
    join {{ ref('dim_course') }} dc on coalesce(sc.course_id, sc.course_name) = coalesce(dc.course_id, dc.course_name)
    group by all
),
layouts as (
    select *
    from layouts_grouped
    qualify row_number() over (partition by layout_sk order by scorecard_count desc, latest_start_date desc) = 1
)

select
    l.layout_sk,
    l.layout_id,
    l.course_sk,
    l.layout_name,
    l.layout_full_name,
    l.layout_type,
    l.hole_count,
    l.is_custom,
    l.created_at,
    l.updated_at

from layouts l

{% if is_incremental() %}
  where l.updated_at > (select max(updated_at) from {{ this }})
{% endif %}