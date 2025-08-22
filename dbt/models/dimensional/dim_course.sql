{{
  config(
    unique_key='course_sk'
  )
}}

with courses as (
  select
      sc.course_id,
      sc.course_name,
      initcap(replace(sc.difficulty, '-', ' ')) as difficulty,
      sc.course_id is null as is_custom,
      {{ dbt_utils.generate_surrogate_key(["coalesce(sc.course_id, sc.course_name)"]) }} as course_sk,
      min(sc.created_at) as created_at,
      max(sc.updated_at) as updated_at

  from {{ ref('scorecards') }} sc
  group by all
  qualify row_number() over (partition by course_sk order by count(distinct sc.scorecard_id) desc, max(sc.start_date) desc) = 1
)

select
  c.course_sk,
  c.course_id,
  c.course_name,
  c.difficulty,
  c.is_custom,
  c.created_at,
  c.updated_at

from courses c

{% if is_incremental() %}
  where c.updated_at > (select max(updated_at) from {{ this }})
{% endif %}