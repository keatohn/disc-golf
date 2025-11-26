{{
  config(
    unique_key='course_sk'
  )
}}

with courses_grouped as (
  select
      sc.course_id,
      sc.course_name,
      upper(substring(replace(sc.difficulty, '-', ' '), 1, 1)) || lower(substring(replace(sc.difficulty, '-', ' '), 2)) as difficulty,
      sc.course_id is null as is_custom,
      {{ dbt_utils.generate_surrogate_key(["coalesce(sc.course_id, sc.course_name)"]) }} as course_sk,
      min(sc.created_at) as created_at,
      max(sc.updated_at) as updated_at,
      count(distinct sc.scorecard_id) as scorecard_count,
      max(sc.start_date) as latest_start_date

  from {{ ref('scorecards') }} sc
  group by all
),
courses as (
  select *
  from courses_grouped
  qualify row_number() over (partition by course_sk order by scorecard_count desc, latest_start_date desc) = 1
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