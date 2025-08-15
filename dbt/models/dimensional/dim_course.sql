{{
  config(
    unique_key='course_sk'
  )
}}

with courses as (
  select
      course_id,
      course_name,
      course_id is null as is_custom,
      {{ dbt_utils.generate_surrogate_key(["coalesce(course_id, course_name)"]) }} as course_sk,
      min(convert_timezone('America/Los_Angeles', 'America/New_York', created_at)) as created_at,
      max(convert_timezone('America/Los_Angeles', 'America/New_York', updated_at)) as updated_at

  from {{ ref('scorecards') }}
  group by all
  qualify row_number() over (partition by course_sk order by min(created_at) desc) = 1
)

select
  c.course_sk,
  c.course_id,
  c.course_name,
  c.is_custom,
  c.created_at,
  c.updated_at

from courses c

{% if is_incremental() %}
  where c.updated_at > (select max(updated_at) from {{ this }})
{% endif %}