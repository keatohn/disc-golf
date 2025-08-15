{{
  config(
    unique_key='round_hole_sk'
  )
}}

with round_holes as (
    select
        {{ dbt_utils.generate_surrogate_key(["fr.round_sk", "dlh.layout_hole_sk"]) }} as round_hole_sk,
        fr.round_sk,
        dlh.layout_hole_sk,
        se.hole_number, 
        se.hole_strokes as strokes,
        (((se.hole_number - fs.starting_hole + dl.hole_count) % dl.hole_count) + 1) as play_order_number,
        dateadd(minute, (duration_minutes / dl.hole_count) * play_order_number, fs.start_date) as estimated_start_date,
        se.entry_created_at as created_at,
        se.entry_updated_at as updated_at
    
    from {{ ref('scorecard_entries') }} se
    join {{ ref('fct_round') }} fr on se.entry_id = fr.round_id
    join {{ ref('fct_scorecard') }} fs on fr.scorecard_sk = fs.scorecard_sk
    join {{ ref('dim_layout') }} dl on fs.layout_sk = dl.layout_sk
    join {{ ref('dim_layout_hole') }} dlh on dl.layout_sk = dlh.layout_sk
        and se.hole_number = dlh.hole_number
    
    group by all
)

select
    round_hole_sk,
    round_sk,
    layout_hole_sk,
    hole_number,
    strokes,
    play_order_number,
    estimated_start_date,
    lead(estimated_start_date) over (partition by round_sk order by play_order_number) as estimated_end_date,
    created_at,
    updated_at

from round_holes

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}