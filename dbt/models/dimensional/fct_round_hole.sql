{{
  config(
    unique_key='round_hole_sk'
  )
}}

with round_holes_grouped as (
    select
        {{ dbt_utils.generate_surrogate_key(["fr.round_sk", "ch.hole_version_hash"]) }} as round_hole_sk,
        fr.round_sk,
        ch.hole_version_hash as hole_sk,
        fs.starting_hole,
        se.hole_number,
        se.hole_strokes,
        ch.hole_par,
        se.hole_strokes - ch.hole_par as hole_score,
        case
            when se.hole_strokes - ch.hole_par = 0 then 'E'
            when se.hole_strokes - ch.hole_par > 0 then '+' || cast(se.hole_strokes - ch.hole_par as varchar)
            else cast(se.hole_strokes - ch.hole_par as varchar)
        end as hole_score_display,
        case
            when se.hole_strokes = 1 then 'Ace'
            when se.hole_strokes - ch.hole_par = -4 then 'Condor'
            when se.hole_strokes - ch.hole_par = -3 then 'Albatross'
            when se.hole_strokes - ch.hole_par = -2 then 'Eagle'
            when se.hole_strokes - ch.hole_par = -1 then 'Birdie'
            when se.hole_strokes - ch.hole_par = 0 then 'Par'
            when se.hole_strokes - ch.hole_par = 1 then 'Bogey'
            when se.hole_strokes - ch.hole_par = 2 then 'Double Bogey'
            when se.hole_strokes - ch.hole_par = 3 then 'Triple Bogey'
            when se.hole_strokes - ch.hole_par = 4 then 'Quadruple Bogey+'
        end as hole_result,
        se.entry_created_at as created_at,
        se.entry_updated_at as updated_at

    from {{ ref('scorecard_entries') }} se
    join {{ ref('fct_round') }} fr on se.entry_id = fr.round_id
    join {{ ref('fct_scorecard') }} fs on fr.scorecard_sk = fs.scorecard_sk
    join {{ ref('dim_layout') }} dl on fs.layout_sk = dl.layout_sk
    join {{ ref('course_holes') }} ch on se.scorecard_id = ch.scorecard_id
        and ch.hole_number = se.hole_number
    where se.hole_strokes > 0
    group by all
),
round_holes as (
    select *
    from round_holes_grouped
    qualify row_number() over (partition by round_hole_sk order by updated_at desc) = 1
),

hole_count as (
    select
        rh.round_sk,
        count(distinct rh.hole_sk) as hole_count

    from round_holes rh
    group by rh.round_sk
)

select
    rh.round_hole_sk,
    rh.round_sk,
    rh.hole_sk,
    rh.hole_number,
    rh.hole_strokes,
    rh.hole_par,
    rh.hole_score,
    rh.hole_score_display,
    rh.hole_result,
    case 
        when hc.hole_count = 0 then null
        else (((rh.hole_number - rh.starting_hole + hc.hole_count) % hc.hole_count) + 1)
    end as play_order_number,
    rh.created_at,
    rh.updated_at

from round_holes rh
join hole_count hc on rh.round_sk = hc.round_sk

{% if is_incremental() %}
  where rh.updated_at > (select max(updated_at) from {{ this }})
{% endif %}