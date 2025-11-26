{{
  config(
    materialized='table',
    schema='analytics'
  )
}}

with throwing_stats as (

    select
        rh.round_hole_sk,
        sum(
            case
                when rh.hole_par <= 3 and thr.throw_number = 1 and thr.throw_to not in ('Out of Bounds', 'Off Fairway') then 1
                when rh.hole_par = 4
                    and thr.throw_number = 1
                    and thr.throw_type in ('Teeshot', 'Fairway', 'Approach')
                    and thr.throw_to not in ('Out of Bounds', 'Off Fairway')
                    then 1
                when rh.hole_par = 4
                    and thr.throw_number = 2
                    and thr.throw_type in ('Teeshot', 'Fairway', 'Approach')
                    and thr.throw_to in ('Circle 2', 'Circle 1', 'Basket')
                    then 1
                when rh.hole_par >= 5
                    and thr.throw_number between 1 and 2
                    and thr.throw_type in ('Teeshot', 'Fairway', 'Approach')
                    and thr.throw_to not in ('Out of Bounds', 'Off Fairway')
                    then 1
                when rh.hole_par >= 5
                    and thr.throw_number = 3
                    and thr.throw_type in ('Teeshot', 'Fairway', 'Approach')
                    and thr.throw_to in ('Circle 2', 'Circle 1', 'Basket')
                    then 1
                else 0
            end
        ) as fairway_hits,
        sum(
            case
                when rh.hole_par <= 3 and thr.throw_number = 1 then 1
                when rh.hole_par = 4 and thr.throw_number between 1 and 2 and thr.throw_type in ('Teeshot', 'Fairway', 'Approach') then 1
                when rh.hole_par >= 5 and thr.throw_number between 1 and 3 and thr.throw_type in ('Teeshot', 'Fairway', 'Approach') then 1
                else 0
            end
        ) as fairway_attempts,
        sum(
            case
                when thr.throw_number < rh.hole_par
                    and (thr.throw_from in ('Circle 2', 'Circle 1')
                            or thr.throw_to = 'Basket')
                    then 1
                else 0
            end
        ) as gir_c2,
        sum(
            case
                when thr.throw_number < rh.hole_par
                    and (thr.throw_from = 'Circle 1'
                            or thr.throw_to = 'Basket')
                    then 1
                else 0
            end
        ) as gir_c1,
        sum(
            case
                when thr.throw_number < rh.hole_par
                    and thr.made_from = 'Tap In'
                    then 1
                else 0
            end
        ) as parked,
        sum(case when thr.throw_from = 'Circle 1' and thr.throw_to = 'Basket' then 1 else 0 end) as c1_putts_made,
        sum(case when thr.throw_from = 'Circle 1' then 1 else 0 end) as c1_putts_attempted,
        sum(case when thr.throw_from = 'Circle 1' and thr.throw_to = 'Basket' and thr.made_from != 'Tap In' then 1 else 0 end) as c1x_putts_made,
        sum(case when thr.throw_from = 'Circle 1' and (thr.made_from IS DISTINCT FROM 'Tap In') then 1 else 0 end) as c1x_putts_attempted,
        sum(case when thr.throw_from = 'Circle 2' and thr.throw_to = 'Basket' then 1 else 0 end) as c2_putts_made,
        sum(case when thr.throw_from = 'Circle 2' then 1 else 0 end) as c2_putts_attempted,
        max_by(thr.distance, thr.throw_number) as distance_made_from,
        max_by(thr.made_from, thr.throw_number) as zone_made_from

    from {{ ref('fct_round_hole') }} rh
    join {{ ref('fct_throw') }} thr on rh.round_hole_sk = thr.round_hole_sk

    group by rh.round_hole_sk

)

select
    sc.scorecard_sk as "Scorecard SK",
    sc.start_date as "Scorecard Date",
    rnd.round_sk as "Round SK",
    crs.course_sk as "Course SK",
    crs.course_name as "Course Name",
    lay.layout_sk as "Layout SK",
    lay.layout_full_name as "Course Layout Name",
    coalesce(pl.player_sk, tm.team_sk) as "Competitor SK",
    coalesce(pl.first_name, pl.display_name, tm.team_name) as "Competitor Name",
    hl.hole_sk as "Hole SK",
    rh.hole_number as "Hole Number",
    rh.hole_par as "Hole Par",
    rh.hole_strokes as "Hole Strokes",
    rh.hole_score as "Hole Score",
    rh.hole_score_display as "Hole Score Display",
    rh.hole_result as "Hole Result",
    hl.distance as "Hole Distance",
    thst.round_hole_sk is not null as "Has Throwing Stats",
    thst.fairway_hits as "Fairway Hits",
    thst.fairway_attempts as "Fairway Attempts",
    thst.gir_c2 as "GIR C2",
    thst.gir_c1 as "GIR C1",
    thst.parked as "Parked",
    thst.c1_putts_made as "C1 Putts Made",
    thst.c1_putts_attempted as "C1 Putts Attempted",
    thst.c1x_putts_made as "C1X Putts Made",
    thst.c1x_putts_attempted as "C1X Putts Attempted",
    thst.c2_putts_made as "C2 Putts Made",
    thst.c2_putts_attempted as "C2 Putts Attempted",
    thst.distance_made_from as "Distance Made From",
    thst.zone_made_from as "Zone Made From"

from {{ ref('fct_scorecard') }} sc
join {{ ref('dim_layout') }} lay on sc.layout_sk = lay.layout_sk
join {{ ref('dim_course') }} crs on lay.course_sk = crs.course_sk
join {{ ref('fct_round') }} rnd on sc.scorecard_sk = rnd.scorecard_sk
left join {{ ref('dim_player') }} pl on rnd.player_sk = pl.player_sk
left join {{ ref('dim_team') }} tm on rnd.team_sk = tm.team_sk
join {{ ref('fct_round_hole') }} rh on rnd.round_sk = rh.round_sk
join {{ ref('dim_hole') }} hl on rh.hole_sk = hl.hole_sk
left join throwing_stats thst on rh.round_hole_sk = thst.round_hole_sk

where not sc.is_deleted