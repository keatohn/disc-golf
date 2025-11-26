{{
  config(
    materialized='table',
    schema='analytics'
  )
}}

-- Base rounds model for analytics dashboard
-- This is a denormalized view with all round details including hole-by-hole scores
-- Percentages and averages should be calculated in the frontend after filtering

with round_holes_pivoted as (
    select
        fr.round_sk,
        max(case when frh.hole_number = 1 then frh.hole_strokes end) as hole_1,
        max(case when frh.hole_number = 2 then frh.hole_strokes end) as hole_2,
        max(case when frh.hole_number = 3 then frh.hole_strokes end) as hole_3,
        max(case when frh.hole_number = 4 then frh.hole_strokes end) as hole_4,
        max(case when frh.hole_number = 5 then frh.hole_strokes end) as hole_5,
        max(case when frh.hole_number = 6 then frh.hole_strokes end) as hole_6,
        max(case when frh.hole_number = 7 then frh.hole_strokes end) as hole_7,
        max(case when frh.hole_number = 8 then frh.hole_strokes end) as hole_8,
        max(case when frh.hole_number = 9 then frh.hole_strokes end) as hole_9,
        max(case when frh.hole_number = 10 then frh.hole_strokes end) as hole_10,
        max(case when frh.hole_number = 11 then frh.hole_strokes end) as hole_11,
        max(case when frh.hole_number = 12 then frh.hole_strokes end) as hole_12,
        max(case when frh.hole_number = 13 then frh.hole_strokes end) as hole_13,
        max(case when frh.hole_number = 14 then frh.hole_strokes end) as hole_14,
        max(case when frh.hole_number = 15 then frh.hole_strokes end) as hole_15,
        max(case when frh.hole_number = 16 then frh.hole_strokes end) as hole_16,
        max(case when frh.hole_number = 17 then frh.hole_strokes end) as hole_17,
        max(case when frh.hole_number = 18 then frh.hole_strokes end) as hole_18,
        max(case when frh.hole_number = 19 then frh.hole_strokes end) as hole_19,
        max(case when frh.hole_number = 20 then frh.hole_strokes end) as hole_20,
        max(case when frh.hole_number = 21 then frh.hole_strokes end) as hole_21,
        max(case when frh.hole_number = 22 then frh.hole_strokes end) as hole_22,
        max(case when frh.hole_number = 23 then frh.hole_strokes end) as hole_23,
        max(case when frh.hole_number = 24 then frh.hole_strokes end) as hole_24,
        max(case when frh.hole_number = 25 then frh.hole_strokes end) as hole_25,
        max(case when frh.hole_number = 26 then frh.hole_strokes end) as hole_26,
        max(case when frh.hole_number = 27 then frh.hole_strokes end) as hole_27,
        -- Front 9 and back 9 scores relative to par
        sum(case when frh.hole_number between 1 and 9 then frh.hole_score else 0 end) as front_9,
        sum(case when frh.hole_number between 10 and 18 then frh.hole_score else 0 end) as back_9,
        -- Count holes played
        count(distinct frh.hole_number) as holes_played
    from {{ ref('fct_round') }} fr
    left join {{ ref('fct_round_hole') }} frh on fr.round_sk = frh.round_sk
    group by fr.round_sk
),

round_hole_results_pivoted as (
    select
        fr.round_sk,
        max(case when frh.hole_number = 1 then frh.hole_result end) as result_1,
        max(case when frh.hole_number = 2 then frh.hole_result end) as result_2,
        max(case when frh.hole_number = 3 then frh.hole_result end) as result_3,
        max(case when frh.hole_number = 4 then frh.hole_result end) as result_4,
        max(case when frh.hole_number = 5 then frh.hole_result end) as result_5,
        max(case when frh.hole_number = 6 then frh.hole_result end) as result_6,
        max(case when frh.hole_number = 7 then frh.hole_result end) as result_7,
        max(case when frh.hole_number = 8 then frh.hole_result end) as result_8,
        max(case when frh.hole_number = 9 then frh.hole_result end) as result_9,
        max(case when frh.hole_number = 10 then frh.hole_result end) as result_10,
        max(case when frh.hole_number = 11 then frh.hole_result end) as result_11,
        max(case when frh.hole_number = 12 then frh.hole_result end) as result_12,
        max(case when frh.hole_number = 13 then frh.hole_result end) as result_13,
        max(case when frh.hole_number = 14 then frh.hole_result end) as result_14,
        max(case when frh.hole_number = 15 then frh.hole_result end) as result_15,
        max(case when frh.hole_number = 16 then frh.hole_result end) as result_16,
        max(case when frh.hole_number = 17 then frh.hole_result end) as result_17,
        max(case when frh.hole_number = 18 then frh.hole_result end) as result_18,
        max(case when frh.hole_number = 19 then frh.hole_result end) as result_19,
        max(case when frh.hole_number = 20 then frh.hole_result end) as result_20,
        max(case when frh.hole_number = 21 then frh.hole_result end) as result_21,
        max(case when frh.hole_number = 22 then frh.hole_result end) as result_22,
        max(case when frh.hole_number = 23 then frh.hole_result end) as result_23,
        max(case when frh.hole_number = 24 then frh.hole_result end) as result_24,
        max(case when frh.hole_number = 25 then frh.hole_result end) as result_25,
        max(case when frh.hole_number = 26 then frh.hole_result end) as result_26,
        max(case when frh.hole_number = 27 then frh.hole_result end) as result_27
    from {{ ref('fct_round') }} fr
    left join {{ ref('fct_round_hole') }} frh on fr.round_sk = frh.round_sk
    group by fr.round_sk
),

round_results as (
    select
        frh.round_sk,
        count(case when frh.hole_result = 'Ace' then 1 end) as aces,
        count(case when frh.hole_result = 'Eagle' then 1 end) as eagles,
        count(case when frh.hole_result = 'Birdie' then 1 end) as birdies,
        count(case when frh.hole_result = 'Par' then 1 end) as pars,
        count(case when frh.hole_result = 'Bogey' then 1 end) as bogeys,
        count(case when frh.hole_result = 'Double Bogey' then 1 end) as doubles,
        count(case when frh.hole_result = 'Triple Bogey' then 1 end) as triples,
        count(case when frh.hole_result = 'Quadruple Bogey+' then 1 end) as quads_plus
    from {{ ref('fct_round_hole') }} frh
    group by frh.round_sk
),

hole_stats_aggregated as (
    select
        fr.round_sk,
        sum(hr."Fairway Hits") as "Fairway Hits",
        sum(hr."Fairway Attempts") as "Fairway Attempts",
        sum(hr."GIR C2") as "GIR C2",
        sum(hr."GIR C1") as "GIR C1",
        sum(hr."Parked") as "Parked",
        sum(hr."C1 Putts Made") as "C1 Putts Made",
        sum(hr."C1 Putts Attempted") as "C1 Putts Attempted",
        sum(hr."C1X Putts Made") as "C1X Putts Made",
        sum(hr."C1X Putts Attempted") as "C1X Putts Attempted",
        sum(hr."C2 Putts Made") as "C2 Putts Made",
        sum(hr."C2 Putts Attempted") as "C2 Putts Attempted"
    from {{ ref('fct_round') }} fr
    left join {{ ref('hole_results') }} hr on fr.round_sk = hr."Round SK"
    group by fr.round_sk
)

select
    fr.round_sk as "Round SK",
    
    -- Player/Team info
    coalesce(dp.player_sk, dt.team_sk) as "Player SK",
    coalesce(dp.full_name, dt.team_name) as "Player",
    dp.username as "Username",
    dp.is_udisc_user as "Is UDisc User",
    
    -- Course/Layout info
    dc.course_sk as "Course SK",
    dc.course_name as "Course Name",
    dl.layout_sk as "Layout SK",
    dl.layout_full_name as "Course Layout Name",
    dl.layout_name as "Layout Name",
    dl.hole_count as "Hole Count",
    
    -- Round metadata
    fr.round_format as "Format",
    fsc.start_date as "Date",
    case date_part('month', fsc.start_date)
        when 1 then 'Jan'
        when 2 then 'Feb'
        when 3 then 'Mar'
        when 4 then 'Apr'
        when 5 then 'May'
        when 6 then 'Jun'
        when 7 then 'Jul'
        when 8 then 'Aug'
        when 9 then 'Sep'
        when 10 then 'Oct'
        when 11 then 'Nov'
        when 12 then 'Dec'
    end || ' ' || cast(date_part('day', fsc.start_date) as varchar) || ', ' || cast(date_part('year', fsc.start_date) as varchar) as "Date Formatted",
    date_part('year', fsc.start_date) as "Year",
    date_part('month', fsc.start_date) as "Month",
    date_part('day', fsc.start_date) as "Day",
    
    -- Round scores
    fr.round_strokes as "Strokes",
    fr.round_par as "Par",
    fr.round_score as "Score",
    fr.round_score_display as "Score Display",
    round(fr.round_rating_udisc, 0) as "UDisc Rating",
    fr.round_starting_score as "Starting Score",
    
    -- Front/Back 9
    rhp.front_9 as "Front 9",
    rhp.back_9 as "Back 9",
    rhp.holes_played as "Holes Played",
    
    -- Hole-by-hole scores (for display) - just numbers
    rhp.hole_1 as "1", rhp.hole_2 as "2", rhp.hole_3 as "3", rhp.hole_4 as "4", rhp.hole_5 as "5", rhp.hole_6 as "6",
    rhp.hole_7 as "7", rhp.hole_8 as "8", rhp.hole_9 as "9", rhp.hole_10 as "10", rhp.hole_11 as "11", rhp.hole_12 as "12",
    rhp.hole_13 as "13", rhp.hole_14 as "14", rhp.hole_15 as "15", rhp.hole_16 as "16", rhp.hole_17 as "17", rhp.hole_18 as "18",
    rhp.hole_19 as "19", rhp.hole_20 as "20", rhp.hole_21 as "21", rhp.hole_22 as "22", rhp.hole_23 as "23", rhp.hole_24 as "24",
    rhp.hole_25 as "25", rhp.hole_26 as "26", rhp.hole_27 as "27",
    
    -- Hole-by-hole results (for color coding)
    rhr.result_1 as "Result 1", rhr.result_2 as "Result 2", rhr.result_3 as "Result 3", rhr.result_4 as "Result 4", rhr.result_5 as "Result 5", rhr.result_6 as "Result 6",
    rhr.result_7 as "Result 7", rhr.result_8 as "Result 8", rhr.result_9 as "Result 9", rhr.result_10 as "Result 10", rhr.result_11 as "Result 11", rhr.result_12 as "Result 12",
    rhr.result_13 as "Result 13", rhr.result_14 as "Result 14", rhr.result_15 as "Result 15", rhr.result_16 as "Result 16", rhr.result_17 as "Result 17", rhr.result_18 as "Result 18",
    rhr.result_19 as "Result 19", rhr.result_20 as "Result 20", rhr.result_21 as "Result 21", rhr.result_22 as "Result 22", rhr.result_23 as "Result 23", rhr.result_24 as "Result 24",
    rhr.result_25 as "Result 25", rhr.result_26 as "Result 26", rhr.result_27 as "Result 27",
    
    -- Result counts (for calculating percentages in frontend)
    coalesce(rr.aces, 0) as "Aces",
    coalesce(rr.eagles, 0) as "Eagles",
    coalesce(rr.birdies, 0) as "Birdies",
    coalesce(rr.pars, 0) as "Pars",
    coalesce(rr.bogeys, 0) as "Bogeys",
    coalesce(rr.doubles, 0) as "Doubles",
    coalesce(rr.triples, 0) as "Triples",
    coalesce(rr.quads_plus, 0) as "Quads+",
    
    -- Throwing stats (from hole_results)
    coalesce(hsa."Fairway Hits", 0) as "Fairway Hits",
    coalesce(hsa."Fairway Attempts", 0) as "Fairway Attempts",
    coalesce(hsa."GIR C2", 0) as "GIR C2",
    coalesce(hsa."GIR C1", 0) as "GIR C1",
    coalesce(hsa."Parked", 0) as "Parked",
    coalesce(hsa."C1 Putts Made", 0) as "C1 Putts Made",
    coalesce(hsa."C1 Putts Attempted", 0) as "C1 Putts Attempted",
    coalesce(hsa."C1X Putts Made", 0) as "C1X Putts Made",
    coalesce(hsa."C1X Putts Attempted", 0) as "C1X Putts Attempted",
    coalesce(hsa."C2 Putts Made", 0) as "C2 Putts Made",
    coalesce(hsa."C2 Putts Attempted", 0) as "C2 Putts Attempted",
    
    -- Timestamps
    fr.created_at as "Created Date",
    fr.updated_at as "Updated Date"

from {{ ref('fct_round') }} fr
inner join {{ ref('fct_scorecard') }} fsc on fr.scorecard_sk = fsc.scorecard_sk
left join {{ ref('dim_player') }} dp on fr.player_sk = dp.player_sk
left join {{ ref('dim_team') }} dt on fr.team_sk = dt.team_sk
left join {{ ref('dim_layout') }} dl on fsc.layout_sk = dl.layout_sk
left join {{ ref('dim_course') }} dc on dl.course_sk = dc.course_sk
left join round_holes_pivoted rhp on fr.round_sk = rhp.round_sk
left join round_hole_results_pivoted rhr on fr.round_sk = rhr.round_sk
left join round_results rr on fr.round_sk = rr.round_sk
left join hole_stats_aggregated hsa on fr.round_sk = hsa.round_sk

where fsc.is_finished = true