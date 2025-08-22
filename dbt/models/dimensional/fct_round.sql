{{
  config(
    unique_key='round_sk'
  )
}}

with player_or_team_id as (
    select
        se.entry_id,
        listagg(distinct se.player_id, '_') within group (order by se.player_id) as player_or_team_id

    from {{ ref('scorecard_entries') }} se
    group by se.entry_id
),

round_par as (
    select
        ch.scorecard_id,
        sum(ch.hole_par) as round_par
    from {{ ref('course_holes') }} ch
    group by ch.scorecard_id
),

hole_strokes as (
    select
        se.entry_id,
        se.hole_number,
        max(se.hole_strokes) as hole_strokes
    from {{ ref('scorecard_entries') }} se
    group by se.entry_id, se.hole_number
),

round_strokes as (
    select
        hs.entry_id,
        sum(hs.hole_strokes) as round_strokes
    from hole_strokes hs
    group by hs.entry_id
),

rounds as (
  select
      se.entry_id as round_id,
      {{ dbt_utils.generate_surrogate_key(["round_id"]) }} as round_sk,
      fsc.scorecard_sk,
      dp.player_sk,
      dt.team_sk,
      case
        when dp.player_sk is not null then 'Singles'
        else dt.team_type
      end as round_type,
      se.starting_score as round_starting_score,
      rs.round_strokes,
      rp.round_par,
      round_starting_score + (round_strokes - rp.round_par) as round_score,
      case
          when round_score = 0 then 'E'
          when round_score > 0 then '+' || to_varchar(round_score)
          else to_varchar(round_score)
      end as round_score_display,
      se.round_rating_udisc,
      se.entry_created_at as created_at,
      se.entry_updated_at as updated_at

  from {{ ref('scorecard_entries') }} se
  join player_or_team_id ptid on se.entry_id = ptid.entry_id
  join {{ ref('fct_scorecard') }} fsc on se.scorecard_id = fsc.scorecard_id
  join round_par rp on se.scorecard_id = rp.scorecard_id
  join round_strokes rs on se.entry_id = rs.entry_id
  left join {{ ref('dim_player') }} dp on ptid.player_or_team_id = dp.player_id
  left join {{ ref('dim_team') }} dt on ptid.player_or_team_id = dt.team_id
  group by all
  qualify row_number() over (partition by round_sk order by se.entry_updated_at desc) = 1
)

select
    r.round_sk,
    r.round_id,
    r.scorecard_sk,
    r.player_sk,
    r.team_sk,
    r.round_type,
    r.round_starting_score,
    r.round_strokes,
    r.round_par,
    r.round_score,
    r.round_score_display,
    r.round_rating_udisc,
    r.created_at,
    r.updated_at

from rounds r

{% if is_incremental() %}
  where r.updated_at > (select max(updated_at) from {{ this }})
{% endif %}