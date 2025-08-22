{{
  config(
    unique_key='throw_sk'
  )
}}

with player_or_team_id as (
    select
        se.entry_id,
        listagg(distinct se.player_id, '_') within group (order by se.player_id) as player_or_team_id

    from {{ ref('scorecard_entries') }} se
    group by se.entry_id
),

throw_data as (
    select
        frh.round_hole_sk,
        frh.hole_par,
        thr.throw_number,
        case
            when thr.landing_zone = 'teePad' then 'Tee Pad'
            when thr.landing_zone = 'ob' then 'Out of Bounds'
            when thr.landing_zone = 'offFairway' then 'Off Fairway'
            when thr.landing_zone = 'center' then 'Fairway'
            when thr.landing_zone = 'circle2' then 'Circle 2'
            when thr.landing_zone = 'circle1' then 'Circle 1'
            when thr.landing_zone = 'basket' then 'Basket'
        end as landing_spot,
        case
            when thr.landing_zone = 'teePad' then 'T'
            when thr.landing_zone = 'ob' then 'OB'
            when thr.landing_zone = 'offFairway' then 'OFF'
            when thr.landing_zone = 'center' then 'F'
            when thr.landing_zone = 'circle2' then 'C2'
            when thr.landing_zone = 'circle1' then 'C1'
            when thr.landing_zone = 'basket' then 'B'
        end as landing_spot_code,
        nullifzero(
            round(
                utils.meters_to_feet(thr.throw_distance)
            , 0)
        ) as distance,
        thr.created_at,
        thr.updated_at
    
    from {{ ref('throws') }} thr
    join {{ ref('fct_round') }} fr on thr.entry_id = fr.round_id
    join {{ ref('fct_round_hole') }} frh on fr.round_sk = frh.round_sk
        and thr.hole_number = frh.hole_number
    group by all
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['thr_to.round_hole_sk', 'thr_to.throw_number']) }} as throw_sk,
        thr_to.round_hole_sk,
        thr_to.throw_number,
        case
            when thr_to.throw_number = 1 then 'Ace'
            when thr_to.hole_par - thr_to.throw_number = 4 then 'Condor'
            when thr_to.hole_par - thr_to.throw_number = 3 then 'Albatross'
            when thr_to.hole_par - thr_to.throw_number = 2 then 'Eagle'
            when thr_to.hole_par - thr_to.throw_number = 1 then 'Birdie'
            when thr_to.hole_par - thr_to.throw_number = 0 then 'Par'
            when thr_to.hole_par - thr_to.throw_number = -1 then 'Bogey'
            when thr_to.hole_par - thr_to.throw_number = -2 then 'Double Bogey'
            when thr_to.hole_par - thr_to.throw_number = -3 then 'Triple Bogey'
            when thr_to.hole_par - thr_to.throw_number = -4 then 'Quadruple Bogey+'
        end as throw_for,
        coalesce(thr_from.landing_spot, 'Tee Pad') as throw_from,
        coalesce(thr_from.landing_spot_code, 'T') as throw_from_code,
        thr_to.landing_spot as throw_to,
        thr_to.landing_spot_code as throw_to_code,
        case
            when throw_from_code = 'T' then 'Teeshot'
            when throw_from_code = 'OB' then 'Penalty'
            when throw_from_code = 'OFF' then 'Scramble'
            when throw_from_code in ('C1','C2') then 'Putt'
            -- First Fairway throw on par 5 => Fairway
            when throw_from_code = 'F' and thr_to.hole_par - row_number() over (
                    partition by thr_to.round_hole_sk
                    order by case when throw_from_code = 'F' then 1 else 0 end desc, thr_to.throw_number
                    ) >= 4
                then 'Fairway'
            -- Other throws from Fairway => Approach
            when throw_from_code = 'F' then 'Approach'
        end as throw_type,
        thr_to.distance,
        case
            when throw_to_code = 'B'
            then case
                when throw_from_code = 'C1'
                    and thr_to.distance < 11
                    then 'Tap In'
                when throw_from_code = 'C1'
                    and thr_to.distance < 22
                    then 'C1 Middle'
                when throw_from_code = 'C1'
                    then 'C1 Long'
                when throw_from_code = 'C2'
                    and thr_to.distance < 44
                    then 'C2 Short'
                when throw_from_code = 'C2'
                    and thr_to.distance < 55
                    then 'C2 Middle'
                when throw_from_code = 'C2'
                    then 'C2 Long'
                else throw_from
            end
        end as made_from,
        thr_to.created_at,
        thr_to.updated_at

    from throw_data thr_to
    left join throw_data thr_from on thr_to.round_hole_sk = thr_from.round_hole_sk
        and thr_to.throw_number - 1  = thr_from.throw_number
)

select
    f.throw_sk,
    f.round_hole_sk,
    f.throw_number,
    f.throw_type,
    f.throw_for,
    f.throw_from,
    f.throw_to,
    f.distance,
    f.made_from,
    f.created_at,
    f.updated_at

from final f

{% if is_incremental() %}
  where f.updated_at > (select max(updated_at) from {{ this }})
{% endif %}