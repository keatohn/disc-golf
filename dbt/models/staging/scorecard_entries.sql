{{
  config(
    schema='STAGING'
  )
}}

with entries_flattened as (
    select
        sc.scorecard_id,
        sc.layout_id,
        sc.created_by_user_id,
        sc.start_date,
        sc.created_at,
        sc.updated_at,
        
        -- Entry level data
        entry.value:objectId::string as entry_id,
        convert_timezone('America/Los_Angeles', 'America/New_York', entry.value:createdAt::timestamp_ntz) as entry_created_at,
        convert_timezone('America/Los_Angeles', 'America/New_York', entry.value:updatedAt::timestamp_ntz) as entry_updated_at,
        entry.value:includeInHandicaps::boolean as include_in_handicaps,
        entry.value:includeInProfile::boolean as include_in_profile,
        entry.value:startingScore::number as starting_score,
        entry.value:roundRating::number as round_rating_udisc,
        
        -- Raw arrays for further processing
        entry.value:players as players,
        entry.value:users as users,
        entry.value:holeScores as hole_scores
        
    from {{ ref('scorecards') }} sc,
         lateral flatten(input => entries) entry
    where entries is not null
),

hole_scores_flattened as (
    select
        ef.scorecard_id,
        ef.entry_id,
        hole_score.value:strokes::number as hole_strokes,
        hole_score.value:changeVersion::number as hole_score_change_version,
        hole_score.value:holeThrows as hole_throws,
        row_number() over (partition by entry_id order by hole_score.index) as hole_number
        
    from entries_flattened ef,
         lateral flatten(input => hole_scores) hole_score
    where hole_scores is not null
),

players_flattened as (
    select
        ef.scorecard_id,
        ef.entry_id,
        player.value:objectId::string as player_id,
        null as player_full_name,
        null as player_first_name,
        null as player_last_name,
        player.value:name::string as player_display_name,
        null as player_username,
        false as player_is_udisc_user,
        coalesce(player.value:isDeleted::boolean, false) as player_is_deleted,
        convert_timezone('America/Los_Angeles', 'America/New_York', player.value:createdAt::timestamp_ntz) as player_created_at,
        convert_timezone('America/Los_Angeles', 'America/New_York', player.value:updatedAt::timestamp_ntz) as player_updated_at
        
    from entries_flattened ef,
         lateral flatten(input => players) player
    where hole_scores is not null

    union all

    select
        ef.scorecard_id,
        ef.entry_id,
        user.value:objectId::string as player_id,
        coalesce(user.value:fullName::string, user.value:name::string) as player_full_name,
        split_part(player_full_name, ' ', 1) as player_first_name,
        split_part(player_full_name, ' ', 2) as player_last_name,
        user.value:name::string as player_display_name,
        user.value:username::string as player_username,
        true as player_is_udisc_user,
        false as player_is_deleted,
        convert_timezone('America/Los_Angeles', 'America/New_York', user.value:createdAt::timestamp_ntz) as player_created_at,
        convert_timezone('America/Los_Angeles', 'America/New_York', user.value:updatedAt::timestamp_ntz) as player_updated_at
        
    from entries_flattened ef,
         lateral flatten(input => users) as user
    where hole_scores is not null
),

final as (
    select
        ef.scorecard_id,
        ef.layout_id,
        ef.entry_id,
        ef.entry_created_at,
        ef.entry_updated_at,
        ef.include_in_handicaps,
        ef.include_in_profile,
        ef.starting_score,
        ef.round_rating_udisc,

        hsc.hole_strokes,
        hsc.hole_score_change_version,
        hsc.hole_throws,
        hsc.hole_number,
        
        pl.player_id,
        pl.player_full_name,
        pl.player_first_name,
        pl.player_last_name,
        pl.player_display_name,
        pl.player_username,
        pl.player_is_udisc_user,
        pl.player_is_deleted,
        pl.player_created_at,
        pl.player_updated_at,

        ef.created_by_user_id,
        ef.start_date,
        ef.created_at,
        ef.updated_at
        
    from entries_flattened ef
    left join hole_scores_flattened hsc on ef.scorecard_id = hsc.scorecard_id
        and ef.entry_id = hsc.entry_id
    left join players_flattened pl on ef.scorecard_id = pl.scorecard_id
        and ef.entry_id = pl.entry_id
)

select * from final