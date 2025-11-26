{{
  config(
    schema='staging'
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
        json_extract_string(entry, '$.objectId') as entry_id,
        timezone('America/New_York', cast(json_extract_string(entry, '$.createdAt') as timestamp)) as entry_created_at,
        timezone('America/New_York', cast(json_extract_string(entry, '$.updatedAt') as timestamp)) as entry_updated_at,
        cast(json_extract(entry, '$.includeInHandicaps') as boolean) as include_in_handicaps,
        cast(json_extract(entry, '$.includeInProfile') as boolean) as include_in_profile,
        cast(json_extract(entry, '$.startingScore') as integer) as starting_score,
        cast(json_extract(entry, '$.roundRating') as double) as round_rating_udisc,
        
        -- Raw arrays for further processing
        json_extract(entry, '$.players') as players,
        json_extract(entry, '$.users') as users,
        json_extract(entry, '$.holeScores') as hole_scores
        
    from {{ ref('scorecards') }} sc,
         unnest(json_extract(sc.entries, '$')::json[]) as t(entry)
    where entries is not null
),

hole_scores_flattened as (
    select
        ef.scorecard_id,
        ef.entry_id,
        cast(json_extract(hole_score, '$.strokes') as integer) as hole_strokes,
        cast(json_extract(hole_score, '$.changeVersion') as integer) as hole_score_change_version,
        json_extract(hole_score, '$.holeThrows') as hole_throws,
        row_number() over (partition by entry_id order by ordinality) as hole_number
        
    from entries_flattened ef,
         unnest(json_extract(ef.hole_scores, '$')::json[]) with ordinality as t(hole_score, ordinality)
    where hole_scores is not null
),

players_flattened as (
    select
        ef.scorecard_id,
        ef.entry_id,
        json_extract_string(player, '$.objectId') as player_id,
        null as player_full_name,
        null as player_first_name,
        null as player_last_name,
        json_extract_string(player, '$.name') as player_display_name,
        null as player_username,
        false as player_is_udisc_user,
        coalesce(cast(json_extract(player, '$.isDeleted') as boolean), false) as player_is_deleted,
        timezone('America/New_York', cast(json_extract_string(player, '$.createdAt') as timestamp)) as player_created_at,
        timezone('America/New_York', cast(json_extract_string(player, '$.updatedAt') as timestamp)) as player_updated_at
        
    from entries_flattened ef,
         unnest(json_extract(ef.players, '$')::json[]) as t(player)
    where hole_scores is not null

    union all

    select
        ef.scorecard_id,
        ef.entry_id,
        json_extract_string(user, '$.objectId') as player_id,
        coalesce(json_extract_string(user, '$.fullName'), json_extract_string(user, '$.name')) as player_full_name,
        split_part(player_full_name, ' ', 1) as player_first_name,
        split_part(player_full_name, ' ', 2) as player_last_name,
        json_extract_string(user, '$.name') as player_display_name,
        json_extract_string(user, '$.username') as player_username,
        true as player_is_udisc_user,
        false as player_is_deleted,
        timezone('America/New_York', cast(json_extract_string(user, '$.createdAt') as timestamp)) as player_created_at,
        timezone('America/New_York', cast(json_extract_string(user, '$.updatedAt') as timestamp)) as player_updated_at
        
    from entries_flattened ef,
         unnest(json_extract(ef.users, '$')::json[]) as t(user)
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