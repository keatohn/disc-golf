# Analytics Models Needed for Streamlit Dashboard

This document outlines the analytics schema models needed to support all dashboard tabs.

## Required Analytics Models

### 1. Monthly Summary
**Model:** `analytics.monthly_summary`
**Purpose:** Monthly aggregated stats per player
**Key Fields:**
- `month`, `year`
- `player_name`
- `player_rating`, `rating_change`
- `rounds`, `avg_rating`, `best_rating`
- `new_records`, `aces`
- `eagle_pct`, `birdie_pct`, `par_pct`, `bogey_pct`, `double_pct`, `triple_plus_pct`
- `bogeyless_rounds`, `turkeys`

### 2. All Rounds
**Model:** `analytics.all_rounds`
**Purpose:** Detailed round data with all filters
**Key Fields:**
- `round_id`, `player_name`, `course_layout_name`, `date`
- `score`, `rating`
- `hole_1` through `hole_18` (individual hole scores)
- `format` (Singles/Doubles)
- `front_9_score`, `back_9_score`
- All fields needed for filtering

### 3. Course Records
**Model:** `analytics.course_records`
**Purpose:** Best scores/ratings per player per course layout
**Key Fields:**
- `course_layout_name`, `player_name`
- `best_score`, `best_rating`
- `rounds_played`
- `format`
- `state`, `retired_layout` (boolean)

### 4. Player Stats
**Model:** `analytics.player_stats`
**Purpose:** Comprehensive player statistics
**Key Fields:**
- `player_name`
- `rounds`, `holes`, `strokes`
- `courses_played`
- `avg_score`, `avg_rating`, `median_rating`
- `best_rating`, `worst_rating`, `stdev_rating`
- `mpo_rounds_pct`, `ma1_rounds_pct`, `ma2_rounds_pct`, `ma3_rounds_pct`
- All other career stats

### 5. Player Rating History
**Model:** `analytics.player_rating_history`
**Purpose:** Time series of player ratings
**Key Fields:**
- `player_name`, `date`
- `rating`
- Used for rating history charts

### 6. Course Stats
**Model:** `analytics.course_stats`
**Purpose:** Aggregate statistics per course layout
**Key Fields:**
- `course_layout_name`
- `holes`, `par`, `difficulty`
- `rounds`, `players`
- `avg_score`, `median_score`, `best_score`, `worst_score`
- `best_f9`, `avg_f9`, `best_b9`, `avg_b9`
- `aces`, `eagle_pct`, `birdie_pct`, `par_pct`, `bogey_pct`, `double_pct`, `triple_plus_pct`
- `bogeyless_pct`, `bounce_back_pct`, `avg_turkeys`

### 7. Course Rounds
**Model:** `analytics.course_rounds`
**Purpose:** Ranked rounds for a specific course
**Key Fields:**
- `rank`, `player_name`, `course_layout_name`, `date`
- `score`, `rating`
- `hole_1` through `hole_18`

### 8. Hole Statistics
**Model:** `analytics.hole_statistics`
**Purpose:** Per-hole statistics
**Key Fields:**
- `course_layout_name`, `hole_number`
- `avg_score_vs_par`
- `par`, `difficulty`
- Other hole-level metrics

### 9. Hole Result Distribution
**Model:** `analytics.hole_result_distribution`
**Purpose:** Distribution of scores per hole
**Key Fields:**
- `course_layout_name`, `hole_number`
- `eagle_count`, `birdie_count`, `par_count`, `bogey_count`, `double_count`, `triple_plus_count`
- Percentages for each result type

### 10. Player Stats Summary
**Model:** `analytics.player_stats_summary`
**Purpose:** Wide table format for stats tables view
**Key Fields:**
- Rows: Various metrics (Rounds, Holes, Strokes, Courses, Avg Score, etc.)
- Columns: Players (pivoted)
- Filterable by course, date, etc.

### 11. Head-to-Head
**Model:** `analytics.head_to_head`
**Purpose:** Comparison between two players
**Key Fields:**
- `player1_name`, `player2_name`
- `rounds_together`, `player1_wins`, `ties`, `player2_wins`
- `player1_win_pct`, `player2_win_pct`
- `player1_total_strokes`, `player2_total_strokes`
- `player1_avg_score`, `player2_avg_score`
- `player1_avg_rating`, `player2_avg_rating`
- Filterable by course, date, format

### 12. Power Scores
**Model:** `analytics.power_scores`
**Purpose:** Best possible score per player per course (sum of best hole scores)
**Key Fields:**
- `player_name`, `course_layout_name`
- `power_score` (sum of best score on each hole)
- `rounds_played`
- `hole_1_best`, `hole_2_best`, ..., `hole_18_best` (for drill-down)

### 13. Turkeys
**Model:** `analytics.turkeys`
**Purpose:** Count of 3 consecutive birdies
**Key Fields:**
- `entity_name` (course layout or player name, depending on grouping)
- `entity_type` ('layout' or 'player')
- `holes_1_3`, `holes_2_4`, `holes_3_5`, ..., `holes_16_18` (counts for each 3-hole window)
- Filterable by course, player, month/year

### 14. Bounce Backs
**Model:** `analytics.bounce_backs`
**Purpose:** Count of birdies following bogeys
**Key Fields:**
- `entity_name`, `entity_type`
- `hole_2`, `hole_3`, ..., `hole_18` (counts for bounce backs on each hole)
- Filterable by course, player, month/year

### 15. Hole Streaks
**Model:** `analytics.hole_streaks`
**Purpose:** Various streak types (birdie, par, bogey, bogey-free, etc.)
**Key Fields:**
- `player_name`
- `streak_type` ('total', 'course', 'hole')
- `course_layout_name` (if course-specific)
- `hole_number` (if hole-specific)
- `long_birdie_streak`, `long_par_streak`, `long_bogey_streak`
- `current_bogey_free`, `long_bogey_free`
- `current_double_free`, `long_double_free`
- `current_triple_free`, `long_triple_free`
- `as_of_date` (for current streaks)

### 16. Golden Birdie Stats
**Model:** `analytics.golden_birdie_stats`
**Purpose:** Aggregate stats for golden birdies per hole
**Key Fields:**
- `course_layout_name`, `hole_number`
- `golden_birdies` (count of times hole has been birdied N times where N = hole number)
- `birdie_rate` (percentage)
- `avg_rounds_for_golden_birdie`
- `fewest_rounds_for_golden_birdie`

### 17. Golden Birdie Tracker
**Model:** `analytics.golden_birdie_tracker`
**Purpose:** Per-player tracking of golden birdie progress
**Key Fields:**
- `player_name`, `course_layout_name`, `hole_number`
- `birdie_count` (how many times player has birdied this hole)
- `status` ('Golden Birdie', 'One Birdie Away', 'More Than One Birdie Away')
- `rounds_played`

## Implementation Notes

1. **Historic Ratings**: This is a calculated metric that needs to be developed. It should track player rating over time, likely calculated from round ratings.

2. **Power Scores**: This is a calculated metric - the sum of a player's best score on each hole across all rounds at a course. Requires aggregating hole-level best scores.

3. **All models should be in the `analytics` schema** as configured in `dbt_project.yml`

4. **Models should be materialized as tables** for performance in the dashboard

5. **Consider incremental models** for large tables that are frequently updated

6. **Filter-friendly design**: Include all fields needed for filtering in each model

