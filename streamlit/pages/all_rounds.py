"""All Rounds - round search with filters"""

import streamlit as st
import pandas as pd


def show_all_rounds(conn):
    """All Rounds - round search with filters"""
    # Get available options for filters first
    try:
        players = conn.execute(
            'SELECT DISTINCT "Player" FROM analytics.rounds WHERE "Player" IS NOT NULL ORDER BY "Player"').fetchall()
        player_options = [p[0] for p in players] if players else []
    except:
        player_options = []

    try:
        courses = conn.execute(
            'SELECT DISTINCT "Course Name" FROM analytics.rounds WHERE "Course Name" IS NOT NULL ORDER BY "Course Name"').fetchall()
        course_options = [c[0] for c in courses] if courses else []
    except:
        course_options = []

    # Filters
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        player_filter = st.multiselect(
            "Player", player_options if player_options else [])
    with col2:
        format_filter = st.multiselect(
            "Format", ["Singles", "Doubles"], default=[])
    with col3:
        course_filter = st.multiselect(
            "Course", course_options if course_options else [], key="course_filter")

    # Layout filter depends on course selection - only show when course is selected
    layout_filter = []  # Default value
    layout_filter_shown = False
    if course_filter and len(course_filter) > 0:
        try:
            # Get layouts for all selected courses
            placeholders = ",".join(["?"] * len(course_filter))
            layouts = conn.execute(
                f'SELECT DISTINCT "Layout Name" FROM analytics.rounds WHERE "Course Name" IN ({placeholders}) AND "Layout Name" IS NOT NULL ORDER BY "Layout Name"',
                course_filter).fetchall()
            layout_options = [l[0] for l in layouts] if layouts else []
        except Exception as e:
            st.error(f"Error loading layouts: {e}")
            layout_options = []

        # Only show layout filter when a course is selected
        if layout_options:
            with col4:
                layout_filter = st.multiselect(
                    "Layout", layout_options, key="layout_filter")
                layout_filter_shown = True

    # Date and other filters - use col4 if no layout filter, col5 if layout filter is shown
    filter_col = col4 if not layout_filter_shown else col5
    with filter_col:
        date_range = st.selectbox(
            "Date", ["Last 1 month", "Last 3 months", "Last 6 months", "Last 12 months", "Last 24 months", "All"], index=1)
        holes_min, holes_max = st.slider("Holes", 6, 27, (6, 27))
        score_min, score_max = st.slider("Score", -20, 30, (-20, 30))

    try:
        # Build query based on filters
        query = """
            SELECT 
                "Round SK",
                "Player",
                "Course Name",
                "Layout Name",
                "Course Layout Name",
                "Date Formatted",
                "Score",
                "Score Display",
                "UDisc Rating",
                "Holes Played",
                "Par",
                "Aces",
                "Eagles",
                "Birdies",
                "Pars",
                "Bogeys",
                "Doubles",
                "Triples",
                "Quads+",
                "Fairway Hits",
                "Fairway Attempts",
                "GIR C2",
                "GIR C1",
                "Parked",
                "C1 Putts Made",
                "C1 Putts Attempted",
                "C1X Putts Made",
                "C1X Putts Attempted",
                "C2 Putts Made",
                "C2 Putts Attempted",
                "1", "2", "3", "4", "5", "6",
                "7", "8", "9", "10", "11", "12",
                "13", "14", "15", "16", "17", "18",
                "19", "20", "21", "22", "23", "24",
                "25", "26", "27",
                "Result 1", "Result 2", "Result 3", "Result 4", "Result 5", "Result 6",
                "Result 7", "Result 8", "Result 9", "Result 10", "Result 11", "Result 12",
                "Result 13", "Result 14", "Result 15", "Result 16", "Result 17", "Result 18",
                "Result 19", "Result 20", "Result 21", "Result 22", "Result 23", "Result 24",
                "Result 25", "Result 26", "Result 27"
            FROM analytics.rounds
            WHERE 1=1
        """
        params = []

        if player_filter and len(player_filter) > 0:
            placeholders = ",".join(["?"] * len(player_filter))
            query += f' AND "Player" IN ({placeholders})'
            params.extend(player_filter)

        if course_filter and len(course_filter) > 0:
            placeholders = ",".join(["?"] * len(course_filter))
            query += f' AND "Course Name" IN ({placeholders})'
            params.extend(course_filter)

        if layout_filter and len(layout_filter) > 0:
            placeholders = ",".join(["?"] * len(layout_filter))
            query += f' AND "Layout Name" IN ({placeholders})'
            params.extend(layout_filter)

        if format_filter and len(format_filter) > 0:
            placeholders = ",".join(["?"] * len(format_filter))
            query += f' AND "Format" IN ({placeholders})'
            params.extend(format_filter)

        # Date filtering
        if date_range != "All":
            months = {
                "Last 1 month": 1,
                "Last 3 months": 3,
                "Last 6 months": 6,
                "Last 12 months": 12
            }.get(date_range, 3)
            query += f' AND "Date" >= CURRENT_DATE - INTERVAL \'{months} months\''

        query += ' ORDER BY "Date" DESC, "UDisc Rating" DESC LIMIT 500'

        try:
            df = conn.execute(query, params).df()
        except Exception as query_error:
            st.error(f"Query error: {query_error}")
            st.code(query)
            st.code(f"Params: {params}")
            import traceback
            st.code(traceback.format_exc())
            st.stop()

        if df is not None and not df.empty and len(df) > 0:
            # Calculate summary stats from filtered data
            st.markdown("#### Summary Statistics")

            # Calculate totals from aggregated columns
            total_holes = df['Holes Played'].sum(
            ) if 'Holes Played' in df.columns else 0
            aces = int(df['Aces'].sum()) if 'Aces' in df.columns else 0
            eagles = int(df['Eagles'].sum()) if 'Eagles' in df.columns else 0
            birdies = int(df['Birdies'].sum()
                          ) if 'Birdies' in df.columns else 0
            pars = int(df['Pars'].sum()) if 'Pars' in df.columns else 0
            bogeys = int(df['Bogeys'].sum()) if 'Bogeys' in df.columns else 0
            doubles = int(df['Doubles'].sum()
                          ) if 'Doubles' in df.columns else 0
            triples = int(df['Triples'].sum()
                          ) if 'Triples' in df.columns else 0
            quads_plus = int(df['Quads+'].sum()
                             ) if 'Quads+' in df.columns else 0

            # Stats in smaller format - custom HTML boxes (dark mode compatible)
            st.markdown("""
            <style>
            .stat-box {
                display: inline-block;
                padding: 6px 10px;
                margin: 2px;
                background-color: rgba(250, 250, 250, 0.8);
                border: 1px solid rgba(128, 128, 128, 0.3);
                border-radius: 4px;
                font-size: 12px;
                min-width: 70px;
            }
            .stat-label {
                font-size: 10px;
                color: rgba(0, 0, 0, 0.6);
                display: block;
                margin-bottom: 2px;
            }
            .stat-value {
                font-size: 16px;
                font-weight: bold;
                display: block;
                color: rgba(0, 0, 0, 0.9);
            }
            /* Dark mode styles */
            @media (prefers-color-scheme: dark) {
                .stat-box {
                    background-color: rgba(30, 30, 30, 0.8);
                    border-color: rgba(100, 100, 100, 0.5);
                }
                .stat-label {
                    color: rgba(255, 255, 255, 0.7);
                }
                .stat-value {
                    color: rgba(255, 255, 255, 0.9);
                }
            }
            /* Streamlit dark mode */
            .stApp[data-theme="dark"] .stat-box,
            [data-theme="dark"] .stat-box {
                background-color: rgba(30, 30, 30, 0.8);
                border-color: rgba(100, 100, 100, 0.5);
            }
            .stApp[data-theme="dark"] .stat-label,
            [data-theme="dark"] .stat-label {
                color: rgba(255, 255, 255, 0.7);
            }
            .stApp[data-theme="dark"] .stat-value,
            [data-theme="dark"] .stat-value {
                color: rgba(255, 255, 255, 0.9);
            }
            </style>
            """, unsafe_allow_html=True)

            # All stats in one row - 14 columns
            col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14 = st.columns(
                14)
            with col1:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Rounds</span><span class="stat-value">{len(df)}</span></div>', unsafe_allow_html=True)
            with col2:
                avg_rating = df['UDisc Rating'].mean()
                if pd.isna(avg_rating):
                    rating_val = "N/A"
                else:
                    rating_val = f"{avg_rating:.0f}"
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Avg Rating</span><span class="stat-value">{rating_val}</span></div>', unsafe_allow_html=True)
            with col3:
                avg_score = df['Score'].mean(
                ) if 'Score' in df.columns else None
                score_val = f"{avg_score:.1f}" if avg_score is not None and not pd.isna(
                    avg_score) else "N/A"
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Avg Score</span><span class="stat-value">{score_val}</span></div>', unsafe_allow_html=True)
            with col4:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Aces</span><span class="stat-value">{aces}</span></div>', unsafe_allow_html=True)
            with col5:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Eagles</span><span class="stat-value">{eagles}</span></div>', unsafe_allow_html=True)
            with col6:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Birdies</span><span class="stat-value">{birdies}</span></div>', unsafe_allow_html=True)
            with col7:
                birdie_pct = (birdies / total_holes *
                              100) if total_holes > 0 else 0
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Birdie %</span><span class="stat-value">{birdie_pct:.1f}%</span></div>', unsafe_allow_html=True)
            with col8:
                par_pct = (pars / total_holes * 100) if total_holes > 0 else 0
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Par %</span><span class="stat-value">{par_pct:.1f}%</span></div>', unsafe_allow_html=True)
            with col9:
                bogey_pct = (bogeys / total_holes *
                             100) if total_holes > 0 else 0
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Bogey %</span><span class="stat-value">{bogey_pct:.1f}%</span></div>', unsafe_allow_html=True)
            with col10:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Pars</span><span class="stat-value">{pars}</span></div>', unsafe_allow_html=True)
            with col11:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Bogeys</span><span class="stat-value">{bogeys}</span></div>', unsafe_allow_html=True)
            with col12:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Doubles</span><span class="stat-value">{doubles}</span></div>', unsafe_allow_html=True)
            with col13:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Triples</span><span class="stat-value">{triples}</span></div>', unsafe_allow_html=True)
            with col14:
                st.markdown(
                    f'<div class="stat-box"><span class="stat-label">Quads+</span><span class="stat-value">{quads_plus}</span></div>', unsafe_allow_html=True)

            st.markdown("---")
            st.markdown("### Rounds")

            # CSS to style the expand button
            st.markdown("""
            <style>
            /* Make the expand button smaller and less prominent */
            button[kind="secondary"] {
                padding: 0.25rem 0.5rem !important;
                font-size: 0.8rem !important;
                min-height: 1.5rem !important;
            }
            </style>
            """, unsafe_allow_html=True)

            # Find max hole number across all rounds to determine header width
            max_hole = 18
            for idx, row in df.iterrows():
                for i in range(27, 17, -1):
                    hole_num = str(i)
                    if pd.notna(row.get(hole_num)):
                        max_hole = max(max_hole, i)
                        break

            # Display column headers
            header_cols = st.columns(
                [2, 3.5, 1.8, 0.9, 0.9] + [0.4] * max_hole)
            with header_cols[0]:
                st.markdown("<strong>Player</strong>", unsafe_allow_html=True)
            with header_cols[1]:
                st.markdown("<strong>Course</strong>", unsafe_allow_html=True)
            with header_cols[2]:
                st.markdown("<strong>Date</strong>", unsafe_allow_html=True)
            with header_cols[3]:
                st.markdown("<strong>Score</strong>", unsafe_allow_html=True)
            with header_cols[4]:
                st.markdown("<strong>Rating</strong>", unsafe_allow_html=True)
            # Hole number headers
            for i in range(1, max_hole + 1):
                if 5 + i - 1 < len(header_cols):
                    with header_cols[5 + i - 1]:
                        st.markdown(
                            f"<div style='text-align: center; font-size: 11px;'><strong>{i}</strong></div>", unsafe_allow_html=True)

            st.markdown("---")

            # Display rounds with color-coded holes
            for idx, row in df.iterrows():
                # Calculate number of holes for this round
                round_max_hole = 18
                for i in range(27, 17, -1):
                    hole_num = str(i)
                    if pd.notna(row.get(hole_num)):
                        round_max_hole = i
                        break

                # Create expander for each round
                round_sk = row.get('Round SK', idx)
                expander_key = f"round_{round_sk}_{idx}"

                # Calculate throwing stats percentages
                fairway_hits = row.get('Fairway Hits', 0) or 0
                fairway_attempts = row.get('Fairway Attempts', 0) or 0
                fairway_pct = (fairway_hits / fairway_attempts *
                               100) if fairway_attempts > 0 else 0

                c1_made = row.get('C1 Putts Made', 0) or 0
                c1_attempted = row.get('C1 Putts Attempted', 0) or 0
                c1_pct = (c1_made / c1_attempted *
                          100) if c1_attempted > 0 else 0

                c1x_made = row.get('C1X Putts Made', 0) or 0
                c1x_attempted = row.get('C1X Putts Attempted', 0) or 0
                c1x_pct = (c1x_made / c1x_attempted *
                           100) if c1x_attempted > 0 else 0

                c2_made = row.get('C2 Putts Made', 0) or 0
                c2_attempted = row.get('C2 Putts Attempted', 0) or 0
                c2_pct = (c2_made / c2_attempted *
                          100) if c2_attempted > 0 else 0

                holes_played = row.get(
                    'Holes Played', round_max_hole) or round_max_hole
                gir_c2 = row.get('GIR C2', 0) or 0
                gir_c2_pct = (gir_c2 / holes_played *
                              100) if holes_played > 0 else 0

                gir_c1 = row.get('GIR C1', 0) or 0
                gir_c1_pct = (gir_c1 / holes_played *
                              100) if holes_played > 0 else 0

                parked = row.get('Parked', 0) or 0
                parked_pct = (parked / holes_played *
                              100) if holes_played > 0 else 0

                # Create row header (always visible) with expand button
                num_hole_cols = round_max_hole
                cols = st.columns([2, 3.5, 1.8, 0.9, 0.9] +
                                  [0.4] * num_hole_cols + [0.3])

                with cols[0]:
                    st.markdown(
                        f"<div style='white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'><strong>{row['Player']}</strong></div>", unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(
                        f"<div style='white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>{row['Course Layout Name']}</div>", unsafe_allow_html=True)
                with cols[2]:
                    st.markdown(
                        f"<div style='white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>{row['Date Formatted']}</div>", unsafe_allow_html=True)
                with cols[3]:
                    st.markdown(
                        f"<div style='white-space: nowrap;'><strong>{row['Score Display']}</strong></div>", unsafe_allow_html=True)
                with cols[4]:
                    rating = row['UDisc Rating']
                    if pd.isna(rating):
                        rating_display = "N/A"
                    else:
                        rating_display = f"{rating:.0f}"
                    st.markdown(
                        f"<div style='white-space: nowrap;'>{rating_display}</div>", unsafe_allow_html=True)

                # Display holes with color coding (only up to round_max_hole)
                for i in range(1, round_max_hole + 1):
                    if 5 + i - 1 >= len(cols):
                        break
                    hole_col = cols[5 + i - 1]
                    hole_num = str(i)
                    result_col = f"Result {i}"

                    if pd.notna(row[hole_num]) and pd.notna(row.get(result_col)):
                        hole_score = int(row[hole_num])
                        hole_result = row[result_col]

                        # Determine color based on result
                        if hole_result == 'Ace':
                            color = '#1E90FF'  # Blue
                        elif hole_result == 'Eagle':
                            color = '#1E90FF'  # Blue
                        elif hole_result == 'Birdie':
                            color = '#87CEEB'  # Light blue
                        elif hole_result == 'Par':
                            color = '#D3D3D3'  # Light gray
                        elif hole_result == 'Bogey':
                            color = '#FFD700'  # Yellow
                        elif hole_result == 'Double Bogey':
                            color = '#FFA500'  # Orange
                        elif hole_result == 'Triple Bogey':
                            color = '#A0522D'  # Brownish (sienna)
                        elif hole_result == 'Quadruple Bogey+':
                            color = '#2F2F2F'  # Dark gray
                        else:
                            color = '#CCCCCC'  # Default gray

                        # Create colored circle with number (smaller, less spacing)
                        hole_col.markdown(
                            f'<div style="display: inline-block; width: 26px; height: 26px; '
                            f'border-radius: 50%; background-color: {color}; color: white; '
                            f'text-align: center; line-height: 26px; font-weight: bold; font-size: 12px; margin: 0;">{hole_score}</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        hole_col.write("")

                # Small expand/collapse button at the end
                with cols[-1]:
                    if f"expanded_{expander_key}" not in st.session_state:
                        st.session_state[f"expanded_{expander_key}"] = False
                    if st.button("📊", key=f"btn_{expander_key}", help="View scorecard"):
                        st.session_state[f"expanded_{expander_key}"] = not st.session_state[f"expanded_{expander_key}"]

                # Expandable scorecard section (shown/hidden based on button)
                if st.session_state.get(f"expanded_{expander_key}", False):
                    # CSS to make metric values slightly smaller
                    st.markdown("""
                    <style>
                    div[data-testid="stMetricValue"] {
                        font-size: 1.5rem !important;
                    }
                    </style>
                    """, unsafe_allow_html=True)

                    # All 7 throwing stats in one compact row (reordered)
                    stat_cols = st.columns(7)

                    with stat_cols[0]:
                        st.metric("C1 Putting", f"{c1_pct:.1f}%")
                        st.caption(f"{int(c1_made)}/{int(c1_attempted)}")
                    with stat_cols[1]:
                        st.metric("C1X Putting", f"{c1x_pct:.1f}%")
                        st.caption(f"{int(c1x_made)}/{int(c1x_attempted)}")
                    with stat_cols[2]:
                        st.metric("C2 Putting", f"{c2_pct:.1f}%")
                        st.caption(f"{int(c2_made)}/{int(c2_attempted)}")
                    with stat_cols[3]:
                        st.metric("Fairway Hits", f"{fairway_pct:.1f}%")
                        st.caption(
                            f"{int(fairway_hits)}/{int(fairway_attempts)}")
                    with stat_cols[4]:
                        st.metric("GIR C2", f"{gir_c2_pct:.1f}%")
                        st.caption(f"{int(gir_c2)}/{int(holes_played)}")
                    with stat_cols[5]:
                        st.metric("GIR C1", f"{gir_c1_pct:.1f}%")
                        st.caption(f"{int(gir_c1)}/{int(holes_played)}")
                    with stat_cols[6]:
                        st.metric("Parked", f"{parked_pct:.1f}%")
                        st.caption(f"{int(parked)}/{int(holes_played)}")

                st.markdown("---")
        else:
            st.info(
                "No rounds found matching filters. Create `analytics.rounds` model in dbt if table doesn't exist.")

    except Exception as e:
        import traceback
        st.error(f"Error loading rounds: {e}")
        st.code(traceback.format_exc())
        st.info("Make sure the `analytics.rounds` table exists. Run: `cd dbt && dbt run --select analytics.rounds`")
