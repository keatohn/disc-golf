"""Power Scores - best possible score per course"""

import streamlit as st


def show_power_scores(conn):
    """Power Scores - best possible score per course"""

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        power_sour = st.selectbox(
            "Power/Sour Score", ["Power Scores", "Sour Scores"])
    with col2:
        course_layout_grouped = st.selectbox(
            "Course Layout (grouped)", ["All"])
    with col3:
        player_filter = st.multiselect("Player", ["All"])

    rounds_played_min, rounds_played_max = st.slider(
        "Rounds Played", 2, 200, (2, 200))

    st.info("💡 Tip: Click on a colored score to see the hole makeup of that power score below.")

    try:
        df = conn.execute("""
            SELECT * FROM analytics.power_scores
            WHERE rounds_played BETWEEN ? AND ?
            ORDER BY course_layout_name, power_score
        """, [rounds_played_min, rounds_played_max]).df()

        if not df.empty:
            st.dataframe(df, use_container_width=True)

            # Hole power scores detail (when a score is clicked)
            st.subheader("Hole Power Scores")
            st.info("Select a power score above to see hole-by-hole breakdown")
        else:
            st.info(
                "No power score data available. Create `analytics.power_scores` model in dbt.")

    except Exception as e:
        st.warning(
            f"`analytics.power_scores` table not found. Create this model in `dbt/models/analytics/power_scores.sql`")
        st.error(f"Error: {e}")
