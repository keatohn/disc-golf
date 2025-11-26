"""Golden Birdies - cumulative birdie tracking"""

import streamlit as st


def show_golden_birdies(conn):
    """Golden Birdies - cumulative birdie tracking"""

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        course_layout = st.selectbox("Course Layout", ["Dolly Cooper Park"])
    with col2:
        player_filter = st.multiselect("Player", ["All"])

    try:
        # Golden Birdie Stats
        st.subheader("Golden Birdie Stats")
        stats_df = conn.execute("""
            SELECT * FROM analytics.golden_birdie_stats
            WHERE course_layout_name = ?
            ORDER BY hole_number
        """, [course_layout]).df()

        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True)
        else:
            st.info("No golden birdie stats available.")

        # Golden Birdie Tracker
        st.subheader("Golden Birdie Tracker")
        tracker_df = conn.execute("""
            SELECT * FROM analytics.golden_birdie_tracker
            WHERE course_layout_name = ?
            ORDER BY player_name, hole_number
        """, [course_layout]).df()

        if not tracker_df.empty:
            st.dataframe(tracker_df, use_container_width=True)
        else:
            st.info(
                "No golden birdie tracker data available. Create `analytics.golden_birdie_tracker` model in dbt.")

    except Exception as e:
        st.warning(
            f"Golden birdie data not found. Create analytics models: `golden_birdie_stats`, `golden_birdie_tracker`")
        st.error(f"Error: {e}")
