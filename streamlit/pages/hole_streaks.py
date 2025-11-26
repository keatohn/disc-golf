"""Hole Streaks - various streak types"""

import streamlit as st


def show_hole_streaks(conn):
    """Hole Streaks - various streak types"""

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        player_filter = st.multiselect("Player", ["All"])
    with col2:
        as_of_date = st.date_input("As Of", value=None)
    with col3:
        course_layout = st.selectbox("Course Layout", ["All", "Timmons Park"])

    st.info("Note: All filters apply to any view below them.")

    try:
        # Total streaks
        st.subheader("Total Streaks")
        total_streaks_df = conn.execute("""
            SELECT * FROM analytics.hole_streaks
            WHERE streak_type = 'total'
            ORDER BY player_name
        """).df()

        if not total_streaks_df.empty:
            st.dataframe(total_streaks_df, use_container_width=True)

        # Course-specific streaks
        if course_layout != "All":
            st.subheader(f"Streaks at {course_layout}")
            course_streaks_df = conn.execute("""
                SELECT * FROM analytics.hole_streaks
                WHERE streak_type = 'course' AND course_layout_name = ?
                ORDER BY player_name
            """, [course_layout]).df()

            if not course_streaks_df.empty:
                st.dataframe(course_streaks_df, use_container_width=True)
        else:
            st.info(
                "No streak data available. Create `analytics.hole_streaks` model in dbt.")

    except Exception as e:
        st.warning(
            f"`analytics.hole_streaks` table not found. Create this model in `dbt/models/analytics/hole_streaks.sql`")
        st.error(f"Error: {e}")
