"""Course Profile - dashboard for single, selectable course"""

import streamlit as st


def show_course_profile(conn):
    """Course Profile - dashboard for single, selectable course"""

    # Course selector
    try:
        courses = conn.execute(
            "SELECT DISTINCT course_layout_name FROM analytics.course_stats ORDER BY course_layout_name").fetchall()
        course_options = [c[0]
                          for c in courses] if courses else ["No courses found"]
    except:
        course_options = ["No courses found"]

    selected_course = st.selectbox("Select Course Layout", course_options)

    if selected_course == "No courses found":
        st.info(
            "Create `analytics.course_stats` model in dbt to enable course profiles.")
        return

    try:
        # Course stats
        st.subheader("Course Statistics")
        stats_df = conn.execute("""
            SELECT * FROM analytics.course_stats
            WHERE course_layout_name = ?
        """, [selected_course]).df()

        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True)

        # Top rounds
        st.subheader("Top Rounds")
        rounds_df = conn.execute("""
            SELECT * FROM analytics.course_rounds
            WHERE course_layout_name = ?
            ORDER BY score ASC, rating DESC
            LIMIT 20
        """, [selected_course]).df()

        if not rounds_df.empty:
            st.dataframe(rounds_df, use_container_width=True)
        else:
            st.info("No rounds data available.")

    except Exception as e:
        st.warning(
            f"Course profile data not found. Create analytics models: `course_stats`, `course_rounds`")
        st.error(f"Error: {e}")
