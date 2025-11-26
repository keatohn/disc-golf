"""Turkeys and Bounce Backs - streaks and patterns"""

import streamlit as st


def show_turkeys_bounce_backs(conn):
    """Turkeys and Bounce Backs - streaks and patterns"""

    # Filters
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        group_by = st.selectbox(
            "Group by Layout or Player", ["Layout", "Player"])
    with col2:
        course_layout = st.selectbox("Course Layout", ["All"])
    with col3:
        player_filter = st.selectbox("Player", ["All"])
    with col4:
        month_year = st.selectbox("Month, Year of Date", ["All"])

    try:
        # Turkeys table
        st.subheader("Turkeys")
        turkeys_df = conn.execute("""
            SELECT * FROM analytics.turkeys
            ORDER BY total_turkeys DESC
        """).df()

        if not turkeys_df.empty:
            st.dataframe(turkeys_df, use_container_width=True)
        else:
            st.info("No turkey data available.")

        # Bounce Backs table
        st.subheader("Bounce Backs")
        bounce_backs_df = conn.execute("""
            SELECT * FROM analytics.bounce_backs
            ORDER BY total_bounce_backs DESC
        """).df()

        if not bounce_backs_df.empty:
            st.dataframe(bounce_backs_df, use_container_width=True)
        else:
            st.info("No bounce back data available.")

    except Exception as e:
        st.warning(
            f"Turkeys/Bounce Backs data not found. Create analytics models: `turkeys`, `bounce_backs`")
        st.error(f"Error: {e}")
