"""Stats Tables - big tables of raw stats"""

import streamlit as st


def show_stats_tables(conn):
    """Stats Tables - big tables of raw stats"""

    # Filters
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        player_filter = st.multiselect("Player", ["All"])
    with col2:
        course_filter = st.selectbox("Course", ["All"])
    with col3:
        course_layout_filter = st.selectbox("Course Layout", ["All"])
    with col4:
        date_range = st.selectbox(
            "Date", ["Last 1", "Last 6 months", "Last year", "All"])

    st.info("Note: All filters apply to any view below them.")

    try:
        df = conn.execute(
            "SELECT * FROM analytics.player_stats_summary LIMIT 1000").df()

        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info(
                "No stats table data available. Create `analytics.player_stats_summary` model in dbt.")

    except Exception as e:
        st.warning(
            f"`analytics.player_stats_summary` table not found. Create this model in `dbt/models/analytics/player_stats_summary.sql`")
        st.error(f"Error: {e}")
