"""Record Sheet - personal records per course layout"""

import streamlit as st


def show_record_sheet(conn):
    """Record Sheet - personal records per course layout"""

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        course_input = st.text_input("Course", "")
        record_holder = st.text_input("Record Holder", "")
    with col2:
        sort_by = st.selectbox(
            "Sort By", ["Most Played", "Best Score", "Best Rating"])
        format_filter = st.selectbox("Format", ["All", "Singles", "Doubles"])
    with col3:
        retired_layout = st.selectbox("Retired Layout?", ["No", "Yes", "All"])
        state_filter = st.selectbox("State", ["All"])

    try:
        df = conn.execute("""
            SELECT * FROM analytics.course_records
            ORDER BY rounds_played DESC
            LIMIT 100
        """).df()

        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info(
                "No record data available. Create `analytics.course_records` model in dbt.")

    except Exception as e:
        st.warning(
            f"`analytics.course_records` table not found. Create this model in `dbt/models/analytics/course_records.sql`")
        st.error(f"Error: {e}")
