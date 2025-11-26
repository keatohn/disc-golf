"""Monthly Summary - monthly stats for each player"""

import streamlit as st


def show_monthly_summary(conn):
    """Monthly Summary - monthly stats for each player"""
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        month = st.selectbox("Month", ["April 2024", "May 2024", "June 2024"])
    with col2:
        year = st.selectbox("Year", ["2024", "2023", "2022"])

    try:
        df = conn.execute("""
            SELECT * FROM analytics.monthly_summary
            WHERE month = ? AND year = ?
            ORDER BY player_rating DESC
        """, [month, year]).df()

        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info(
                "No monthly summary data available. Create `analytics.monthly_summary` model in dbt.")

    except Exception as e:
        st.warning(
            f"`analytics.monthly_summary` table not found. Create this model in `dbt/models/analytics/monthly_summary.sql`")
        st.error(f"Error: {e}")
