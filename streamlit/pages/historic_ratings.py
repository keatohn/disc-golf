"""Historic Ratings - history of player ratings"""

import streamlit as st
import pandas as pd


def show_historic_ratings(conn):
    """Historic Ratings - history of player ratings"""

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        player_filter = st.multiselect("Player", ["All"])
    with col2:
        date_range = st.selectbox(
            "Date", ["Last 3 years", "Last 5 years", "Last 10 years", "All"])

    try:
        rating_df = conn.execute("""
            SELECT date, player_name, rating
            FROM analytics.player_rating_history
            ORDER BY date, player_name
        """).df()

        if not rating_df.empty:
            # Pivot for line chart
            pivot_df = rating_df.pivot(
                index='date', columns='player_name', values='rating')
            st.line_chart(pivot_df)
        else:
            st.info(
                "No rating history available. Create `analytics.player_rating_history` model in dbt.")

    except Exception as e:
        st.warning(
            f"`analytics.player_rating_history` table not found. Create this model in `dbt/models/analytics/player_rating_history.sql`")
        st.error(f"Error: {e}")
