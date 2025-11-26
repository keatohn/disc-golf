"""Head-to-Head - compare 2 players"""

import streamlit as st


def show_head_to_head(conn):
    """Head-to-Head - compare 2 players"""

    # Player selectors
    try:
        players = conn.execute(
            "SELECT DISTINCT player_name FROM analytics.player_stats ORDER BY player_name").fetchall()
        player_options = [p[0] for p in players] if players else ["No players"]
    except:
        player_options = ["No players"]

    col1, col2 = st.columns(2)
    with col1:
        player1 = st.selectbox("Player 1", player_options)
    with col2:
        player2 = st.selectbox("Player 2", player_options)

    # Additional filters
    col3, col4, col5 = st.columns(3)
    with col3:
        course_filter = st.selectbox("Course", ["All"])
    with col4:
        date_range = st.selectbox(
            "Date", ["Last 10 years", "Last 5 years", "Last 3 years", "All"])
    with col5:
        format_filter = st.selectbox("Format", ["All", "Singles", "Doubles"])

    if player1 == "No players" or player2 == "No players":
        st.info(
            "Create `analytics.player_stats` model to enable head-to-head comparisons.")
        return

    try:
        h2h_df = conn.execute("""
            SELECT * FROM analytics.head_to_head
            WHERE (player1_name = ? AND player2_name = ?)
               OR (player1_name = ? AND player2_name = ?)
        """, [player1, player2, player2, player1]).df()

        if not h2h_df.empty:
            st.dataframe(h2h_df, use_container_width=True)
        else:
            st.info(
                "No head-to-head data available. Create `analytics.head_to_head` model in dbt.")

    except Exception as e:
        st.warning(
            f"`analytics.head_to_head` table not found. Create this model in `dbt/models/analytics/head_to_head.sql`")
        st.error(f"Error: {e}")
