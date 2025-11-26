"""Player Profile - dashboard for single, selectable player"""

import streamlit as st


def show_player_profile(conn):
    """Player Profile - dashboard for single, selectable player"""

    # Player selector
    try:
        players = conn.execute(
            "SELECT DISTINCT player_name FROM analytics.player_stats ORDER BY player_name").fetchall()
        player_options = [p[0]
                          for p in players] if players else ["No players found"]
    except:
        player_options = ["No players found"]

    selected_player = st.selectbox("Select Player", player_options)

    if selected_player == "No players found":
        st.info(
            "Create `analytics.player_stats` model in dbt to enable player profiles.")
        return

    try:
        # Rating history chart
        st.subheader("Rating History")
        rating_df = conn.execute("""
            SELECT date, rating 
            FROM analytics.player_rating_history
            WHERE player_name = ?
            ORDER BY date
        """, [selected_player]).df()

        if not rating_df.empty:
            st.line_chart(rating_df.set_index('date'))
        else:
            st.info(
                "No rating history available. Create `analytics.player_rating_history` model.")

        # Career stats
        st.subheader("Career Stats")
        stats_df = conn.execute("""
            SELECT * FROM analytics.player_stats
            WHERE player_name = ?
        """, [selected_player]).df()

        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True)
        else:
            st.info("No player stats available.")

    except Exception as e:
        st.warning(
            f"Player profile data not found. Create analytics models: `player_stats`, `player_rating_history`")
        st.error(f"Error: {e}")
