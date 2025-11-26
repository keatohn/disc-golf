"""
Disc Golf Analytics Dashboard
Interactive dashboard for analyzing disc golf scorecard data.
"""

import streamlit as st
from utils.db_connection import get_db_connection
from pages import (
    show_title_page,
    show_monthly_summary,
    show_all_rounds,
    show_record_sheet,
    show_player_profile,
    show_course_profile,
    show_hole_analysis,
    show_stats_tables,
    show_historic_ratings,
    show_head_to_head,
    show_power_scores,
    show_turkeys_bounce_backs,
    show_hole_streaks,
    show_golden_birdies,
)


# Page configuration
st.set_page_config(
    page_title="Disc Golf Analytics",
    page_icon="⛳",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Initialize connection
@st.cache_resource
def init_connection():
    """Initialize and cache DuckDB connection"""
    return get_db_connection()


def main():
    """Main application"""
    # Initialize database connection
    try:
        conn = init_connection()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        st.stop()

    # Navigation - only show All Rounds for now
    tabs = st.tabs(["All Rounds"])

    with tabs[0]:
        st.title("📋 All Rounds")
        st.markdown("---")
        show_all_rounds(conn)


if __name__ == "__main__":
    main()
