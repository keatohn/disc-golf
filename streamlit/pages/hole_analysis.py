"""Hole Analysis - in-depth stats for specific holes"""

import streamlit as st


def show_hole_analysis(conn):
    """Hole Analysis - in-depth stats for specific holes"""

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        course_layout = st.selectbox("Course Layout", ["Timmons Park"])
    with col2:
        player_filter = st.multiselect("Player", ["All"])
    with col3:
        date_range = st.selectbox(
            "Date", ["Last 5 years", "Last 3 years", "Last year", "All"])

    st.info("💡 Tip: Click on a colored hole average score to filter the rest of the dashboard for that hole.")

    try:
        # Hole average scores
        st.subheader("Hole Average Scores")
        hole_avg_df = conn.execute("""
            SELECT hole_number, avg_score_vs_par
            FROM analytics.hole_statistics
            WHERE course_layout_name = ?
            ORDER BY hole_number
        """, [course_layout]).df()

        if not hole_avg_df.empty:
            # Display as colored boxes
            cols = st.columns(18)
            for idx, row in hole_avg_df.iterrows():
                with cols[idx % 18]:
                    score = row['avg_score_vs_par']
                    color = "blue" if score < 0 else "orange"
                    st.metric(f"Hole {row['hole_number']}", f"{score:+.2f}")

        # Hole result distribution
        st.subheader("Hole Result Distribution")
        dist_df = conn.execute("""
            SELECT * FROM analytics.hole_result_distribution
            WHERE course_layout_name = ?
        """, [course_layout]).df()

        if not dist_df.empty:
            st.dataframe(dist_df, use_container_width=True)
        else:
            st.info("No hole analysis data available. Create `analytics.hole_statistics` and `analytics.hole_result_distribution` models.")

    except Exception as e:
        st.warning(
            f"Hole analysis data not found. Create analytics models: `hole_statistics`, `hole_result_distribution`")
        st.error(f"Error: {e}")
