"""Title Page - updates and versions"""

import streamlit as st


def show_title_page(conn):
    """Title Page - updates and versions"""
    st.markdown("""
    ## Disc Golf Analytics Dashboard
    
    Welcome to the Disc Golf Analytics Dashboard! This dashboard provides comprehensive
    analysis of disc golf scorecard data.
    
    ### Current Version
    **Version 1.0.0** - Initial Release
    
    ### Recent Updates
    - Initial dashboard setup
    - Analytics schema integration
    - Multiple tab navigation
    
    ### Data Source
    Data is sourced from the DuckDB warehouse (`analytics` schema) and is updated
    through the ETL pipeline and dbt transformations.
    """)
