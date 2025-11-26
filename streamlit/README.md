# Disc Golf Analytics Dashboard

Interactive Streamlit dashboard for analyzing disc golf scorecard data from the DuckDB warehouse.

## Quick Start

### 1. Install Dependencies

```bash
cd streamlit
pip install -r requirements.txt
```

### 2. Ensure Database Exists

The app expects the DuckDB warehouse at `etl/data/warehouse.duckdb` (relative to project root).

- If the database doesn't exist, run your ETL pipeline first
- Run dbt models to populate analytics tables: `cd dbt && dbt run --select analytics.*`

### 3. Run the App

**Important:** Make sure you're using the Python environment where dependencies are installed.

```bash
cd streamlit
streamlit run app.py
```

If you get "No module named streamlit", try:
```bash
# Use the specific Python version that has packages installed
/Library/Frameworks/Python.framework/Versions/3.12/bin/python3 -m streamlit run app.py
```

Or check which Python has streamlit:
```bash
python3.12 -m streamlit --version
```

The app will automatically open in your browser at `http://localhost:8501`.

**Alternative:** Run from project root:
```bash
streamlit run streamlit/app.py
```

## Development

### Auto-Reload

Streamlit automatically reloads when you save changes to any file. No need to restart the server.

### Project Structure

```
streamlit/
├── app.py                 # Main application entry point
├── page_modules/          # Individual page modules (not auto-discovered by Streamlit)
│   ├── __init__.py       # Page exports
│   ├── title_page.py
│   ├── all_rounds.py
│   ├── monthly_summary.py
│   └── ...               # Other page modules
├── utils/                 # Utility modules
│   └── db_connection.py  # DuckDB connection handling
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

**Note:** The folder is named `page_modules/` (not `pages/`) to prevent Streamlit from auto-discovering them as separate pages. We use custom radio button navigation instead.

### Adding New Pages

1. Create a new file in `page_modules/` (e.g., `page_modules/my_new_page.py`)
2. Define a function `show_my_new_page(conn)` that takes a database connection
3. Import and add it to `page_modules/__init__.py`
4. Add it to the navigation list in `app.py`

Example:
```python
# pages/my_new_page.py
import streamlit as st

def show_my_new_page(conn):
    st.header("My New Page")
    df = conn.execute("SELECT * FROM analytics.my_table").df()
    st.dataframe(df)
```

## Analytics Schema

The dashboard queries tables from the `analytics` schema in DuckDB. Create dbt models in `dbt/models/analytics/` to populate these tables.

### Current Models

- `analytics.rounds` - Base rounds model with all round details
- `analytics.hole_results` - Hole-by-hole performance metrics

### Pages and Their Required Models

| Page | Required Analytics Models |
|------|---------------------------|
| All Rounds | `analytics.rounds` |
| Monthly Summary | `analytics.monthly_summary` |
| Record Sheet | `analytics.course_records` |
| Player Profile | `analytics.player_stats`, `analytics.player_rating_history` |
| Course Profile | `analytics.course_stats`, `analytics.course_rounds` |
| Hole Analysis | `analytics.hole_statistics`, `analytics.hole_result_distribution` |
| Stats Tables | `analytics.player_stats_summary` |
| Historic Ratings | `analytics.player_rating_history` |
| Head-to-Head | `analytics.head_to_head` |
| Power Scores | `analytics.power_scores` |
| Turkeys and Bounce Backs | `analytics.turkeys`, `analytics.bounce_backs` |
| Hole Streaks | `analytics.hole_streaks` |
| Golden Birdies | `analytics.golden_birdie_stats`, `analytics.golden_birdie_tracker` |

Pages will show helpful error messages if their required models don't exist yet.

## Troubleshooting

### Database Not Found

If you see `FileNotFoundError: Database not found at...`:
1. Check that `etl/data/warehouse.duckdb` exists
2. Run the ETL pipeline to create the warehouse
3. Verify the path in `utils/db_connection.py` is correct

### Tables Not Found

If pages show "table not found" errors:
1. Run dbt to create analytics models: `cd dbt && dbt run --select analytics.*`
2. Check that models are in the `analytics` schema
3. Verify table names match what the page expects

### Import Errors

If you see import errors:
1. Make sure you're in the `streamlit/` directory or project root
2. Verify all dependencies are installed: `pip install -r requirements.txt`
3. Check that `pages/__init__.py` exports all page functions
