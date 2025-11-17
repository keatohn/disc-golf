#!/usr/bin/env python3
"""
Load scorecard data from Parquet files into DuckDB.
"""

import os
import duckdb
from pathlib import Path
from datetime import datetime
from typing import Optional


def get_duckdb_path():
    """Get the path to the DuckDB database file."""
    # Use /opt/airflow/data which is mounted from ./data
    db_path = Path('/opt/airflow/data') / 'warehouse.duckdb'
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return str(db_path)


def get_data_directory():
    """Get the path to the data directory containing Parquet files."""
    # Use /opt/airflow/data which is mounted from ./data
    return Path('/opt/airflow/data')


def create_scorecards_table(conn):
    """Create the raw scorecards table if it doesn't exist."""
    print("Creating raw_udisc_scorecards table...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_udisc_scorecards (
            raw_data JSON,
            user_name VARCHAR,
            file_name VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Table created successfully!")


def load_latest_scorecards():
    """Load the latest scorecard data from Parquet files for each user."""
    db_path = get_duckdb_path()
    data_dir = get_data_directory()

    conn = duckdb.connect(db_path)

    try:
        print("Starting scorecard data load...")

        # Create table if needed
        create_scorecards_table(conn)

        # Find all user directories
        user_dirs = [d for d in data_dir.iterdir() if d.is_dir()
                     and d.name != '__pycache__']

        if not user_dirs:
            print("No user directories found in data folder")
            return None

        loaded_files = []

        for user_dir in user_dirs:
            user_name = user_dir.name

            # Find all parquet files for this user
            parquet_files = sorted(user_dir.glob('data_*.parquet'))

            if not parquet_files:
                print(f"No Parquet files found for user: {user_name}")
                continue

            # Get the latest file (sorted by name, which includes timestamp)
            latest_file = parquet_files[-1]

            print(
                f"Loading data for user: {user_name} from {latest_file.name}")

            # Load Parquet file into table
            # First, read the parquet file
            temp_table = f"temp_{user_name}"
            conn.execute(f"""
                CREATE TEMP TABLE {temp_table} AS 
                SELECT * FROM read_parquet('{latest_file}')
            """)

            # Insert each row from the temp table
            # The raw_data column contains JSON strings that we need to keep as JSON
            conn.execute(f"""
                INSERT INTO raw_udisc_scorecards (raw_data, user_name, file_name, loaded_at)
                SELECT 
                    raw_data::JSON as raw_data,
                    user_name,
                    '{latest_file.name}' as file_name,
                    loaded_at
                FROM {temp_table}
            """)

            # Get record count
            result = conn.execute(
                f"SELECT COUNT(*) FROM {temp_table}").fetchone()
            record_count = result[0] if result else 0

            loaded_files.append({
                'user': user_name,
                'file': latest_file.name,
                'records': record_count
            })

            print(f"  - Loaded {record_count} records from {latest_file.name}")

        print(f"Successfully loaded data from {len(loaded_files)} files!")

        # Get total record count
        result = conn.execute(
            "SELECT COUNT(*) FROM raw_udisc_scorecards").fetchone()
        total_records = result[0] if result else 0

        return {
            'files_loaded': loaded_files,
            'total_records': total_records
        }

    except Exception as e:
        print(f"Error loading scorecards: {e}")
        raise
    finally:
        conn.close()


def load_to_duckdb():
    """Main function to setup and load data to DuckDB."""
    try:
        print("Starting DuckDB integration...")

        # Load the latest data
        result = load_latest_scorecards()

        if result:
            print(f"DuckDB integration completed successfully!")
            print(f"   - Files processed:")
            for file_info in result['files_loaded']:
                print(
                    f"     * {file_info['user']}: {file_info['file']} ({file_info['records']} records)")
            print(f"   - Total records in database: {result['total_records']}")
            return result
        else:
            print("No files found to load")
            return None

    except Exception as e:
        print(f"DuckDB integration failed: {e}")
        raise
