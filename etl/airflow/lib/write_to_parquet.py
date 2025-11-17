import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


def write_scorecard_data(scorecard_data: Dict[str, Any], user_name: str) -> str:
    """Write scorecard data to Parquet file."""
    # Generate storage path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # Use /opt/airflow/data which is mounted from ./data
    data_dir = Path('/opt/airflow/data')
    user_dir = data_dir / user_name.lower()
    user_dir.mkdir(parents=True, exist_ok=True)

    file_path = user_dir / f"data_{timestamp}.parquet"

    try:
        # Convert scorecard data to DataFrame
        # Each scorecard becomes a row with the full JSON as a column
        df = pd.DataFrame({
            'raw_data': [json.dumps(scorecard) for scorecard in scorecard_data],
            'user_name': user_name.lower(),
            'loaded_at': datetime.now()
        })

        # Write to Parquet
        df.to_parquet(file_path, engine='pyarrow', index=False)

        print(f"Wrote scorecard data for {user_name} to {file_path}")
        return str(file_path)

    except Exception as e:
        print(f"Error writing Parquet file for {user_name}: {e}")
        raise e


def create_file_path(user_name: str, date: str = None) -> str:
    """Generate the storage path for scorecard data."""
    if date is None:
        date = datetime.now().strftime('%Y%m%d')

    time_stamp = datetime.now().strftime('%H%M%S')
    return f"{user_name.lower()}/data_{date}_{time_stamp}.parquet"


def write_all_scorecards(scorecards_data: Dict[str, Any]) -> Dict[str, str]:
    """Write scorecard data to Parquet files for all users."""
    results = {}

    for user_name, user_data in scorecards_data.items():
        try:
            # Extract the actual scorecards from the user data
            if isinstance(user_data, dict) and 'scorecards' in user_data:
                scorecards = user_data['scorecards']
            else:
                # Fallback: assume user_data is directly the scorecards
                scorecards = user_data

            file_path = write_scorecard_data(scorecards, user_name.lower())
            results[user_name] = file_path
            print(f"Successfully wrote scorecards for {user_name} to Parquet")
        except Exception as e:
            print(f"Error writing scorecards for {user_name}: {e}")
            raise e

    return results
