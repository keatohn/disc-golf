import json
import os
import boto3
from datetime import datetime
from typing import Dict, Any
from botocore.config import Config


def upload_scorecard_data(scorecard_data: Dict[str, Any], user_name: str) -> str:
    """Upload scorecard data to S3."""
    bucket_name = os.getenv('AWS_S3_BUCKET')
    if not bucket_name:
        raise ValueError("AWS_S3_BUCKET environment variable not set")

    # Generate storage path with simplified structure: user/filename_with_date_time
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    storage_key = f"{user_name.lower()}/data_{timestamp}.json"

    # Create S3 client with timeout configuration
    config = Config(
        connect_timeout=30,  # 30 seconds to establish connection
        read_timeout=60,     # 60 seconds for read operations
        retries={'max_attempts': 3}  # Retry failed requests up to 3 times
    )

    s3_client = boto3.client('s3', config=config)

    try:
        # Upload to S3 with timeout protection
        s3_client.put_object(
            Bucket=bucket_name,
            Key=storage_key,
            Body=json.dumps(scorecard_data, indent=2),
            ContentType='application/json'
        )

        print(f"Uploaded scorecard data for {user_name} to cloud storage")
        return storage_key

    except Exception as e:
        print(f"Error uploading to S3 for {user_name}: {e}")
        raise e


def create_file_path(user_name: str, date: str = None) -> str:
    """Generate the storage path for scorecard data."""
    if date is None:
        date = datetime.now().strftime('%Y%m%d')

    time_stamp = datetime.now().strftime('%H%M%S')
    return f"{user_name.lower()}/data_{date}_{time_stamp}.json"


def upload_all_scorecards(scorecards_data: Dict[str, Any]) -> Dict[str, str]:
    """Upload scorecard data to S3 for all users."""
    results = {}

    for user_name, user_data in scorecards_data.items():
        try:
            # Extract the actual scorecards from the user data
            if isinstance(user_data, dict) and 'scorecards' in user_data:
                scorecards = user_data['scorecards']
            else:
                # Fallback: assume user_data is directly the scorecards
                scorecards = user_data

            storage_path = upload_scorecard_data(scorecards, user_name.lower())
            results[user_name] = storage_path
            print(f"Successfully uploaded scorecards for {user_name} to S3")
        except Exception as e:
            print(f"Error uploading scorecards for {user_name}: {e}")
            raise e

    return results
