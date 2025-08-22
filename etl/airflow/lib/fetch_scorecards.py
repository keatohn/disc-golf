"""
Fetch scorecard data from UDisc API.
"""

import os
import sys
import json
import api
from user_manager import get_user_manager, User, login_user
from concurrent.futures import ThreadPoolExecutor
from create_snowflake_connection import create_snowflake_connection


def get_latest_snowflake_timestamp():
    """Get the latest updated_at timestamp from Snowflake scorecard table"""

    scorecard_table = "FCT_SCORECARD"
    try:
        conn = create_snowflake_connection(schema="DW")
        if not conn:
            print("Warning: Could not connect to Snowflake, will fetch all scorecards")
            return None

        cursor = conn.cursor()

        # Query the latest updated_at timestamp from scorecard table
        query = "SELECT MAX(updated_at) as latest_timestamp FROM {scorecard_table}"
        cursor.execute(query)

        result = cursor.fetchone()
        if result and result[0]:
            # Convert to ISO format with Z
            latest_timestamp = result[0].isoformat() + 'Z'
            print(f"Retrieved latest Snowflake timestamp: {latest_timestamp}")
            return latest_timestamp
        else:
            print("No existing scorecards found in Snowflake, will fetch all scorecards")
            return None

    except Exception as e:
        print(f"Error querying Snowflake for latest timestamp: {e}")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def fetch_scorecards(user: User):
    """Fetch scorecard data from UDisc API for a specific user."""
    if not user.username or not user.password:
        raise ValueError(
            f"Missing username or password for {user.display_name}")

    # Login to get fresh API token
    if not login_user(user):
        raise ValueError(f"Failed to login {user.display_name}")

    all_scorecards = []
    skip = 0

    # Check if we're doing incremental loading
    load_type = os.getenv('LOAD_TYPE', 'full').lower()
    latest_snowflake_timestamp = None

    if load_type == 'incremental':
        # Get latest timestamp from Snowflake
        latest_snowflake_timestamp = get_latest_snowflake_timestamp()
        if latest_snowflake_timestamp:
            print(
                f"  Incremental mode: Fetching scorecards newer than {latest_snowflake_timestamp}")
        else:
            print(
                f"  Incremental mode: No Snowflake timestamp found, fetching all recent scorecards")
    else:
        print(f"  Full load mode: Fetching all scorecards")

    while True:
        response = api.get(
            endpoint="/classes/Scorecard",
            params={
                "where": json.dumps({
                    "$or": [
                        {
                            "createdBy": {
                                "__type": "Pointer",
                                "className": "_User",
                                "objectId": user.user_object_id
                            },
                        },
                        {
                            "users": {
                                "__type": "Pointer",
                                "className": "_User",
                                "objectId": user.user_object_id
                            },
                        }
                    ],
                    "version": {
                        "$gt": 0,
                        "$lt": 4
                    },
                }),
                "order": "updatedAt",
                "include": "createdBy,entries,entries.users,entries.players",
                "limit": 50,
                "skip": skip
            },
            session_token=user.api_token
        )

        if response.ok:
            scorecards = response.json()["results"]
            all_scorecards.extend(scorecards)

            # Check if we should stop pagination
            if len(scorecards) < 50:
                print(f"    Less than 50 results, ending pagination")
                break

            # For incremental loading, check the last scorecard's timestamp
            if load_type == 'incremental' and latest_snowflake_timestamp and scorecards:
                last_scorecard_timestamp = scorecards[-1].get('updatedAt')
                if last_scorecard_timestamp and last_scorecard_timestamp <= latest_snowflake_timestamp:
                    print(
                        f"    Incremental mode: Reached scorecards older than Snowflake timestamp ({last_scorecard_timestamp} <= {latest_snowflake_timestamp}), stopping pagination")
                    break

            skip += 50
        else:
            print("Failed to fetch results:", response.status_code,
                  response.text, file=sys.stderr)
            break

    print(f"{user.display_name}: Fetched {len(all_scorecards)} scorecards from API.")
    return all_scorecards


def fetch_all_scorecards(user_names=None):
    """Fetch scorecards for all users using concurrent processing"""
    user_manager = get_user_manager()

    if user_names is None:
        users = user_manager.get_all_users()
    else:
        users = [user_manager.get_user(name) for name in user_names]
        users = [u for u in users if u is not None]  # Filter out None values

    if not users:
        print("No users to fetch scorecards for")
        return {}

    print(f"Fetching scorecards for {len(users)} users...")

    # Use concurrent processing for 2+ users, sequential for single user
    if len(users) > 1:
        print(f"Using concurrent processing for {len(users)} users...")

        with ThreadPoolExecutor(max_workers=len(users)) as executor:
            user_results = list(executor.map(fetch_scorecards, users))

        # Create a dictionary with user names as keys
        scorecards_by_user = {}
        for i, user in enumerate(users):
            scorecards_by_user[user.name] = user_results[i]
    else:
        # Single user - sequential processing
        scorecards_by_user = {users[0].name: fetch_scorecards(users[0])}

    print(f"Retrieved scorecards for {len(scorecards_by_user)} users from API")
    print(
        f"Returning data structure: {type(scorecards_by_user)} with keys: {list(scorecards_by_user.keys()) if isinstance(scorecards_by_user, dict) else 'Not a dict'}")
    return scorecards_by_user


if __name__ == "__main__":
    # For manual execution
    print("User Summary:")
    user_manager = get_user_manager()
    summary = user_manager.get_user_summary()
    for name, info in summary.items():
        print(f"  {name}: {'✓' if info['configured'] else '✗'} configured")

    print("\nFetching scorecards...")
    fetch_scorecards()
