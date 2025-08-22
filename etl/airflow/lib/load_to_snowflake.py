#!/usr/bin/env python3
"""
Load scorecard data from S3 into Snowflake.
This module handles setting up the external stage, creating the table, and loading data.
"""

import os
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional
from create_snowflake_connection import create_snowflake_connection

# Load environment variables at module level
load_dotenv()


def get_configs():
    """Get Snowflake configuration from environment variables."""
    config = {
        'stage_name': os.getenv('SNOWFLAKE_STAGE_NAME', 'udisc_scorecards'),
        'table_name': os.getenv('SNOWFLAKE_TABLE_NAME', 'raw_udisc_scorecards'),
        'bucket_name': os.getenv('AWS_S3_BUCKET'),
        'aws_key_id': os.getenv('AWS_S3_ACCESS_KEY_ID'),
        'aws_secret_key': os.getenv('AWS_S3_SECRET_ACCESS_KEY')
    }

    # Validate required fields
    missing_fields = []
    for key, value in config.items():
        if value is None:
            missing_fields.append(key.upper())

    if missing_fields:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_fields)}")

    return config


def setup_external_stage():
    """Setup the external stage and file format if they don't exist."""
    config = get_configs()

    conn = create_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Step 1: Create file format for JSON data (if not exists)
        print("Creating JSON file format...")
        cursor.execute("""
            CREATE FILE FORMAT IF NOT EXISTS json_format
                TYPE = 'JSON'
                COMPRESSION = 'AUTO'
                ENABLE_OCTAL = FALSE
                ALLOW_DUPLICATE = FALSE
                STRIP_OUTER_ARRAY = FALSE
                STRIP_NULL_VALUES = FALSE
                IGNORE_UTF8_ERRORS = FALSE
        """)

        # Step 2: Create external stage pointing to S3 bucket (if not exists)
        print(
            f"Creating external stage '{config['stage_name']}' for bucket: s3://{config['bucket_name']}/")
        cursor.execute(f"""
            CREATE STAGE IF NOT EXISTS {config['stage_name']}
                URL = 's3://{config['bucket_name']}/'
                CREDENTIALS = (
                    AWS_KEY_ID = '{config['aws_key_id']}'
                    AWS_SECRET_KEY = '{config['aws_secret_key']}'
                )
                FILE_FORMAT = json_format
        """)

        print("External stage setup completed")

    except Exception as e:
        print(f"Error setting up external stage: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def create_scorecards_table():
    """Create the scorecards table if it doesn't exist."""
    config = get_configs()

    conn = create_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Create the scorecards table (if not exists)
        print(f"Creating table '{config['table_name']}'...")
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {config['table_name']} (
                raw_data VARIANT,
                user_name STRING,
                file_name STRING,
                loaded_at TIMESTAMP DEFAULT CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', CURRENT_TIMESTAMP())
            )
        """)

        print("Scorecards table created successfully!")

    except Exception as e:
        print(f"Error creating table: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def load_latest_scorecards():
    """Load the latest scorecard data from S3 for each user."""
    config = get_configs()

    conn = create_snowflake_connection()
    cursor = conn.cursor()

    try:
        print("Starting scorecard data load...")

        # Find all user files
        print("Finding scorecard files for each user...")
        try:
            cursor.execute(f"""
                SELECT
                    split_part(metadata$filename, '/', 1) as user_name,
                    split_part(metadata$filename, '/', 2) as file_name,
                    metadata$file_last_modified as file_last_modified
                FROM @{config['stage_name']}/
                WHERE file_name LIKE 'data_%.json'
                QUALIFY row_number() over (partition by user_name order by file_last_modified desc) = 1
            """)

            user_files = cursor.fetchall()
        except Exception as e:
            print(
                f"Error finding scorecard files in Snowflake external stage: {e}")
            raise

        if not user_files:
            print("No scorecard files found in Snowflake external stage")
            return None

        loaded_files = []

        # Load each user's latest file into stage table
        for user_name, file_name, file_last_modified in user_files:
            print(f"Loading data for user: {user_name}")

            # Copy user's file into stage table
            try:
                cursor.execute(f"""
                    COPY INTO {config['table_name']} (raw_data, user_name, file_name)
                    FROM (
                        SELECT
                            $1 AS raw_data,
                            '{user_name}' AS user_name,
                            METADATA$FILENAME AS file_name
                        FROM @{config['stage_name']}/{user_name}/{file_name}
                    )
                    FILE_FORMAT = (TYPE = 'JSON')
                """)
            except Exception as e:
                print(
                    f"Error copying data to main table for {user_name}: {e}")
                raise

            # Add file info to loaded_files list
            loaded_files.append({
                'user': user_name,
                'file': file_name,
                'file_last_modified': file_last_modified
            })

            print(f"  - Loaded data from {file_name}")

        print(f"Successfully loaded data from {len(loaded_files)} files!")

        return {
            'files_loaded': loaded_files
        }

    except Exception as e:
        print(f"Error loading scorecards: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def load_to_snowflake():
    """Main function to setup and load data to Snowflake."""
    try:
        print("Starting Snowflake integration...")

        # Step 1: Setup external stage and file format
        setup_external_stage()

        # Step 2: Create table if it doesn't exist
        create_scorecards_table()

        # Step 3: Load the latest data
        result = load_latest_scorecards()

        if result:
            print(f"Snowflake integration completed successfully!")
            print(f"   - Files processed:")
            for file_info in result['files_loaded']:
                print(
                    f"     * {file_info['user']}: {file_info['file']}")
            return result
        else:
            print("No files found in S3 stage")
            return None

    except Exception as e:
        print(f"Snowflake integration failed: {e}")
        raise
