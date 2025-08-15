import os
import snowflake.connector


def create_snowflake_connection(**kwargs):
    """Create Snowflake connection with optional parameter overrides"""
    connection_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA')
    }

    # Override with any passed parameters
    connection_params.update(kwargs)

    return snowflake.connector.connect(**connection_params)
