"""
Create Snowflake users based on user configuration.
Creates users if they don't exist and grants appropriate roles.
"""

import os
from typing import List, Optional
from user_manager import get_user_manager, User
from create_snowflake_connection import create_snowflake_connection


def create_snowflake_user(user: User, cursor) -> bool:
    """Create a Snowflake user if they don't exist"""
    try:
        # Skip users without roles
        if not user.role:
            print(f"Skipping user {user.name}. No role specified.")
            return True

        role = user.role.upper()

        # Use accountadmin role for user creation
        cursor.execute("USE ROLE accountadmin")

        # Create user
        create_user_sql = f"""
            CREATE USER IF NOT EXISTS {user.name}
            PASSWORD = '{user.password}'
            DISPLAY_NAME = '{user.display_name}'
            EMAIL = '{user.email or ''}'
            DEFAULT_ROLE = '{role}'
            DEFAULT_WAREHOUSE = 'SNOWSIGHT_WH'
            MUST_CHANGE_PASSWORD = TRUE
        """
        try:
            cursor.execute(create_user_sql)
        except Exception as e:
            print(f"Error creating user {user.name}: {e}")
            return False

        # Grant the role
        grant_sql = f"GRANT ROLE {role} TO USER {user.name}"
        try:
            cursor.execute(grant_sql)
        except Exception as e:
            print(f"Error granting role {role} to user {user.name}: {e}")
            return False

        print(f"Successfully created Snowflake user: {user.name}")
        return True

    except Exception as e:
        print(f"Error creating Snowflake user {user.name}: {e}")
        return False


def create_all_snowflake_users() -> bool:
    """Create Snowflake users for all configured users"""
    user_manager = get_user_manager()
    users = user_manager.get_all_users()

    if not users:
        print("No users configured to create Snowflake accounts for")
        return False

    success_count = 0
    total_count = len(users)

    print(f"Creating Snowflake users for {total_count} users...")

    try:
        conn = create_snowflake_connection()
        cursor = conn.cursor()

        for user in users:
            if create_snowflake_user(user, cursor):
                success_count += 1

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return False

    print(
        f"Successfully created {success_count}/{total_count} Snowflake users")
    return success_count == total_count


if __name__ == "__main__":
    # For manual execution
    print("Setting up Snowflake users and roles...")

    try:
        create_all_snowflake_users()
    except Exception as e:
        print(f"Failed to create Snowflake users: {e}")
