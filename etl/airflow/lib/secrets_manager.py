#!/usr/bin/env python3
"""
AWS Secrets Manager integration for retrieving user credentials.
This module handles fetching usernames and passwords from AWS Secrets Manager.
"""

import os
import json
import boto3
from typing import Dict, List, Optional
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class SecretsManager:
    """Handles AWS Secrets Manager operations for user credentials"""

    def __init__(self, secret_name: Optional[str] = None, region: Optional[str] = None):
        """
        Initialize the AWS Secrets Manager client.
        """
        self.secret_name = secret_name or os.getenv(
            'AWS_SECRETS_MANAGER_NAME', 'udisc-users')
        self.region = region or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.client = None

    def _get_client(self):
        """Get AWS Secrets Manager client."""
        if self.client is None:
            try:
                session = boto3.Session(
                    aws_access_key_id=os.getenv(
                        'AWS_SECRETS_MANAGER_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv(
                        'AWS_SECRETS_MANAGER_SECRET_ACCESS_KEY'),
                    region_name=self.region
                )
                self.client = session.client('secretsmanager')
            except Exception as e:
                print(f"Error creating Secrets Manager client: {e}")
                return None
        return self.client

    def get_user_credentials(self) -> List[Dict[str, str]]:
        """
        Retrieve user credentials from AWS Secrets Manager.
        """
        client = self._get_client()
        if not client:
            print("Failed to create AWS Secrets Manager client")
            return []

        try:
            response = client.get_secret_value(SecretId=self.secret_name)
            if 'SecretString' in response:
                secret_data = json.loads(response['SecretString'])
                users_data = secret_data.get('users', [])

                # Handle case where users might be stored as a string
                if isinstance(users_data, str):
                    try:
                        users_data = json.loads(users_data)
                    except json.JSONDecodeError:
                        print("Warning: Could not parse users data as JSON")
                        return []

                return users_data
            return []
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(
                    f"Secret '{self.secret_name}' not found in AWS Secrets Manager")
                return []
            print(f"Error retrieving users: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error: {e}")
            return []

    def test_connection(self) -> bool:
        """
        Test the connection to AWS Secrets Manager.
        """
        try:
            client = self._get_client()
            if not client:
                return False

            # Try to get the secret to test connection
            response = client.get_secret_value(SecretId=self.secret_name)
            return 'SecretString' in response
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False


def get_user_credentials() -> List[Dict[str, str]]:
    """
    Convenience function to get user credentials from AWS Secrets Manager.
    """
    secrets_manager = SecretsManager()
    return secrets_manager.get_user_credentials()


def test_aws_connection() -> bool:
    """
    Test AWS Secrets Manager connection.
    """
    secrets_manager = SecretsManager()
    return secrets_manager.test_connection()


if __name__ == '__main__':
    # Test the module
    print("Testing AWS Secrets Manager integration...")

    if test_aws_connection():
        print("Successfully connected to AWS Secrets Manager")

        credentials = get_user_credentials()
        print(f"Found {len(credentials)} users:")

        for user in credentials:
            print(f"  - {user.get('username', 'Unknown')}")

    else:
        print("Failed to connect to AWS Secrets Manager")
        print("Please check your AWS credentials and configuration")
