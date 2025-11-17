import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional
from dataclasses import dataclass
from secrets_manager import get_user_credentials
import api


@dataclass
class User:
    """Represents a user with their login credentials and metadata"""
    name: str
    display_name: str
    username: str
    password: str
    user_object_id: Optional[str] = None
    pdga_id: Optional[str] = None
    api_token: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None


class UserManager:
    """Manages user configurations and operations"""

    _unmatched_check_sent = False

    def __init__(self):
        self.users = {}
        self._load_users_from_env()

    def _load_users_from_env(self):
        """Load user configurations from environment variable and merge with AWS credentials"""
        users_config = os.getenv('UDISC_USERS')

        if not users_config:
            print("Warning: UDISC_USERS environment variable not set")
            return

        try:
            users_data = json.loads(users_config)

            # Check if AWS credentials are configured
            aws_access_key = os.getenv('AWS_SECRETS_MANAGER_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRETS_MANAGER_SECRET_ACCESS_KEY')
            use_aws_secrets = bool(
                aws_access_key and aws_secret_key and
                aws_access_key != 'your_access_key' and
                aws_secret_key != 'your_secret_key'
            )

            # Get credentials from AWS Secrets Manager if configured
            aws_credential_lookup = {}
            if use_aws_secrets:
                aws_credentials = get_user_credentials()
                print(
                    f"Retrieved {len(aws_credentials)} credentials from AWS Secrets Manager")

                for cred in aws_credentials:
                    username = cred.get('username', '').strip()
                    if username:
                        aws_credential_lookup[username.lower()] = cred.get(
                            'password', '')

                print(
                    f"Created credential lookup for {len(aws_credential_lookup)} usernames")
            else:
                print(
                    "AWS Secrets Manager not configured - using passwords from UDISC_USERS")

            # Merge metadata with credentials
            for user_data in users_data:
                username = user_data.get('username', '').strip()
                email = user_data.get('email', '').strip()
                pdga_id = user_data.get('pdga_id', '').strip()

                # Get password from AWS Secrets Manager or UDISC_USERS
                password = None
                if use_aws_secrets:
                    if username and username.lower() in aws_credential_lookup:
                        password = aws_credential_lookup[username.lower()]
                    elif email and email.lower() in aws_credential_lookup:
                        password = aws_credential_lookup[email.lower()]
                    else:
                        print(
                            f"No credentials found for user: {username or email}")
                        continue
                else:
                    password = user_data.get('password', '').strip()
                    if not password:
                        print(
                            f"Warning: No password found for user: {username or email}")
                        continue

                # Validate and set role
                role = user_data.get('role')
                if role and role not in ['viewer', 'developer', 'admin']:
                    print(
                        f"Warning: Invalid role '{role}' for user {username}")
                    role = None

                if not role:
                    role = 'viewer'

                user = User(
                    name=user_data['name'],
                    display_name=user_data['display_name'],
                    username=username,
                    password=password,
                    email=email,
                    role=role,
                    pdga_id=pdga_id
                )
                self.users[user.name.upper()] = user

            print(
                f"Successfully loaded {len(self.users)} users with credentials")

            # Check for unmatched AWS secrets users
            if use_aws_secrets:
                check_unmatched = os.getenv(
                    'CHECK_UNMATCHED_AWS_USERS', 'false').lower() == 'true'
                if check_unmatched:
                    self.check_for_unmatched_aws_users()
                else:
                    print(
                        "Skipping unmatched AWS users check (set CHECK_UNMATCHED_AWS_USERS=true to enable)")

        except json.JSONDecodeError as e:
            print(f"Error parsing UDISC_USERS JSON: {e}")
        except KeyError as e:
            print(f"Missing required field in user configuration: {e}")
        except Exception as e:
            print(f"Error loading users: {e}")

    def get_user(self, name: str) -> Optional[User]:
        """Get a user by name"""
        return self.users.get(name.upper())

    def get_all_users(self) -> List[User]:
        """Get all configured users"""
        return list(self.users.values())

    def get_user_summary(self) -> Dict[str, Dict]:
        """Get a summary of all users and their configuration status"""
        summary = {}
        for name, user in self.users.items():
            summary[name] = {
                "display_name": user.display_name,
                "configured": bool(user.username and user.password),
                "has_api_token": bool(user.api_token),
                "has_user_id": bool(user.user_object_id),
                "pdga_id": user.pdga_id,
                "role": user.role or "not specified"
            }
        return summary

    def login_user(self, user: User) -> bool:
        """Login a user to the UDisc API and update their API token and user object ID"""
        if not user.username or not user.password:
            print(f"Missing username or password for {user.display_name}")
            return False

        try:
            response = api.post(
                "/login",
                json={
                    "username": user.username,
                    "password": user.password
                }
            )

            if response.ok:
                login_data = response.json()
                if 'sessionToken' in login_data and 'objectId' in login_data:
                    user.api_token = login_data['sessionToken']
                    user.user_object_id = login_data['objectId']
                    print(f"Successfully logged in {user.display_name}")
                    return True
                else:
                    print(
                        f"Failed to get session token for {user.display_name}")
                    return False
            else:
                print(
                    f"Login failed for {user.display_name} (status = {response.status_code})")
                if response.status_code == 401:
                    error_data = response.json()
                    print(
                        f"Reason: {error_data.get('error', 'Unknown error')}")
                return False

        except Exception as e:
            print(f"Login failed for {user.display_name}: {e}")
            return False

    def _get_aws_credentials_for_comparison(self) -> List[Dict[str, str]]:
        """Get AWS credentials for comparison with UDISC_USERS metadata"""
        try:
            return get_user_credentials()
        except Exception as e:
            print(
                f"Warning: Could not retrieve AWS credentials for comparison: {e}")
            return []

    def _send_unmatched_users_email(self, unmatched_users: List[str]) -> bool:
        """Send email notification about unmatched AWS secrets users"""
        try:
            smtp_server = os.getenv(
                'AIRFLOW__SMTP__SMTP_HOST', 'smtp.gmail.com')
            smtp_port = int(os.getenv('AIRFLOW__SMTP__SMTP_PORT', '587'))
            sender_email = os.getenv('AIRFLOW__SMTP__SMTP_USER')
            sender_password = os.getenv('AIRFLOW__SMTP__SMTP_PASSWORD')
            recipient_email = os.getenv('AIRFLOW__SMTP__SMTP_USER')

            if not all([sender_email, sender_password]):
                print(
                    "Warning: Airflow SMTP configuration incomplete. Skipping email notification.")
                return False

            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = recipient_email
            msg['Subject'] = "Disc Golf ETL: Unmatched AWS Secrets Users Detected"

            body = f"""
There are {len(unmatched_users)} user(s) in AWS Secrets Manager 
that do not have matching metadata in the UDISC_USERS environment variable.

Unmatched users:
{chr(10).join([f"- {user}" for user in unmatched_users])}
"""

            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)

            print(
                f"Email notification sent to {recipient_email} about {len(unmatched_users)} unmatched users")
            return True

        except Exception as e:
            print(f"Failed to send email notification: {e}")
            return False

    def check_for_unmatched_aws_users(self) -> List[str]:
        """Check for AWS secrets users that don't match UDISC_USERS metadata"""
        if UserManager._unmatched_check_sent:
            print("Unmatched AWS users check already performed - skipping email")
            return []

        aws_credentials = self._get_aws_credentials_for_comparison()
        udisc_usernames = {user.username.lower()
                           for user in self.users.values() if user.username}
        udisc_emails = {user.email.lower()
                        for user in self.users.values() if user.email}

        unmatched_users = []
        for cred in aws_credentials:
            username = cred.get('username', '').lower()
            if username not in udisc_usernames and username not in udisc_emails:
                unmatched_users.append(username)

        if unmatched_users:
            print(
                f"Warning: Found {len(unmatched_users)} unmatched AWS secrets users: {unmatched_users}")
            if self._send_unmatched_users_email(unmatched_users):
                UserManager._unmatched_check_sent = True
        else:
            print("All AWS secrets users have matching UDISC_USERS metadata")

        return unmatched_users


# Global instance for easy access
user_manager = UserManager()


def get_user_manager() -> UserManager:
    """Get the global user manager instance"""
    return user_manager


def login_user(user: User) -> bool:
    """Convenience function to login a user (calls UserManager.login_user)"""
    return user_manager.login_user(user)
