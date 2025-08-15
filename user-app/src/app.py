#!/usr/bin/env python3
"""
Simple web interface for adding users to AWS Secrets Manager.
This allows administrators to add new users that will be pulled into the ETL process.
"""

import os
import json
import boto3
from datetime import datetime
from flask import Flask, render_template, request, flash, redirect, url_for
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from validate_user_credentials import validate_udisc_credentials

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY')

# AWS configuration
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
SECRETS_MANAGER_NAME = os.getenv('AWS_SECRETS_MANAGER_NAME', 'udisc-users')


def get_secrets_manager_client():
    """Get AWS Secrets Manager client."""
    try:
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_SECRETS_MANAGER_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv(
                'AWS_SECRETS_MANAGER_SECRET_ACCESS_KEY'),
            region_name=AWS_REGION
        )
        return session.client('secretsmanager')
    except Exception as e:
        print(f"Error creating Secrets Manager client: {e}")
        return None


def get_existing_users():
    """Retrieve existing users from AWS Secrets Manager."""
    client = get_secrets_manager_client()
    if not client:
        return []

    try:
        response = client.get_secret_value(SecretId=SECRETS_MANAGER_NAME)
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
            # Secret doesn't exist yet, return empty list
            return []
        print(f"Error retrieving users: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []


def add_user_to_secrets_manager(username, password):
    """Add a new user to AWS Secrets Manager."""
    # First, validate the UDisc credentials
    print(f"Validating UDisc credentials for user: {username}")
    is_valid, validation_message = validate_udisc_credentials(
        username, password)

    if not is_valid:
        return False, validation_message

    client = get_secrets_manager_client()
    if not client:
        return False, "Failed to create AWS client"

    try:
        # Get existing users
        existing_users = get_existing_users()

        # Check if user already exists
        if any(user['username'] == username for user in existing_users):
            return False, f"User '{username}' already exists"

        # Add new user
        new_user = {
            'username': username,
            'password': password,
            'created_at': datetime.now().strftime('%-m/%-d/%Y')
        }

        existing_users.append(new_user)

        # Update the secret - store users as a JSON string to match AWS format
        secret_value = {
            'users': json.dumps(existing_users),
            'last_updated': datetime.now().isoformat()
        }

        try:
            # Try to update existing secret
            client.update_secret(
                SecretId=SECRETS_MANAGER_NAME,
                SecretString=json.dumps(secret_value)
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Secret doesn't exist, create it
                client.create_secret(
                    Name=SECRETS_MANAGER_NAME,
                    SecretString=json.dumps(secret_value),
                    Description='Disc Golf user credentials'
                )
            else:
                raise

        return True, f"User '{username}' added successfully"

    except ClientError as e:
        error_msg = f"AWS error: {e.response['Error']['Message']}"
        print(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(error_msg)
        return False, error_msg


def update_user_password(username, new_password):
    """Update password for an existing user."""
    client = get_secrets_manager_client()
    if not client:
        return False, "Failed to create AWS client"

    try:
        # Get existing users
        existing_users = get_existing_users()

        # Find the user to update
        user_found = False
        for user in existing_users:
            if user['username'] == username:
                # Check if password is the same
                if user['password'] == new_password:
                    return False, "Password is the same as existing password"

                user['password'] = new_password
                user['updated_at'] = datetime.now().isoformat()
                user_found = True
                break

        if not user_found:
            return False, f"User '{username}' not found"

        # Update the secret
        secret_value = {
            'users': json.dumps(existing_users),
            'last_updated': datetime.now().isoformat()
        }

        client.update_secret(
            SecretId=SECRETS_MANAGER_NAME,
            SecretString=json.dumps(secret_value)
        )

        return True, f"Password updated for user '{username}'"

    except ClientError as e:
        error_msg = f"AWS error: {e.response['Error']['Message']}"
        print(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(error_msg)
        return False, error_msg


def verify_user_password(username, password):
    """Verify a user's password."""
    try:
        existing_users = get_existing_users()

        for user in existing_users:
            if user['username'] == username and user['password'] == password:
                return True
        return False
    except Exception as e:
        print(f"Error verifying password: {e}")
        return False


def delete_user_from_secrets_manager(username):
    """Delete a user from AWS Secrets Manager."""
    client = get_secrets_manager_client()
    if not client:
        return False, "Failed to create AWS client"

    try:
        # Get existing users
        existing_users = get_existing_users()

        # Find and remove the user
        user_found = False
        for i, user in enumerate(existing_users):
            if user['username'] == username:
                del existing_users[i]
                user_found = True
                break

        if not user_found:
            return False, f"User '{username}' not found"

        # Update the secret
        secret_value = {
            'users': json.dumps(existing_users),
            'last_updated': datetime.now().isoformat()
        }

        client.update_secret(
            SecretId=SECRETS_MANAGER_NAME,
            SecretString=json.dumps(secret_value)
        )

        return True, f"User '{username}' deleted successfully"

    except ClientError as e:
        error_msg = f"AWS error: {e.response['Error']['Message']}"
        print(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(error_msg)
        return False, error_msg


@app.route('/')
def index():
    """Main page with user form and list of existing users."""
    try:
        existing_users = get_existing_users()
        return render_template('index.html', users=existing_users)
    except Exception as e:
        flash(f"Error loading users: {e}", 'error')
        return render_template('index.html', users=[])


@app.route('/add_user', methods=['POST'])
def add_user():
    """Handle form submission to add a new user."""
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()

    # Basic validation
    if not username or not password:
        flash('Username and password are required', 'error')
        return redirect(url_for('index'))

    # Add user to Secrets Manager
    success, message = add_user_to_secrets_manager(username, password)

    if success:
        flash(message, 'success')
    else:
        flash(message, 'error')

    return redirect(url_for('index'))


@app.route('/update_password', methods=['POST'])
def update_password():
    """Handle form submission to update an existing user's password."""
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()

    # Basic validation
    if not username or not password:
        flash('Username and password are required', 'error')
        return redirect(url_for('index'))

    # Update password in Secrets Manager
    success, message = update_user_password(username, password)

    if success:
        flash(message, 'success')
    elif "Password is the same as existing password" in message:
        flash(message, 'warning')
    else:
        flash(message, 'error')

    return redirect(url_for('index'))


@app.route('/delete_user', methods=['POST'])
def delete_user():
    """Handle form submission to delete an existing user."""
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()

    # Basic validation
    if not username or not password:
        flash('Username and password are required', 'error')
        return redirect(url_for('index'))

    # Verify the user's password before allowing deletion
    if not verify_user_password(username, password):
        flash(
            'Invalid username or password. Deletion requires correct credentials.', 'error')
        return redirect(url_for('index'))

    # Delete user from Secrets Manager
    success, message = delete_user_from_secrets_manager(username)

    if success:
        flash(message, 'success')
    else:
        flash(message, 'error')

    return redirect(url_for('index'))


@app.route('/health')
def health():
    """Health check endpoint."""
    return {'status': 'healthy', 'service': 'disc-golf-user-manager'}


if __name__ == '__main__':
    # Get port from environment or default to 5001
    port = int(os.getenv('PORT', 5001))

    print(f"🚀 Starting Disc Golf User Manager on port {port}")
    print(f"📁 Using Secrets Manager: {SECRETS_MANAGER_NAME}")
    print("⚠️  AWS connection will be tested on first request")

    app.run(host='0.0.0.0', port=port, debug=os.getenv(
        'FLASK_ENV') == 'development')
