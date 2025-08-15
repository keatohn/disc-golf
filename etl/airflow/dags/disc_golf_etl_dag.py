import json
import os
import sys
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow import PythonOperator
from airflow import send_email
from lib.user_manager import get_user_manager
from lib.create_snowflake_users import create_all_snowflake_users
from lib.load_to_snowflake import load_to_snowflake
from lib.upload_to_s3 import upload_all_scorecards
from lib.fetch_scorecards import fetch_scorecards


# Check if email is configured
EMAIL_ENABLED = bool(
    os.getenv('AIRFLOW__SMTP__SMTP_USER') and
    os.getenv('AIRFLOW__SMTP__SMTP_PASSWORD') and
    os.getenv('AIRFLOW__SMTP__SMTP_PASSWORD') != 'your_app_password_here'
)

if not EMAIL_ENABLED:
    print("Email notifications disabled - Missing AIRFLOW__SMTP__SMTP_USER or AIRFLOW__SMTP__SMTP_PASSWORD")

# Default arguments for the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': [os.getenv('AIRFLOW__SMTP__SMTP_USER')] if EMAIL_ENABLED else [],
    'email_on_failure': EMAIL_ENABLED,
    'email_on_retry': False,
}

# Create the DAG
dag = DAG(
    'disc_golf_etl',
    default_args=default_args,
    description='Disc Golf ETL Pipeline',
    schedule='0 6 * * 1',  # Run weekly on Mondays at 6 AM
    catchup=False,
    tags=['disc-golf', 'etl'],
)


def fetch_and_upload_scorecards_task(**context):
    """Fetch scorecard data from UDisc API and upload to S3"""
    try:
        # Get user summary for logging
        user_manager = get_user_manager()
        summary = user_manager.get_user_summary()

        print("User Configuration Status:")
        for name, info in summary.items():
            status = "configured" if info['configured'] else "not configured"
            print(f"  {name}: {status}")

        print("Fetching data from UDisc API")

        # Fetch scorecards for all configured users
        scorecards_data = fetch_scorecards()

        print(
            f"Successfully fetched scorecards from API: {list(scorecards_data.keys())}")

        # Upload scorecards to S3
        results = upload_all_scorecards(scorecards_data)
        print(f"Successfully uploaded scorecards to S3: {results}")

        return results
    except Exception as e:
        print(f"Error in fetch and upload task: {e}")
        raise e


def load_to_snowflake_task(**context):
    """Load scorecard data from S3 to Snowflake"""
    try:
        # Load data to Snowflake
        result = load_to_snowflake()

        if result:
            print(f"Successfully loaded data to Snowflake: {result}")
        else:
            print("No files found in S3 stage")

        return result
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
        raise e


def create_snowflake_users_task(**context):
    """Create Snowflake users based on configuration"""
    try:
        print("Creating Snowflake users...")
        result = create_all_snowflake_users()

        if result:
            print("Successfully created all Snowflake users")
        else:
            print("Failed to create some Snowflake users")

        return result
    except Exception as e:
        print(f"Error creating Snowflake users: {e}")
        raise e


def notify_success(**context):
    """Send success notification email"""
    if not EMAIL_ENABLED:
        print("Email notifications disabled")
        return "Email notifications disabled"

    try:
        ti = context['ti']
        upload_results = ti.xcom_pull(task_ids='fetch_and_upload')
        snowflake_results = ti.xcom_pull(task_ids='load_to_snowflake')
        user_creation_results = ti.xcom_pull(task_ids='create_snowflake_users')

        subject = "Disc Golf ETL Pipeline - Success"

        # Format Snowflake results nicely
        snowflake_summary = ""
        if snowflake_results:
            snowflake_summary = f"<strong>Total Records:</strong> {snowflake_results.get('total_records', 'N/A')}<br>"
            if 'files_loaded' in snowflake_results:
                snowflake_summary += "<strong>Files Processed:</strong><br>"
                for file_info in snowflake_results['files_loaded']:
                    snowflake_summary += f"• {file_info['user']}: {file_info['records']} records from {file_info['file']}<br>"

        html_content = f"""
        <h2>Disc Golf ETL Pipeline Completed Successfully</h2>
        <p><strong>Uploaded to S3:</strong> {upload_results}</p>
        <p><strong>Loaded to Snowflake:</strong><br>{snowflake_summary}</p>
        <p><strong>Snowflake Users Created:</strong> {user_creation_results}</p>
        <p><strong>Execution Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        """

        send_email(
            to=default_args['email'],
            subject=subject,
            html_content=html_content,
            mime_charset='utf-8'
        )

        return "Success notification sent"
    except Exception as e:
        print(f"Failed to send success email notification: {e}")
        return f"Email notification failed: {e}"


def notify_failure(**context):
    """Send failure notification email"""
    if not EMAIL_ENABLED:
        print("Email notifications disabled")
        return "Email notifications disabled"

    try:
        ti = context['ti']
        task_instance = context['task_instance']

        subject = "Disc Golf ETL Pipeline - Failure"
        html_content = f"""
        <h2>Disc Golf ETL Pipeline Failed</h2>
        <p><strong>Failed Task:</strong> {task_instance.task_id}</p>
        <p><strong>Execution Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>DAG Run ID:</strong> {context['dag_run'].run_id}</p>
        <p>Check the Airflow UI for detailed logs.</p>
        """

        send_email(
            to=default_args['email'],
            subject=subject,
            html_content=html_content,
            mime_charset='utf-8'
        )

        return "Failure notification sent"
    except Exception as e:
        print(f"Failed to send failure email notification: {e}")
        return f"Email notification failed: {e}"


# Define tasks
fetch_and_upload_scorecards_task = PythonOperator(
    task_id='fetch_and_upload_scorecards',
    python_callable=fetch_and_upload_scorecards_task,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake_task,
    dag=dag,
)

create_users_task = PythonOperator(
    task_id='create_snowflake_users',
    python_callable=create_snowflake_users_task,
    dag=dag,
)

email_success = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    trigger_rule='all_success',
    dag=dag,
)

email_failure = PythonOperator(
    task_id='notify_failure',
    python_callable=notify_failure,
    trigger_rule='one_failed',
    dag=dag,
)

# Define task dependencies
create_users_task >> email_success
fetch_and_upload_scorecards_task >> load_task >> email_success
fetch_and_upload_scorecards_task >> email_failure
load_task >> email_failure
create_users_task >> email_failure
