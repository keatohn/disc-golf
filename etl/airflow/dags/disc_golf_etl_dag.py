import json
import os
import sys
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from lib.user_manager import get_user_manager
from lib.load_to_duckdb import load_to_duckdb
from lib.write_to_parquet import write_all_scorecards
from lib.fetch_scorecards import fetch_all_scorecards


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


def fetch_and_write_scorecards_task(**context):
    """Fetch scorecard data from UDisc API and write to Parquet files"""
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
        scorecards_data = fetch_all_scorecards()

        print(
            f"Successfully fetched scorecards from API: {list(scorecards_data.keys())}")

        # Write scorecards to Parquet files
        results = write_all_scorecards(scorecards_data)
        print(f"Successfully wrote scorecards to Parquet files: {results}")

        return results
    except Exception as e:
        print(f"Error in fetch and write task: {e}")
        raise e


def load_to_duckdb_task(**context):
    """Load scorecard data from Parquet files to DuckDB"""
    try:
        # Load data to DuckDB
        result = load_to_duckdb()

        if result:
            print(f"Successfully loaded data to DuckDB: {result}")
        else:
            print("No Parquet files found")

        return result
    except Exception as e:
        print(f"Error loading data to DuckDB: {e}")
        raise e


def run_dbt_models_task(**context):
    """Run dbt models on DuckDB"""
    try:
        import subprocess
        import os

        # Change to dbt directory
        dbt_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'dbt')
        os.chdir(dbt_dir)

        print(f"Running dbt models from: {dbt_dir}")

        # Determine load type from ETL environment variable
        load_type = os.getenv('LOAD_TYPE', 'incremental').lower()

        # Build dbt command based on load type
        if load_type == 'full':
            dbt_cmd = ['dbt', 'run', '--target', 'prod', '--full-refresh']
            print(f"Running dbt with full refresh (LOAD_TYPE={load_type})")
        else:
            dbt_cmd = ['dbt', 'run', '--target', 'prod']
            print(f"Running dbt incrementally (LOAD_TYPE={load_type})")

        # Run dbt
        result = subprocess.run(
            dbt_cmd,
            capture_output=True,
            text=True,
            cwd=dbt_dir
        )

        if result.returncode == 0:
            print("dbt models run successful")
            return f"dbt models run completed successfully ({load_type} mode)"
        else:
            print(f"dbt models run failed: {result.stderr}")
            raise Exception(f"dbt models run failed: {result.stderr}")

    except Exception as e:
        print(f"Error running dbt models: {e}")
        raise e


def notify_success(**context):
    """Send success notification email"""
    if not EMAIL_ENABLED:
        print("Email notifications disabled")
        return "Email notifications disabled"

    try:
        ti = context['ti']
        write_results = ti.xcom_pull(task_ids='fetch_and_write_scorecards')
        duckdb_results = ti.xcom_pull(task_ids='load_to_duckdb')
        dbt_results = ti.xcom_pull(task_ids='run_dbt_models')

        subject = "Disc Golf ETL Pipeline - Success"

        # Format DuckDB results nicely
        duckdb_summary = ""
        if duckdb_results:
            duckdb_summary = f"<strong>Total Records:</strong> {duckdb_results.get('total_records', 'N/A')}<br>"
            if 'files_loaded' in duckdb_results:
                duckdb_summary += "<strong>Files Processed:</strong><br>"
                for file_info in duckdb_results['files_loaded']:
                    duckdb_summary += f"• {file_info['user']}: {file_info['records']} records from {file_info['file']}<br>"

        html_content = f"""
        <h2>Disc Golf ETL Pipeline Completed Successfully</h2>
        <p><strong>Parquet Files Written:</strong> {write_results}</p>
        <p><strong>Loaded to DuckDB:</strong><br>{duckdb_summary}</p>
        <p><strong>dbt Models:</strong> {dbt_results}</p>
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
fetch_and_write_task = PythonOperator(
    task_id='fetch_and_write_scorecards',
    python_callable=fetch_and_write_scorecards_task,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb_task,
    dag=dag,
)

dbt_models_task = PythonOperator(
    task_id='run_dbt_models',
    python_callable=run_dbt_models_task,
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
fetch_and_write_task >> load_task >> dbt_models_task >> email_success
fetch_and_write_task >> email_failure
load_task >> email_failure
dbt_models_task >> email_failure
