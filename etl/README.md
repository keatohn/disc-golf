# Disc Golf ETL Pipeline

ETL pipeline that fetches disc golf scorecard data, uploads it to S3, and loads it into Snowflake for analysis.

## Features

- **Scorecard Data**: Automated fetching of scorecard data with concurrent processing
- **PDGA Data**: Player ratings and tournament results
- **User Management**: Multi-user support with secure credential management via AWS Secrets Manager
- **S3 Storage**: Automated upload to S3 for file staging and historical storage in the cloud
- **Snowflake Integration**: Automated loading of data into Snowflake for analysis and user management
- **Email Notifications**: Success and failure notifications via email
- **Docker Support**: Containerized deployment with Docker Compose
- **Scheduled Execution**: Weekly automated runs via Airflow on Mondays at 6am
- **Incremental Loading**: Support for full vs incremental data loading based on LOAD_TYPE

## Project Structure

```
disc-golf-etl/
├── airflow/
│   ├── dags/
│   │   └── disc_golf_etl_dag.py    # Main Airflow DAG
│   └── lib/
│       ├── __init__.py
│       ├── api.py                  # API client
│       ├── create_snowflake_connection.py # Flexible Snowflake connection utility
│       ├── create_snowflake_users.py # Snowflake user management
│       ├── fetch_scorecards.py     # Scorecard fetching with incremental and concurrent processing
│       ├── load_to_snowflake.py    # Snowflake loading
│       ├── login.py                # Login functionality
│       ├── pdga_scraper.py         # General PDGA data scraper
│       ├── pdga_tournament_scraper.py # Tournament-specific PDGA scraper
│       ├── pdga_user_scraper.py    # PDGA user data fetcher
│       ├── upload_to_s3.py         # S3 upload functionality
│       └── user_manager.py         # User management with AWS Secrets Manager
├── scripts/
│   └── get-password.sh            # Airflow password retrieval
├── docker-compose.yaml            # Docker Compose configuration
├── requirements.txt               # Python dependencies
├── setup.sh                      # Initial setup script
└── README.md                     # This file
```

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd disc-golf-etl
   ```

2. **Run the setup script**:
   ```bash
   ./setup.sh
   ```

3. **Create and configure environment variables**:
   Create a `.env` file in the project root with your credentials:
   ```bash
   # Airflow Core Configuration
   AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth
   AIRFLOW__API__MAXIMUM_PAGE_LIMIT=100
   AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300
   AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
   AIRFLOW__CORE__EXECUTOR=LocalExecutor
   AIRFLOW__CORE__FERNET_KEY=your_fernet_key
   AIRFLOW__CORE__LOAD_EXAMPLES=False
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
   AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=True
   AIRFLOW__WEBSERVER__LOG_FETCH_TIMEOUT_SEC=60
   AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=300

   # Airflow SMTP Configuration (for email notifications)
   AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
   AIRFLOW__SMTP__SMTP_PORT=587
   AIRFLOW__SMTP__SMTP_USER=your_email@gmail.com
   AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password
   AIRFLOW__SMTP__SMTP_STARTTLS=True
   AIRFLOW__SMTP__SMTP_SSL=False
   AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@gmail.com
   AIRFLOW__SMTP__SMTP_MAIL_BACKEND=smtp
   AIRFLOW__SMTP__SMTP_TIMEOUT=30
   AIRFLOW__SMTP__SMTP_RETRY_LIMIT=5

   # Python Path
   PYTHONPATH=/opt/airflow:/opt/airflow/lib

   # Airflow UID (optional, defaults to 50000)
   AIRFLOW_UID=50000

   # AWS Configuration
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_S3_BUCKET=your_bucket_name

     # ETL Configuration
   LOAD_TYPE=full  # Options: 'full' or 'incremental'

   # Snowflake Configuration
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_SCHEMA=your_schema
   SNOWFLAKE_STAGE_NAME=your_stage_name
   SNOWFLAKE_TABLE_NAME=your_table_name

   # UDisc Users (JSON array)
   UDISC_USERS='[
     {
       "name": "user1",
       "display_name": "User 1", 
       "username": "your_user1_username",
       "email": "user1@example.com",
       "pdga_id": "12345",
       "role": "viewer"
     }
   ]'
   ```

   **Note:** For Gmail, you'll need to:
   1. Enable 2-Step Verification in your Google Account
   2. Generate an App Password: Go to Security → 2-Step Verification → App passwords
   3. Select "Mail" as the app type
   4. Use the generated 16-character password (with spaces) as `AIRFLOW__SMTP__SMTP_PASSWORD`

4. **Start Airflow**:
   ```bash
   docker-compose up -d
   ```

5. **Access Airflow UI**:
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: Run `./scripts/get-password.sh` to get the password

## Usage

### Running the Pipeline

1. **Access Airflow UI**: http://localhost:8080
2. **Navigate to DAGs**: Find `disc_golf_etl` in the DAG list
3. **Trigger DAG**: Click "Trigger DAG" to run the pipeline manually
4. **Monitor Execution**: Check the task logs for progress and results

### Data Flow

1. **Fetch & Upload**: Scorecard data is fetched and uploaded to S3
2. **Load**: Latest data is loaded into Snowflake for analysis
3. **User Management**: Snowflake users are created based on configuration
4. **Notify**: Success/failure notifications are sent via email

**Note**: User creation runs concurrently with the data pipeline for efficiency.

## User Management

Configure users in the `UDISC_USERS` environment variable as a JSON array:

```json
[
  {
    "name": "user1",
    "display_name": "User 1",
    "username": "your_username",
    "email": "user@example.com",
    "pdga_id": "12345",
    "role": "viewer"
  }
]
```

See https://github.com/keatohn/disc-golf-user-app for more info on user credential management and storage.

**Required fields**: `name` (lowercase), `display_name`, `username`
**Optional fields**: `email`, `pdga_id`, `role` (can be "viewer" or "developer")