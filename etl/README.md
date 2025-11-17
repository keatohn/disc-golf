# Disc Golf ETL Pipeline

ETL pipeline that fetches disc golf scorecard data, writes it to local Parquet files, and loads it into DuckDB for analysis.

## Features

- **Scorecard Data**: Automated fetching of scorecard data with concurrent processing
- **PDGA Data**: Player ratings and tournament results
- **User Management**: Multi-user support with secure credential management via AWS Secrets Manager
- **Parquet Storage**: Automated writing to local Parquet files for efficient data storage
- **DuckDB Warehouse**: Data loading into DuckDB for fast, local transformations
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
│       ├── fetch_scorecards.py     # Scorecard fetching with incremental and concurrent processing
│       ├── load_to_duckdb.py       # DuckDB loading
│       ├── login.py                # Login functionality
│       ├── pdga_scraper.py         # General PDGA data scraper
│       ├── pdga_tournament_scraper.py # Tournament-specific PDGA scraper
│       ├── pdga_user_scraper.py    # PDGA user data fetcher
│       ├── write_to_parquet.py     # Parquet file writing
│       └── user_manager.py         # User management with AWS Secrets Manager
├── data/                          # Local data storage (gitignored)
│   ├── warehouse.duckdb           # DuckDB database file
│   └── {user_name}/               # User-specific Parquet files
├── scripts/
│   └── get-password.sh            # Airflow password retrieval
├── docker-compose.yaml            # Docker Compose configuration
├── requirements.txt               # Python dependencies
├── setup.sh                       # Initial setup script
└── README.md                      # This file
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

   # AWS Secrets Manager Configuration
   AWS_SECRETS_MANAGER_ACCESS_KEY_ID=your_access_key
   AWS_SECRETS_MANAGER_SECRET_ACCESS_KEY=your_secret_key
   AWS_SECRETS_MANAGER_NAME=udisc-users
   AWS_DEFAULT_REGION=us-east-1

   # ETL Configuration
   LOAD_TYPE=full  # Options: 'full' or 'incremental'

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

1. **Fetch & Write**: Scorecard data is fetched and written to local Parquet files
2. **Load**: Latest Parquet files are loaded into DuckDB warehouse
3. **Transform**: dbt models transform raw data into dimensional model
4. **Notify**: Success/failure notifications are sent via email

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

## Deployment

### For Streamlit Dashboard

The `data/warehouse.duckdb` file contains all transformed data and can be deployed with a Streamlit app:

1. Run the ETL pipeline to generate/update `warehouse.duckdb`
2. Copy `warehouse.duckdb` to your Streamlit app directory
3. Deploy your Streamlit app (Streamlit Cloud, Render, etc.)
4. The app can read directly from the DuckDB file - no external database needed!

**Benefits**:
- Zero ongoing database costs
- Fast query performance
- Simple deployment (just one file)
- Re-run pipeline and redeploy when you want fresh data