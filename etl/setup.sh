#!/bin/bash

echo "Setting up Disc Golf Data Pipeline..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env template..."
    cat > .env << EOF
# UDisc Users (JSON format) - passwords come from AWS Secrets Manager
UDISC_USERS='[
  {
    "name": "user1",
    "display_name": "User 1",
    "username": "your_udisc_username",
    "email": "user@example.com",
    "pdga_id": "12345",
    "role": "developer"
  },
  {
    "name": "user2", 
    "display_name": "User 2",
    "username": "another_username",
    "pdga_id": "67890",
    "role": "viewer"
  }
]'

# AWS Secrets Manager Configuration
AWS_SECRETS_MANAGER_ACCESS_KEY_ID=your_access_key
AWS_SECRETS_MANAGER_SECRET_ACCESS_KEY=your_secret_key
AWS_SECRETS_MANAGER_NAME=udisc-users
AWS_DEFAULT_REGION=us-east-1

# AWS S3 Configuration
AWS_S3_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key

# ETL Configuration
LOAD_TYPE=incremental

# Snowflake Configuration
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_STAGE_NAME=your_stage_name
SNOWFLAKE_TABLE_NAME=your_table_name

echo "Created .env template. Please update with your actual values."
else
    echo ".env file already exists."
fi

# Install dependencies
echo "Installing Python dependencies..."
source .venv/bin/activate
pip install -r requirements.txt

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p airflow/logs

echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Update .env file with your actual credentials"
echo "2. Start Airflow: docker-compose up -d"
echo "3. Access Airflow UI: http://localhost:8080"
echo "4. Trigger the disc_golf_etl DAG to test the pipeline"
