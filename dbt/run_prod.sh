#!/bin/bash

# Simple production deployment script for Disc Golf dbt
echo "🚀 Deploying dbt models to production..."

# Activate virtual environment
source .venv/bin/activate

# Determine load type from environment variable
LOAD_TYPE=${LOAD_TYPE:-incremental}
echo "📊 Running dbt models in production (${LOAD_TYPE} mode)..."

if [ "$LOAD_TYPE" = "full" ]; then
    dbt run --target prod --full-refresh
    echo "🔄 Full refresh mode enabled"
else
    dbt run --target prod
    echo "📈 Incremental mode enabled"
fi

# Run tests to ensure data quality
echo "🧪 Running data tests..."
dbt test --target prod

echo "✅ Production deployment complete!"
echo "📈 Models are now available in the DW schema"
