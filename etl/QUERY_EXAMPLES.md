# Querying the DuckDB Warehouse

The DuckDB warehouse is located at `etl/data/warehouse.duckdb`. Here are several ways to query it:

## Method 1: Using Python (in Docker container)

```bash
# Connect to the Airflow container
docker-compose exec airflow-webserver python3

# Then in Python:
import duckdb
conn = duckdb.connect('/opt/airflow/data/warehouse.duckdb')

# Show tables
conn.execute("SHOW TABLES").show()

# Query data
conn.execute("SELECT * FROM raw_udisc_scorecards LIMIT 5").show()

# Close connection
conn.close()
```

## Method 2: Using Python Script (install duckdb locally)

First install duckdb:
```bash
pip install duckdb
```

Then create a script:
```python
import duckdb

# Connect to the database
conn = duckdb.connect('etl/data/warehouse.duckdb')

# Example queries
result = conn.execute("""
    SELECT 
        user_name,
        COUNT(*) as scorecard_count
    FROM raw_udisc_scorecards
    GROUP BY user_name
""").fetchall()

for row in result:
    print(f"{row[0]}: {row[1]} scorecards")

conn.close()
```

## Method 3: Using DuckDB CLI

Install DuckDB CLI:
```bash
# macOS
brew install duckdb

# Or download from https://duckdb.org/docs/installation/
```

Then query:
```bash
duckdb etl/data/warehouse.duckdb

# In the DuckDB prompt:
SHOW TABLES;
SELECT * FROM raw_udisc_scorecards LIMIT 10;
```

## Method 4: Using dbt (after models are run)

Once dbt models are successfully run, you can query the transformed tables:

```bash
cd dbt
dbt run --target prod

# Then query through dbt or directly:
duckdb ../etl/data/warehouse.duckdb
SELECT * FROM dim_player LIMIT 10;
```

## Common Query Examples

### 1. View all scorecards
```sql
SELECT * FROM raw_udisc_scorecards LIMIT 10;
```

### 2. Count records by user
```sql
SELECT user_name, COUNT(*) as count 
FROM raw_udisc_scorecards 
GROUP BY user_name;
```

### 3. Extract fields from JSON
```sql
SELECT 
    user_name,
    raw_data->>'objectId' as scorecard_id,
    raw_data->>'createdAt' as created_at,
    raw_data->>'updatedAt' as updated_at
FROM raw_udisc_scorecards
LIMIT 10;
```

### 4. Get scorecard details
```sql
SELECT 
    user_name,
    raw_data->>'objectId' as scorecard_id,
    raw_data->>'course'->>'name' as course_name,
    raw_data->>'layout'->>'name' as layout_name,
    raw_data->>'date' as date
FROM raw_udisc_scorecards
WHERE user_name = 'keaton'
ORDER BY raw_data->>'createdAt' DESC
LIMIT 10;
```

### 5. Count scorecards by date
```sql
SELECT 
    DATE(raw_data->>'createdAt') as date,
    COUNT(*) as count
FROM raw_udisc_scorecards
GROUP BY DATE(raw_data->>'createdAt')
ORDER BY date DESC;
```

### 6. Get latest loaded files
```sql
SELECT 
    file_name, 
    loaded_at, 
    COUNT(*) as records
FROM raw_udisc_scorecards
GROUP BY file_name, loaded_at
ORDER BY loaded_at DESC;
```

### 7. Query nested JSON fields
```sql
SELECT 
    user_name,
    raw_data->>'objectId' as scorecard_id,
    raw_data->'entries'->0->>'total' as total_score,
    raw_data->'entries'->0->'players'->0->>'name' as player_name
FROM raw_udisc_scorecards
WHERE raw_data->'entries' IS NOT NULL
LIMIT 10;
```

## Using with Python Pandas

```python
import duckdb
import pandas as pd

conn = duckdb.connect('etl/data/warehouse.duckdb')

# Query and convert to pandas DataFrame
df = conn.execute("""
    SELECT 
        user_name,
        raw_data->>'objectId' as scorecard_id,
        raw_data->>'createdAt' as created_at
    FROM raw_udisc_scorecards
""").df()

print(df.head())
conn.close()
```

## Using with Jupyter Notebook

```python
import duckdb
import pandas as pd

# Connect
conn = duckdb.connect('../etl/data/warehouse.duckdb')

# Query and explore
df = conn.execute("SELECT * FROM raw_udisc_scorecards LIMIT 100").df()
df.head()
```

