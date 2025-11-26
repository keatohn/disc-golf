#!/usr/bin/env python3
"""
Example script to query the DuckDB warehouse.
"""

import duckdb
from pathlib import Path

# Path to the warehouse database
db_path = Path(__file__).parent / 'data' / 'warehouse.duckdb'

if not db_path.exists():
    print(f"Database not found at {db_path}")
    exit(1)

# Connect to the database
conn = duckdb.connect(str(db_path))

print("=" * 60)
print("DuckDB Warehouse Query Examples")
print("=" * 60)

# Show tables
print("\n1. Available tables:")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    print(f"   - {table[0]}")

if not tables:
    print("   No tables found. Run the ETL pipeline first.")
    conn.close()
    exit(0)

# Show schema
print("\n2. Schema of raw_udisc_scorecards:")
conn.execute("DESCRIBE raw_udisc_scorecards").show()

# Count records
print("\n3. Record counts:")
count = conn.execute("SELECT COUNT(*) FROM raw_udisc_scorecards").fetchone()[0]
print(f"   Total records: {count}")

count_by_user = conn.execute("""
    SELECT user_name, COUNT(*) as count 
    FROM raw_udisc_scorecards 
    GROUP BY user_name
""").fetchall()
print("\n   Records by user:")
for user, cnt in count_by_user:
    print(f"   - {user}: {cnt}")

# Example queries
print("\n4. Example Queries:")
print("\n   a) Get all scorecards for a specific user:")
print("      SELECT * FROM raw_udisc_scorecards WHERE user_name = 'keaton' LIMIT 5;")

print("\n   b) Extract specific fields from JSON:")
print("      SELECT ")
print("          user_name,")
print("          raw_data->>'objectId' as scorecard_id,")
print("          raw_data->>'createdAt' as created_at,")
print("          raw_data->>'updatedAt' as updated_at")
print("      FROM raw_udisc_scorecards")
print("      LIMIT 10;")

print("\n   c) Count scorecards by date:")
print("      SELECT ")
print("          DATE(raw_data->>'createdAt') as date,")
print("          COUNT(*) as count")
print("      FROM raw_udisc_scorecards")
print("      GROUP BY DATE(raw_data->>'createdAt')")
print("      ORDER BY date DESC;")

print("\n   d) Get latest loaded files:")
print("      SELECT file_name, loaded_at, COUNT(*) as records")
print("      FROM raw_udisc_scorecards")
print("      GROUP BY file_name, loaded_at")
print("      ORDER BY loaded_at DESC;")

# Run a sample query
print("\n5. Sample Query Results:")
print("\n   Latest 3 scorecards:")
sample = conn.execute("""
    SELECT 
        user_name,
        raw_data->>'objectId' as scorecard_id,
        raw_data->>'createdAt' as created_at,
        file_name
    FROM raw_udisc_scorecards
    ORDER BY loaded_at DESC
    LIMIT 3
""").fetchall()

for row in sample:
    print(f"   User: {row[0]}, ID: {row[1][:20]}..., Created: {row[2][:19] if row[2] else 'N/A'}")

print("\n" + "=" * 60)
print("To run custom queries, use:")
print("  conn = duckdb.connect('data/warehouse.duckdb')")
print("  result = conn.execute('YOUR SQL QUERY').fetchall()")
print("=" * 60)

conn.close()

