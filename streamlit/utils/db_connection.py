"""
Database connection utilities for Streamlit app.
"""

import duckdb
from pathlib import Path


def get_db_connection():
    """
    Get connection to DuckDB warehouse.
    
    Returns:
        duckdb.DuckDBPyConnection: Database connection
    """
    # Path to warehouse relative to streamlit folder
    db_path = Path(__file__).parent.parent.parent / 'etl' / 'data' / 'warehouse.duckdb'
    
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found at {db_path}. "
            "Make sure the ETL pipeline has been run to create the warehouse."
        )
    
    return duckdb.connect(str(db_path))


def get_table_info(conn, schema='analytics'):
    """
    Get information about tables in a schema.
    
    Args:
        conn: DuckDB connection
        schema: Schema name (default: 'analytics')
    
    Returns:
        list: List of table names
    """
    try:
        tables = conn.execute(f"SHOW TABLES FROM {schema}").fetchall()
        return [table[0] for table in tables]
    except Exception as e:
        return []


def get_table_schema(conn, schema, table):
    """
    Get schema information for a table.
    
    Args:
        conn: DuckDB connection
        schema: Schema name
        table: Table name
    
    Returns:
        list: List of (column_name, column_type) tuples
    """
    try:
        schema_info = conn.execute(f"DESCRIBE {schema}.{table}").fetchall()
        return schema_info
    except Exception as e:
        return []

