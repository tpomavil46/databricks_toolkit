# Tools Directory

This directory contains utility scripts and tools for the Databricks Toolkit.

## Available Tools

### `create_test_data.py`
Creates test data for SQL-driven pipeline demonstration.
- **Usage:** `python tools/create_test_data.py`
- **Purpose:** Generates sample test data for development and testing

### `check_tables.py`
Utility to check available tables in the default schema.
- **Usage:** `python tools/check_tables.py`
- **Purpose:** Lists all tables in the current schema and checks for common sample datasets

### `find_datasets.py`
Simple script to find available datasets using databricks-connect.
- **Usage:** `python tools/find_datasets.py`
- **Purpose:** Searches for available datasets in DBFS and lists common dataset paths

### `test_sql_library.py`
Tests for SQL library functionality.
- **Usage:** `python tools/test_sql_library.py`
- **Purpose:** Demonstrates and tests the SQL library components (patterns, quality checks, functions, templates)

## Usage Examples

```bash
# Create test data
python tools/create_test_data.py

# Check available tables
python tools/check_tables.py

# Find available datasets
python tools/find_datasets.py

# Test SQL library
python tools/test_sql_library.py
```

## Notes

- All tools are designed to work with Databricks Connect
- Tools use the default cluster configuration
- Some tools may require specific permissions or data access 