# Databricks Toolkit CLI Commands

This document outlines the CLI tools available in the `cli/` directory for the Databricks Toolkit.

---

## CLI Entry Points

### `dbfs_cli.py`

**Description**  
Command-line wrapper around `explore_dbfs_path()` to inspect DBFS directories.

**Usage**
```bash
python cli/dbfs_cli.py --path dbfs:/databricks-datasets --no-recursive --limit 20
```

**Options**
- `--path`: DBFS path to explore.
- `--files-only`: Only return file paths.
- `--no-recursive`: Skip recursive traversal of subdirectories.
- `--limit`: Maximum number of paths to return.
- `--offset`: Number of paths to skip (for pagination).

---

### `query_file.py`

**Description**  
Preview a file from DBFS using Spark SQL with format-specific logic.

**Usage**
```bash
python cli/query_file.py --file /databricks-datasets/path/file.csv --format csv --limit 10
```

**Options**
- `--file`: File path to read (absolute DBFS path).
- `--format`: Format of the file (`csv`, `parquet`, `delta`, etc.).
- `--limit`: Number of rows to preview (default: 10).

**Example**
```bash
python cli/query_file.py --file /databricks-datasets/wine-quality/winequality-red.csv --format csv --limit 5
```

---

### `analyze_dataset.py`

**Description**  
Comprehensive dataset analysis with data quality metrics, schema information, and statistical summaries.

**Usage**
```bash
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/ --name "Retail Customers" --max-rows 5000
```

**Options**
- `--path`: Path to the dataset (DBFS path or table name).
- `--name`: Optional name for the dataset.
- `--max-rows`: Maximum rows to analyze for performance (default: 10000).

**What you get:**
- 📊 Basic information (rows, columns, data types)
- 📋 Detailed schema analysis
- 🔍 Data quality metrics (nulls, distinct values, empty strings)
- 📄 Sample data (first and last 5 rows)
- 📈 Statistical summary for numeric columns
- 🔬 Column-by-column analysis

**Examples**
```bash
# Analyze a DBFS dataset
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/ --name "Retail Customers"

# Analyze a table
python cli/analyze_dataset.py --path retail_customers_bronze --name "Bronze Table"

# Analyze with custom row limit
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/nyctaxi/tripdata/ --max-rows 2000
```

---

### `bronze_ingestion.py`

**Description**  
Ingest raw data into bronze layer tables with metadata tracking.

**Usage**
```bash
python cli/bronze_ingestion.py --source-path dbfs:/databricks-datasets/retail-org/customers/ --bronze-table-name retail_customers_bronze --project-name retail
```

**Options**
- `--source-path`: Path to source data (DBFS path or table name).
- `--bronze-table-name`: Name for the bronze table.
- `--project-name`: Project name for organization (default: default).

**What it does:**
- 📁 Loads data from various formats (CSV, Parquet, Delta, JSON)
- 🏷️ Creates bronze table with metadata columns
- 📊 Adds ingestion timestamp, source path, and layer info
- ✅ Provides table information and statistics

**Examples**
```bash
# Ingest from DBFS
python cli/bronze_ingestion.py --source-path dbfs:/databricks-datasets/retail-org/customers/ --bronze-table-name retail_customers_bronze

# Ingest from existing table
python cli/bronze_ingestion.py --source-path existing_table --bronze-table-name new_bronze_table --project-name ecommerce
```

---

### `drop_table.py`

**Description**  
Drop tables to avoid schema conflicts during development and testing.

**Usage**
```bash
python cli/drop_table.py --table-name your_table_name
```

**Options**
- `--table-name`: Name of the table to drop.

**What it does:**
- 🗑️ Safely drops table if it exists
- ✅ Uses `DROP TABLE IF EXISTS` for safety
- 📝 Provides clear feedback on success/failure

**Examples**
```bash
# Drop a specific table
python cli/drop_table.py --table-name retail_customers_bronze

# Drop test tables
python cli/drop_table.py --table-name test_table_001
```