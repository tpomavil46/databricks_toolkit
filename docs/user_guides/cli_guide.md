# CLI Guide

Master the command-line interface for the Databricks Toolkit.

## 🎯 Overview

The toolkit provides a comprehensive set of CLI tools for:
- **Pipeline Management**: Run and monitor data pipelines
- **Cost Monitoring**: Track GCP costs and usage
- **Administrative Tasks**: Manage users, clusters, and workspace
- **Data Operations**: Query, explore, and manage data
- **Development Tools**: Testing, validation, and debugging

## 🚀 Quick Reference

### Main Commands

```bash
# Run SQL pipeline
python main.py sql <project> [--environment <env>]

# Run PySpark ETL pipeline
python main.py pyspark <pipeline> [--environment <env>]

# Launch dashboard
streamlit run dashboard/dynamic_dashboard.py

# Monitor costs
make billing-costs YEAR=2025 MONTH=8
```

### Help Commands

```bash
# Get help for any command
python main.py --help
make billing-costs --help

# List available options
python main.py sql --help
```

## 📊 Pipeline Management

### SQL-Driven Workflows

#### Basic Pipeline Execution
```bash
# Run retail pipeline
python main.py sql retail

# Run with specific environment
python main.py sql retail --environment dev

# Run with custom parameters
python main.py sql retail --bronze-path /path/to/bronze --silver-path /path/to/silver
```

#### Available Projects
- `retail`: Retail data pipeline
- `ecommerce`: E-commerce data pipeline
- `healthcare`: Healthcare data pipeline

#### Pipeline Options
```bash
# View pipeline status
python main.py sql retail --status

# Run specific layer only
python main.py sql retail --layer bronze

# Dry run (validate without execution)
python main.py sql retail --dry-run
```

### PySpark ETL Workflows

#### Basic ETL Execution
```bash
# Run data ingestion
python main.py pyspark data_ingestion

# Run transformation
python main.py pyspark transformation

# Run aggregation
python main.py pyspark aggregation
```

#### ETL Options
```bash
# Run with custom configuration
python main.py pyspark data_ingestion --config custom_config.json

# Run with specific parameters
python main.py pyspark transformation --input-table source_table --output-table target_table

# Monitor ETL progress
python main.py pyspark data_ingestion --monitor
```

### Direct Workflow Access

#### SQL Workflow
```bash
# Direct SQL workflow execution
python workflows/sql_driven/run.py retail

# With environment
python workflows/sql_driven/run.py retail --environment staging

# With custom parameters
python workflows/sql_driven/run.py retail --bronze-path bronze_data --silver-path silver_data
```

#### PySpark Workflow
```bash
# Direct PySpark workflow execution
python workflows/pyspark_etl/run.py data_ingestion

# With configuration
python workflows/pyspark_etl/run.py data_ingestion --config etl_config.json
```

## ☁️ Cost Monitoring

### GCP Cost Monitoring

#### Basic Cost Monitoring
```bash
# Get current costs
make billing-costs YEAR=2025 MONTH=8

# Get cost trends
make billing-report YEAR=2025 MONTH=8

# Check cost threshold
make billing-check THRESHOLD=100
```

#### Cost Analysis Options
```bash


# Check specific threshold
make billing-check THRESHOLD=1000

# Generate comprehensive report
make billing-report YEAR=2025 MONTH=8
```

### Performance Monitoring

#### Health Checks
```bash
# Health monitoring available through admin tools
python -c "from shared.admin.core.admin_client import AdminClient; print('Admin tools available')"
```

#### Performance Analysis
```bash
# Performance monitoring available through admin tools
python -c "from shared.admin.core.admin_client import AdminClient; print('Admin tools available')"
```

## 🛠️ Data Operations CLI Tools

### DBFS Operations

#### `dbfs_cli.py`
**Description**: Command-line wrapper around `explore_dbfs_path()` to inspect DBFS directories.

**Usage**
```bash
python shared/cli/dbfs_cli.py --path dbfs:/databricks-datasets --no-recursive --limit 20
```

**Options**
- `--path`: DBFS path to explore
- `--files-only`: Only return file paths
- `--no-recursive`: Skip recursive traversal of subdirectories
- `--limit`: Maximum number of paths to return
- `--offset`: Number of paths to skip (for pagination)

### File Operations

#### `query_file.py`
**Description**: Preview a file from DBFS using Spark SQL with format-specific logic.

**Usage**
```bash
python shared/cli/query_file.py --file /databricks-datasets/path/file.csv --format csv --limit 10
```

**Options**
- `--file`: File path to read (absolute DBFS path)
- `--format`: Format of the file (`csv`, `parquet`, `delta`, etc.)
- `--limit`: Number of rows to preview (default: 10)

**Example**
```bash
python shared/cli/query_file.py --file /databricks-datasets/wine-quality/winequality-red.csv --format csv --limit 5
```

### Dataset Analysis

#### `analyze_dataset.py`
**Description**: Comprehensive dataset analysis with data quality metrics, schema information, and statistical summaries.

**Usage**
```bash
python shared/cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/ --name "Retail Customers" --max-rows 5000
```

**Options**
- `--path`: Path to the dataset (DBFS path or table name)
- `--name`: Optional name for the dataset
- `--max-rows`: Maximum rows to analyze for performance (default: 10000)

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
python shared/cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/ --name "Retail Customers"

# Analyze a table
python shared/cli/analyze_dataset.py --path retail_customers_bronze --name "Bronze Table"

# Analyze with custom row limit
python shared/cli/analyze_dataset.py --path dbfs:/databricks-datasets/nyctaxi/tripdata/ --max-rows 2000
```

### Data Ingestion

#### `bronze_ingestion.py`
**Description**: Ingest raw data into bronze layer tables with metadata tracking.

**Usage**
```bash
python shared/cli/bronze_ingestion.py --source-path dbfs:/databricks-datasets/retail-org/customers/ --bronze-table-name retail_customers_bronze --project-name retail
```

**Options**
- `--source-path`: Path to source data (DBFS path or table name)
- `--bronze-table-name`: Name for the bronze table
- `--project-name`: Project name for organization (default: default)

**What it does:**
- 📁 Loads data from various formats (CSV, Parquet, Delta, JSON)
- 🏷️ Creates bronze table with metadata columns
- 📊 Adds ingestion timestamp, source path, and layer info
- ✅ Provides table information and statistics

**Examples**
```bash
# Ingest from DBFS
python shared/cli/bronze_ingestion.py --source-path dbfs:/databricks-datasets/retail-org/customers/ --bronze-table-name retail_customers_bronze

# Ingest from existing table
python shared/cli/bronze_ingestion.py --source-path existing_table --bronze-table-name new_bronze_table --project-name ecommerce
```

### Table Management

#### `drop_table.py`
**Description**: Drop tables to avoid schema conflicts during development and testing.

**Usage**
```bash
python shared/cli/drop_table.py --table-name your_table_name
```

**Options**
- `--table-name`: Name of the table to drop

**What it does:**
- 🗑️ Safely drops table if it exists
- ✅ Uses `DROP TABLE IF EXISTS` for safety
- 📝 Provides clear feedback on success/failure

**Examples**
```bash
# Drop a specific table
python shared/cli/drop_table.py --table-name retail_customers_bronze

# Drop test tables
python shared/cli/drop_table.py --table-name test_table_001
```

## 🔧 Administrative Tools

### User Management

#### List and View Users
```bash
# List all users
python shared/admin/cli/admin_cli.py list-users

# Get user details
python shared/admin/cli/admin_cli.py get-user --user-name john.doe

# Search users
python shared/admin/cli/admin_cli.py search-users --query "john"
```

#### User Operations
```bash
# Create new user
python shared/admin/cli/admin_cli.py create-user --user-name jane.doe --email jane@company.com

# Add user to group
python shared/admin/cli/admin_cli.py add-user-to-group --user-name jane.doe --group developers

# Deactivate user
python shared/admin/cli/admin_cli.py deactivate-user --user-name john.doe
```

### Cluster Management

#### Cluster Operations
```bash
# List clusters
python shared/admin/cli/admin_cli.py list-clusters

# Get cluster details
python shared/admin/cli/admin_cli.py get-cluster --cluster-id cluster-123

# Start cluster
python shared/admin/cli/admin_cli.py start-cluster --cluster-id cluster-123

# Stop cluster
python shared/admin/cli/admin_cli.py stop-cluster --cluster-id cluster-123
```

#### Cluster Monitoring
```bash
# Monitor cluster health
python shared/admin/cli/admin_cli.py cluster-health --cluster-id cluster-123

# Get cluster metrics
python shared/admin/cli/admin_cli.py cluster-metrics --cluster-id cluster-123

# Analyze cluster usage
python shared/admin/cli/admin_cli.py cluster-usage --cluster-id cluster-123
```

### Workspace Management

#### Workspace Information
```bash
# Get workspace info
python shared/admin/cli/admin_cli.py workspace-info

# List workspace objects
python shared/admin/cli/admin_cli.py list-workspace-objects --path /

# Get workspace health
python shared/admin/cli/admin_cli.py workspace-health
```

## 📊 Data Operations

### SQL Library CLI

#### Pattern Management
```bash
# List SQL patterns
python shared/sql_library/cli/sql_library_cli.py list-patterns

# Render pattern
python shared/sql_library/cli/sql_library_cli.py render-pattern bronze_ingestion \
  --parameters catalog=hive_metastore schema=retail table_name=customers

# Search patterns
python shared/sql_library/cli/sql_library_cli.py search "bronze"
```

#### Quality Checks
```bash
# List quality checks
python shared/sql_library/cli/sql_library_cli.py list-quality-checks

# Render quality check
python shared/sql_library/cli/sql_library_cli.py render-quality-check completeness_check \
  --parameters table_name=customers column_name=email

# Create quality check library
python shared/sql_library/cli/sql_library_cli.py create-quality-check-library --output-file quality_checks.sql
```

#### Function Management
```bash
# List SQL functions
python shared/sql_library/cli/sql_library_cli.py list-functions

# Render function
python shared/sql_library/cli/sql_library_cli.py render-function date_format \
  --parameters date_column=order_date format=yyyy-MM-dd

# Create function library
python shared/sql_library/cli/sql_library_cli.py create-function-library --output-file functions.sql
```

### Data Exploration

#### DBFS Operations
```bash
# List DBFS contents
python shared/cli/dbfs_cli.py list /path/to/data

# Copy files
python shared/cli/dbfs_cli.py copy /source/path /target/path

# Delete files
python shared/cli/dbfs_cli.py delete /path/to/file
```

#### Query Execution
```bash
# Execute SQL file
python shared/cli/query_file.py execute sql/bronze/retail/ingest_customers.sql

# Execute with parameters
python shared/cli/query_file.py execute sql/bronze/retail/ingest_customers.sql \
  --parameters catalog=hive_metastore schema=retail

# Execute and save results
python shared/cli/query_file.py execute sql/bronze/retail/ingest_customers.sql \
  --output results.csv
```

## 🛠️ Development Tools

### Testing

#### Run Tests
```bash
# Run all tests
make test

# Run specific test suite
python tests/run_ci_tests.py

# Run integration tests
python tests/run_integration_tests.py

# Run simple tests
python tests/run_simple_tests.py
```

#### Test Specific Components
```bash
# Test SQL library
python tools/test_sql_library.py

# Test pipeline components
python tests/test_pipeline.py

# Test data operations
python tests/test_dbfs_explorer.py
```

### Data Generation

#### Create Test Data
```bash
# Generate sample data
python tools/create_test_data.py

# Generate specific dataset
python tools/create_test_data.py --dataset retail --rows 1000

# Generate with custom schema
python tools/create_test_data.py --schema custom_schema.json
```

#### Data Validation
```bash
# Check table structure
python tools/check_tables.py

# Find datasets
python tools/find_datasets.py --pattern "customer*"

# Validate data quality
python tools/check_tables.py --quality-checks
```

### Deployment

#### Job Management
```bash
# Generate job spec
python scripts/generate_job_spec.py --job-name my-job --email user@company.com

# Deploy job
make deploy-ingest

# Run remote job
make run-ingest-remote JOB_ID=job-123

# Clean up job
make clean-remote JOB_ID=job-123
```

## 🔧 Configuration

### Environment Variables

#### Required Variables
```bash
export DATABRICKS_PROFILE="your-profile"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
```

#### Optional Variables
```bash
export DATABRICKS_CATALOG="your-catalog"
export DATABRICKS_SCHEMA="your-schema"
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
```

### Configuration Files

#### Environment Configs
```bash
# Development environment
config/environments/dev.json

# Production environment
config/environments/prod.json

# Custom environment
config/environments/custom.json
```

#### Job Configs
```bash
# Job specifications
config/jobs/sample_job_config.json

# Pipeline configurations
workflows/sql_driven/config/
workflows/pyspark_etl/config/
```

## 🆘 Troubleshooting

### Common Issues

#### Connection Problems
```bash
# Verify Databricks connection
databricks clusters list

# Check cluster status
databricks clusters get --cluster-id your-cluster-id

# Test connection
python -c "from utils.session import DatabricksSession; print('Connection OK')"
```

#### Import Errors
```bash
# Check virtual environment
which python
pip list | grep databricks

# Reinstall dependencies
pip install -r requirements.txt --force-reinstall

# Check Python path
python -c "import sys; print(sys.path)"
```

#### Permission Errors
```bash
# Check user permissions
python shared/admin/cli/admin_cli.py get-user --user-name your-username

# Verify workspace access
python shared/admin/cli/admin_cli.py workspace-info

# Check cluster access
python shared/admin/cli/admin_cli.py get-cluster --cluster-id your-cluster-id
```

### Debug Mode

#### Enable Debug Logging
```bash
# Set debug environment variable
export DEBUG=1

# Run with debug output
python main.py sql retail --debug

# Enable verbose logging
make billing-costs YEAR=2025 MONTH=8
```

#### Log Analysis
```bash
# View recent logs
tail -f logs/databricks_toolkit.log

# Search logs
grep "ERROR" logs/databricks_toolkit.log

# Analyze log patterns
python -c "import re; print('Log analysis')"
```

## 📚 Best Practices

### Command Organization
1. **Use Aliases**: Create aliases for frequently used commands
2. **Group Commands**: Organize related commands together
3. **Use Help**: Always check `--help` for new commands
4. **Test Commands**: Test commands in development before production

### Error Handling
1. **Check Return Codes**: Always check command exit codes
2. **Use Dry Runs**: Test with `--dry-run` when available
3. **Validate Inputs**: Verify parameters before execution
4. **Monitor Output**: Watch for error messages and warnings

### Performance
1. **Use Filters**: Filter data to reduce processing time
2. **Batch Operations**: Group operations when possible
3. **Monitor Resources**: Watch cluster and memory usage
4. **Optimize Queries**: Use efficient SQL and PySpark code

---

**Need more help?** Check the [Reference](reference/) section or use `--help` with any command!
