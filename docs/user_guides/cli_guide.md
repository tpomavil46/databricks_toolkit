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
python shared/cli/monitoring/gcp_cost_cli.py --project-id <project>
```

### Help Commands

```bash
# Get help for any command
python main.py --help
python shared/cli/monitoring/gcp_cost_cli.py --help

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

### GCP Cost CLI

#### Basic Cost Monitoring
```bash
# Get current costs
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project

# Get cost trends
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --trends

# Get detailed breakdown
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --detailed
```

#### Cost Analysis Options
```bash
# Filter by date range
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --start-date 2024-01-01 --end-date 2024-01-31

# Filter by service
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --service BigQuery

# Export results
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --output json
```

#### Cost Alerts
```bash
# Set cost threshold alert
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --threshold 1000

# Check cost anomalies
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --anomaly-detection
```

### Performance Monitoring

#### Health Checks
```bash
# Run system health check
python shared/cli/monitoring/health_cli.py

# Check specific components
python shared/cli/monitoring/health_cli.py --component clusters

# Get detailed health report
python shared/cli/monitoring/health_cli.py --detailed
```

#### Performance Analysis
```bash
# Monitor cluster performance
python shared/cli/monitoring/performance_cli.py

# Analyze query performance
python shared/cli/monitoring/performance_cli.py --queries

# Get resource utilization
python shared/cli/monitoring/performance_cli.py --resources
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
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project --verbose
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
