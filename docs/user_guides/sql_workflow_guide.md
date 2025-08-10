# SQL-Driven Workflow Guide

Complete guide to the SQL-driven workflow for Databricks data engineering.

## 🎯 Overview

The SQL-Driven Workflow is designed for teams that prefer SQL-first data processing. It provides a clean, parameterized approach to data pipelines where SQL is the primary language for transformations.

## 🏗️ Architecture

```
workflows/sql_driven/
├── run.py                 # Main entry point
├── pipelines/             # SQL-driven pipeline implementations
│   └── sql_driven_pipeline.py
├── sql/                   # SQL templates and queries
│   ├── bronze/           # Bronze layer SQL
│   ├── silver/           # Silver layer SQL
│   ├── gold/             # Gold layer SQL
│   └── templates/        # Reusable SQL templates
├── config/               # Workflow-specific configuration
└── examples/             # Example implementations
```

## ✨ Key Features

### ✅ SQL-First Approach
- SQL is the primary transformation language
- Python orchestrates SQL execution
- Clean separation of concerns

### ✅ Parameterized SQL
- Dynamic SQL generation with parameters
- Environment-specific configurations
- Reusable SQL templates

### ✅ Medallion Architecture
- Bronze: Raw data ingestion
- Silver: Cleaned and validated data
- Gold: Business-ready aggregations

### ✅ Project-Based Organization
- Each project has its own SQL files
- Clear separation between domains
- Easy to maintain and extend

## 🚀 Quick Start

### 1. Run a SQL Pipeline

```bash
# Run SQL workflow for retail project
python main.py sql retail

# Run with specific environment
python main.py sql ecommerce --environment prod
```

### 2. Direct Workflow Access

```bash
# Run SQL workflow directly
python workflows/sql_driven/run.py retail

# Run with environment
python workflows/sql_driven/run.py retail --environment staging
```

## 📁 Project Structure

### SQL Organization

Each project follows this structure:

```
sql/
├── bronze/
│   ├── retail/
│   │   ├── ingest_customers.sql
│   │   └── ingest_orders.sql
│   └── ecommerce/
│       ├── ingest_products.sql
│       └── ingest_transactions.sql
├── silver/
│   ├── retail/
│   │   ├── clean_customers.sql
│   │   └── clean_orders.sql
│   └── ecommerce/
│       ├── clean_products.sql
│       └── clean_transactions.sql
└── gold/
    ├── retail/
    │   ├── customer_kpis.sql
    │   └── sales_analytics.sql
    └── ecommerce/
        ├── product_performance.sql
        └── revenue_metrics.sql
```

### Configuration

Environment-specific configurations:

```python
# config/environments/dev.json
{
    "database": "dev_database",
    "catalog": "dev_catalog",
    "cluster_id": "dev-cluster-123"
}

# config/environments/prod.json
{
    "database": "prod_database", 
    "catalog": "prod_catalog",
    "cluster_id": "prod-cluster-456"
}
```

## 💻 Usage Examples

### Basic Pipeline Execution

```python
from workflows.sql_driven.pipelines.sql_driven_pipeline import SQLDrivenPipeline

# Initialize pipeline
pipeline = SQLDrivenPipeline(
    spark=spark_session,
    sql_base_path="workflows/sql_driven/sql",
    project="retail"
)

# Run the pipeline
pipeline.run()
```

### Custom SQL Execution

```python
# Execute specific SQL file
pipeline.execute_sql_file("bronze/retail/ingest_customers.sql")

# Execute with parameters
pipeline.execute_sql_with_params(
    "silver/retail/clean_orders.sql",
    {"date_filter": "2024-01-01"}
)
```

## ⚙️ Configuration

### Environment Variables

```bash
# Required
export DATABRICKS_PROFILE="your-profile"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# Optional
export DATABRICKS_CATALOG="your-catalog"
export DATABRICKS_SCHEMA="your-schema"
```

### Pipeline Configuration

```python
# workflows/sql_driven/config/pipeline_config.py
PIPELINE_CONFIG = {
    "default_environment": "dev",
    "sql_base_path": "workflows/sql_driven/sql",
    "log_level": "INFO",
    "enable_validation": True
}
```

## 📚 Best Practices

### 1. SQL Organization
- Use descriptive file names
- Group by domain (retail, ecommerce, etc.)
- Follow Medallion architecture layers

### 2. Parameterization
- Use parameters for dynamic values
- Avoid hardcoded values in SQL
- Use environment-specific configurations

### 3. Error Handling
- Include proper error handling in SQL
- Use logging for debugging
- Validate data quality

### 4. Performance
- Use appropriate partitioning
- Optimize SQL queries
- Monitor execution times

## 🔧 Troubleshooting

### Common Issues

1. **SQL File Not Found**
   ```
   Error: SQL file not found: bronze/retail/ingest_customers.sql
   ```
   **Solution**: Check file path and project structure

2. **Parameter Substitution Error**
   ```
   Error: Parameter not found: {date_filter}
   ```
   **Solution**: Ensure all parameters are provided

3. **Database Connection Error**
   ```
   Error: Cannot connect to database
   ```
   **Solution**: Check Databricks credentials and cluster status

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🔗 Integration with Shared Components

### CLI Tools
```bash
# Use CLI tools with SQL workflow
python -m shared.cli.dbfs_cli list /path/to/data
python -m shared.cli.query_file execute sql/bronze/retail/ingest_customers.sql
```

### Admin Tools
```bash
# Manage workspace resources
python -m shared.admin.admin_cli list-users
python -m shared.admin.admin_cli list-clusters
```

### SQL Library
```python
# Use SQL library components
from shared.sql_library.core.sql_patterns import SQLPatterns
from shared.sql_library.core.data_quality import DataQualityChecks
```

## 🔄 Migration from Old Structure

If migrating from the old structure:

1. **Move SQL files** to `workflows/sql_driven/sql/`
2. **Update imports** to use new paths
3. **Test pipelines** with new structure
4. **Update documentation** to reflect changes

## 🎯 Next Steps

1. **Add more SQL templates** for common patterns
2. **Implement data quality checks** in SQL
3. **Add performance monitoring** for SQL execution
4. **Create more example projects** for different domains

---

**For more information, see the [Getting Started](getting_started.md) guide or [CLI Guide](cli_guide.md).**
