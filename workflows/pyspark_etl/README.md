# PySpark ETL Workflow

## Overview

The PySpark ETL Workflow is designed for teams that prefer Python-first data processing. It provides a comprehensive ETL framework with standardized patterns, data validation, and pipeline orchestration using PySpark.

## Architecture

```
workflows/pyspark_etl/
├── README.md              # This documentation
├── run.py                 # Main entry point
├── pipelines/             # PySpark ETL pipeline implementations
│   └── pipeline.py
├── transformations/       # Data transformation modules
│   ├── data_quality.py
│   ├── data_cleaning.py
│   └── aggregations.py
├── config/               # Workflow-specific configuration
└── examples/             # Example implementations
```

## Key Features

### ✅ Python-First Approach
- PySpark DataFrames for data processing
- Python-native transformations
- Rich ecosystem of data processing libraries

### ✅ Standardized ETL Framework
- Consistent pipeline patterns
- Built-in data validation
- Error handling and logging

### ✅ Configuration-Driven
- Environment-specific configurations
- Parameterized pipeline execution
- Flexible data source/sink configuration

### ✅ Data Quality Integration
- Built-in data quality checks
- Validation at each pipeline stage
- Comprehensive error reporting

## Quick Start

### 1. Run a PySpark ETL Pipeline

```bash
# Run PySpark ETL workflow
python main.py pyspark data_ingestion

# Run with specific environment
python main.py pyspark transformation --environment prod
```

### 2. Direct Workflow Access

```bash
# Run PySpark ETL workflow directly
python workflows/pyspark_etl/run.py data_ingestion

# Run with environment
python workflows/pyspark_etl/run.py transformation --environment staging
```

## Project Structure

### Pipeline Organization

Each pipeline follows this structure:

```
pipelines/
├── data_ingestion/
│   ├── config.json
│   ├── pipeline.py
│   └── transformations/
├── data_transformation/
│   ├── config.json
│   ├── pipeline.py
│   └── transformations/
└── data_aggregation/
    ├── config.json
    ├── pipeline.py
    └── transformations/
```

### Configuration

Environment-specific configurations:

```python
# config/environments/dev.json
{
    "spark_config": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    "data_sources": {
        "bronze": "dev_bronze_catalog",
        "silver": "dev_silver_catalog",
        "gold": "dev_gold_catalog"
    }
}

# config/environments/prod.json
{
    "spark_config": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    "data_sources": {
        "bronze": "prod_bronze_catalog",
        "silver": "prod_silver_catalog", 
        "gold": "prod_gold_catalog"
    }
}
```

## Usage Examples

### Basic Pipeline Execution

```python
from workflows.pyspark_etl.pipelines.pipeline import Pipeline

# Initialize pipeline
pipeline = Pipeline(
    name="data_ingestion",
    environment="dev"
)

# Run the pipeline
pipeline.run()
```

### Custom Transformation

```python
from workflows.pyspark_etl.transformations.data_cleaning import DataCleaner

# Create data cleaner
cleaner = DataCleaner(spark_session)

# Apply transformations
cleaned_df = cleaner.clean_dataframe(
    input_df,
    remove_duplicates=True,
    handle_missing_values=True
)
```

### Data Quality Validation

```python
from workflows.pyspark_etl.transformations.data_quality import DataValidator

# Create validator
validator = DataValidator(spark_session)

# Validate data
validation_results = validator.validate_dataframe(
    df,
    rules={
        "completeness": 0.95,
        "uniqueness": 0.99,
        "accuracy": 0.90
    }
)
```

## Configuration

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
# workflows/pyspark_etl/config/pipeline_config.py
PIPELINE_CONFIG = {
    "default_environment": "dev",
    "enable_validation": True,
    "log_level": "INFO",
    "enable_monitoring": True
}
```

## Best Practices

### 1. Data Processing
- Use lazy evaluation for performance
- Cache intermediate DataFrames when needed
- Use appropriate partitioning strategies

### 2. Error Handling
- Implement comprehensive error handling
- Use try-catch blocks for critical operations
- Log errors with sufficient context

### 3. Performance Optimization
- Monitor Spark UI for performance insights
- Use broadcast joins for small tables
- Optimize shuffle operations

### 4. Data Quality
- Validate data at each stage
- Implement data quality checks
- Monitor data lineage

## Troubleshooting

### Common Issues

1. **Memory Issues**
   ```
   Error: Java heap space
   ```
   **Solution**: Increase Spark executor memory or optimize data processing

2. **Partition Skew**
   ```
   Warning: Data skew detected
   ```
   **Solution**: Use appropriate partitioning or repartition data

3. **Serialization Errors**
   ```
   Error: Task not serializable
   ```
   **Solution**: Avoid capturing external variables in transformations

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable Spark debug logging
spark.sparkContext.setLogLevel("DEBUG")
```

## Integration with Shared Components

### CLI Tools
```bash
# Use CLI tools with PySpark ETL workflow
python -m shared.cli.dbfs_cli list /path/to/data
python -m shared.cli.query_file execute transformations/data_cleaning.sql
```

### Admin Tools
```bash
# Manage workspace resources
python -m shared.admin.admin_cli list-clusters
python -m shared.admin.admin_cli monitor-performance
```

### SQL Library
```python
# Use SQL library components for complex queries
from shared.sql_library.core.sql_patterns import SQLPatterns
from shared.sql_library.core.data_quality import DataQualityChecks
```

## Advanced Features

### Custom Transformations

```python
from pyspark.sql.functions import col, when
from workflows.pyspark_etl.transformations.base import BaseTransformation

class CustomTransformation(BaseTransformation):
    def transform(self, df):
        """Apply custom transformation logic."""
        return df.withColumn(
            "processed_flag",
            when(col("amount") > 1000, "high_value")
            .otherwise("standard")
        )
```

### Pipeline Monitoring

```python
from workflows.pyspark_etl.pipelines.pipeline import Pipeline

# Enable monitoring
pipeline = Pipeline(
    name="data_ingestion",
    environment="dev",
    enable_monitoring=True
)

# Run with monitoring
pipeline.run()
```

## Migration from Old Structure

If migrating from the old structure:

1. **Move ETL files** to `workflows/pyspark_etl/`
2. **Update imports** to use new paths
3. **Test pipelines** with new structure
4. **Update documentation** to reflect changes

## Next Steps

1. **Add more transformation patterns** for common use cases
2. **Implement advanced data quality checks** with ML-based validation
3. **Add performance monitoring** and alerting
4. **Create more example pipelines** for different domains

---

**For more information, see the main project README.md** 