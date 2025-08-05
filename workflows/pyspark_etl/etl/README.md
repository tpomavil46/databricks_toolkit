# ETL Framework

A standardized ETL framework for Databricks that provides parameterized, reusable patterns for data ingestion, transformation, and aggregation following the Medallion Architecture.

## 🎯 Overview

The ETL framework provides:
- **Parameterized Configuration** - Environment-based configuration management
- **Standardized Patterns** - Reusable transformation and validation patterns
- **Data Quality Validation** - Comprehensive data quality checks
- **Medallion Architecture** - Bronze, Silver, Gold layer support
- **Job Orchestration** - Individual and full pipeline execution

## 📁 Structure

```
etl/
├── core/                          # Core framework components
│   ├── config.py                  # Configuration management
│   ├── etl_pipeline.py           # Main ETL pipeline class
│   ├── transformations.py         # Standardized transformation patterns
│   └── validators.py             # Data validation framework
├── jobs/                          # Standardized job implementations
│   ├── bronze/
│   │   └── ingestion.py          # Bronze ingestion job
│   ├── silver/
│   │   └── transformation.py     # Silver transformation job
│   └── gold/
│       └── aggregation.py        # Gold aggregation job
├── example_usage.py              # Usage examples
└── README.md                     # This file
```

## 🚀 Quick Start

### 1. Basic Bronze Ingestion

```python
from etl.core.config import create_default_config
from etl.core.etl_pipeline import StandardETLPipeline

# Create configuration
config = create_default_config("retail", "dev")

# Create pipeline
pipeline = StandardETLPipeline(config)

# Run bronze ingestion
results = pipeline.run_bronze_ingestion("dbfs:/databricks-datasets/retail-org/customers/")
print(f"✅ Bronze ingestion completed: {results['table_name']}")

# Run with auto-drop for existing tables
results = pipeline.run_bronze_ingestion("dbfs:/databricks-datasets/retail-org/customers/", auto_drop=True)
```

### 2. Silver Transformation

```python
# Define transformations
transformations = {
    'null_strategy': {
        'customer_name': 'fill_default',
        'state': 'fill_default'
    },
    'type_mapping': {
        'units_purchased': 'integer'
    },
    'filter_conditions': {
        'customer_name': {'not_equals': ''}
    }
}

# Run silver transformation
results = pipeline.run_silver_transformation("bronze_table_name", transformations=transformations)
print(f"✅ Silver transformation completed: {results['table_name']}")

# Run with auto-drop for existing tables
results = pipeline.run_silver_transformation("bronze_table_name", transformations=transformations, auto_drop=True)
```

### 3. Gold Aggregation

```python
from pyspark.sql.functions import sum, count

# Define aggregations
aggregations = {
    'group_by': ['state'],
    'aggregations': {
        'total_units': sum,
        'customer_count': count
    }
}

# Run gold aggregation
results = pipeline.run_gold_aggregation("silver_table_name", aggregations=aggregations)
print(f"✅ Gold aggregation completed: {results['table_name']}")

# Run with auto-drop for existing tables
results = pipeline.run_gold_aggregation("silver_table_name", aggregations=aggregations, auto_drop=True)
```

### 4. Full Pipeline

```python
# Run complete pipeline
results = pipeline.run_full_pipeline(
    source_path="dbfs:/databricks-datasets/retail-org/customers/",
    transformations=transformations,
    aggregations=aggregations
)
print(f"✅ Full pipeline completed: {results['overall_status']}")

# Run with auto-drop for existing tables
results = pipeline.run_full_pipeline(
    source_path="dbfs:/databricks-datasets/retail-org/customers/",
    transformations=transformations,
    aggregations=aggregations,
    auto_drop=True
)
```

## 🔧 Configuration Management

### Table Management

The framework includes intelligent table management to handle existing tables:

#### Auto-Drop Mode
```python
# Automatically drop existing tables
results = pipeline.run_bronze_ingestion("source_path", auto_drop=True)
```

#### Interactive Mode (Default)
When `auto_drop=False` (default), the framework will:
1. **Check if table exists** and show current row count
2. **Present options** to the user:
   - Drop existing table and continue
   - Cancel operation
   - Use different table name
3. **Handle the choice** gracefully

#### Example Interactive Session
```
⚠️  Table 'retail_dev_data_bronze' already exists!
📊 Current row count: 28,813

🔄 Options:
  1. Drop existing table and continue
  2. Cancel operation
  3. Use different table name

Enter choice (1-3, default=1): 1
🗑️  Dropping existing table 'retail_dev_data_bronze'...
✅ Successfully dropped table 'retail_dev_data_bronze'
```

### Environment-Based Configuration

```python
import os
from etl.core.config import PipelineConfig

# Set environment variables
os.environ['ENVIRONMENT'] = 'staging'
os.environ['DATABRICKS_CLUSTER_ID'] = 'your-cluster-id'
os.environ['QUALITY_THRESHOLD'] = '0.98'

# Create configuration
config = PipelineConfig(project_name="retail")
print(f"Environment: {config.table_config.environment}")
print(f"Cluster ID: {config.cluster_config.cluster_id}")
```

### Configuration Files

```python
# Save configuration
config.save_to_file("config/retail/staging.json")

# Load configuration
config = PipelineConfig.from_file("config/retail/staging.json")
```

## 📊 Data Validation

### Automatic Validation

The framework automatically validates data at each layer:

- **Bronze Layer**: Basic data quality checks
- **Silver Layer**: Business rule validation
- **Gold Layer**: KPI and metric validation

### Quality Scoring

```python
# Get validation results
validation_results = pipeline.validator.validate_bronze_data(df, "table_name")
print(f"Quality Score: {validation_results['quality_score']:.1f}/100")

# Print validation report
pipeline.validator.print_validation_report('bronze')
```

## 🔄 Transformation Patterns

### Standard Transformations

```python
from etl.core.transformations import DataTransformation

transformer = DataTransformation(spark)

# Clean string columns
df = transformer.clean_string_columns(df)

# Handle null values
df = transformer.handle_null_values(df, {
    'customer_name': 'fill_default',
    'units_purchased': 'fill_mean'
})

# Convert data types
df = transformer.convert_data_types(df, {
    'units_purchased': 'integer',
    'loyalty_segment': 'integer'
})

# Remove duplicates
df = transformer.remove_duplicates(df, ['customer_id'])

# Filter data
df = transformer.filter_data(df, {
    'customer_name': {'not_equals': ''},
    'units_purchased': {'greater_than': 0}
})
```

## 🏗️ Job Classes

### Individual Job Classes

```python
from etl.jobs.bronze.ingestion import BronzeIngestionJob
from etl.jobs.silver.transformation import SilverTransformationJob
from etl.jobs.gold.aggregation import GoldAggregationJob

# Bronze ingestion
bronze_job = BronzeIngestionJob("retail", "dev")
bronze_results = bronze_job.run("dbfs:/databricks-datasets/retail-org/customers/")

# Silver transformation
silver_job = SilverTransformationJob("retail", "dev")
silver_results = silver_job.run(bronze_results['table_name'])

# Gold aggregation
gold_job = GoldAggregationJob("retail", "dev")
gold_results = gold_job.run(silver_results['table_name'])
```

## 📈 Pipeline Results

### Result Structure

```python
results = pipeline.run_bronze_ingestion("source_path")

# Results contain:
{
    'table_name': 'retail_dev_data_bronze',
    'source_path': 'dbfs:/databricks-datasets/retail-org/customers/',
    'row_count': 28813,
    'validation_results': {
        'quality_score': 95.2,
        'null_analysis': {...},
        'data_type_analysis': {...},
        'recommendations': [...]
    },
    'status': 'success'
}
```

### Pipeline Summary

```python
# Get pipeline summary
summary = pipeline.get_pipeline_summary()
print(f"Layers Completed: {summary['successful_layers']}/{summary['total_layers']}")
print(f"Total Rows Processed: {summary['total_rows_processed']:,}")
print(f"Overall Quality Score: {summary['validation_summary']['overall_quality_score']:.1f}/100")

# Print summary
pipeline.print_pipeline_summary()
```

## 🎯 Best Practices

### 1. Configuration Management
- Use environment variables for sensitive configuration
- Save configurations to files for version control
- Use different configurations for different environments

### 2. Data Quality
- Always validate data at each layer
- Set appropriate quality thresholds
- Review validation recommendations

### 3. Error Handling
- Use try-catch blocks for pipeline execution
- Log errors and validation failures
- Implement retry logic for transient failures

### 4. Performance
- Use appropriate cluster sizes
- Monitor pipeline execution times
- Optimize transformations for large datasets

## 🔧 Customization

### Custom Transformations

```python
def custom_transformation(df):
    """Custom transformation function."""
    return df.withColumn("custom_column", lit("custom_value"))

# Apply custom transformation
df = transformer.apply_custom_transformation(df, custom_transformation)
```

### Custom Validation Rules

```python
class CustomValidator(DataValidator):
    def _validate_business_rules(self, df):
        """Override with custom business rules."""
        return {
            'custom_check': {'status': 'passed', 'message': 'Custom validation passed'}
        }
```

## 📚 Examples

See `example_usage.py` for comprehensive examples of:
- Bronze ingestion
- Silver transformation
- Gold aggregation
- Full pipeline execution
- Environment configuration
- Individual job usage

## 🔗 Integration

### With SQL-Driven Pipelines

The ETL framework can be used alongside the SQL-driven pipeline framework:

```python
# Use ETL for data ingestion and validation
etl_results = pipeline.run_bronze_ingestion("source_path")

# Use SQL-driven pipeline for transformations
sql_pipeline = SQLDrivenPipeline(spark, project="retail")
sql_results = sql_pipeline.run_silver_gold_pipeline()
```

### With CLI Tools

```bash
# Use CLI tools for data discovery
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/ --limit 20

# Use CLI tools for data analysis
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/

# Use ETL framework for processing
python etl/example_usage.py
```

## 🚀 Migration from Old ETL

### From `etl_example.py`

```python
# Old approach
from transformations import steps
df = steps.load_data(spark)
df = steps.filter_columns(df)
df = steps.transform_columns(df)

# New approach
pipeline = StandardETLPipeline(config)
results = pipeline.run_full_pipeline("source_path", transformations, aggregations)
```

### From `jobs/` Directory

```python
# Old approach
from jobs.bronze.ingest import run_bronze_ingestion
from jobs.silver.transform import run_silver_transformation

# New approach
bronze_job = BronzeIngestionJob("project", "environment")
silver_job = SilverTransformationJob("project", "environment")
```

## 📊 Monitoring and Logging

### Built-in Logging

All operations are automatically logged using the `@log_function_call` decorator:

```python
# Logs are automatically generated for:
# - Pipeline execution
# - Data validation
# - Transformation steps
# - Error handling
```

### Quality Metrics

```python
# Track quality scores over time
validation_summary = pipeline.validator.get_validation_summary()
print(f"Overall Quality: {validation_summary['overall_quality_score']:.1f}/100")
```

## 🔄 Next Steps

1. **Test the Framework**: Run the examples in `example_usage.py`
2. **Migrate Existing Code**: Convert old ETL code to use the new framework
3. **Customize Validations**: Add business-specific validation rules
4. **Optimize Performance**: Tune configurations for your data volumes
5. **Add Monitoring**: Integrate with your monitoring and alerting systems

---

*This framework provides a solid foundation for standardized, maintainable ETL operations in Databricks.* 