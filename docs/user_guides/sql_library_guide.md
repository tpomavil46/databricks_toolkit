# SQL Library Guide

Complete guide to the SQL Library for Databricks data engineering.

## 🎯 Overview

The SQL Library is a collection of reusable SQL components designed to standardize data operations in Databricks. It includes:

- **SQL Patterns**: Standardized patterns for common data operations
- **SQL Functions**: Reusable functions for data manipulation and business logic
- **SQL Templates**: Parameterized templates for complete workflows
- **Data Quality Checks**: Comprehensive data quality validation framework

## 🏗️ Architecture

```
sql_library/
├── __init__.py              # Package initialization
├── core/                    # Core library components
│   ├── __init__.py
│   ├── sql_patterns.py      # SQL patterns for common operations
│   ├── data_quality.py      # Data quality check framework
│   ├── sql_functions.py     # SQL function library
│   └── sql_templates.py     # SQL template library
└── cli/                     # Command-line interface
    └── sql_library_cli.py   # CLI tool for library management
```

## 🚀 Quick Start

### Installation

The SQL library is part of the Databricks Toolkit. No additional installation is required.

### Basic Usage

```python
from shared.sql_library.core.sql_patterns import SQLPatterns
from shared.sql_library.core.data_quality import DataQualityChecks
from shared.sql_library.core.sql_functions import SQLFunctions
from shared.sql_library.core.sql_templates import SQLTemplates

# Initialize components
patterns = SQLPatterns()
quality = DataQualityChecks()
functions = SQLFunctions()
templates = SQLTemplates()

# Render a bronze ingestion pattern
sql = patterns.render_pattern('bronze_ingestion', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customers',
    'source_table': 'csv.`/mnt/databricks-datasets/retail-org/customers/`'
})
```

### CLI Usage

```bash
# List all SQL patterns
python shared/sql_library/cli/sql_library_cli.py list-patterns

# Render a pattern with parameters
python shared/sql_library/cli/sql_library_cli.py render-pattern bronze_ingestion \
  --parameters catalog=hive_metastore schema=retail table_name=customers

# Create a function library
python shared/sql_library/cli/sql_library_cli.py create-function-library --output-file functions.sql

# Search the library
python shared/sql_library/cli/sql_library_cli.py search "bronze"
```

## 📊 SQL Patterns

### Available Patterns

| Pattern | Category | Description |
|---------|----------|-------------|
| `bronze_ingestion` | ingestion | Standard bronze layer ingestion |
| `incremental_ingestion` | ingestion | Incremental ingestion with deduplication |
| `silver_transformation` | transformation | Standard silver layer transformation |
| `data_cleaning` | transformation | Data cleaning and standardization |
| `completeness_check` | quality | Null value and completeness analysis |
| `uniqueness_check` | quality | Duplicate detection and uniqueness validation |
| `data_type_check` | quality | Data type and format validation |
| `gold_aggregation` | aggregation | Gold layer aggregation patterns |
| `time_series_aggregation` | aggregation | Time-based aggregation patterns |
| `business_rule_validation` | validation | Business rule validation |
| `referential_integrity` | validation | Referential integrity checks |

### Pattern Usage

```python
# Bronze ingestion pattern
bronze_sql = patterns.render_pattern('bronze_ingestion', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customers',
    'source_table': 'csv.`/mnt/databricks-datasets/retail-org/customers/`'
})

# Silver transformation pattern
silver_sql = patterns.render_pattern('silver_transformation', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customers_clean',
    'source_table': 'hive_metastore.retail.customers_bronze'
})

# Gold aggregation pattern
gold_sql = patterns.render_pattern('gold_aggregation', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customer_kpis',
    'source_table': 'hive_metastore.retail.customers_silver'
})
```

## 🔍 Data Quality Checks

### Available Quality Checks

| Check | Description | Parameters |
|-------|-------------|------------|
| `completeness_check` | Check for null values and completeness | table, column |
| `uniqueness_check` | Check for duplicate values | table, column |
| `data_type_check` | Validate data types and formats | table, column, expected_type |
| `range_check` | Check value ranges | table, column, min_value, max_value |
| `pattern_check` | Validate against regex patterns | table, column, pattern |

### Quality Check Usage

```python
# Completeness check
completeness_sql = quality.render_check('completeness_check', {
    'table': 'hive_metastore.retail.customers',
    'column': 'email'
})

# Uniqueness check
uniqueness_sql = quality.render_check('uniqueness_check', {
    'table': 'hive_metastore.retail.customers',
    'column': 'customer_id'
})

# Data type check
datatype_sql = quality.render_check('data_type_check', {
    'table': 'hive_metastore.retail.orders',
    'column': 'amount',
    'expected_type': 'decimal(10,2)'
})
```

## 🔧 SQL Functions

### Available Functions

| Function | Category | Description |
|----------|----------|-------------|
| `date_format` | Date/Time | Format dates with custom patterns |
| `string_clean` | String | Clean and standardize strings |
| `numeric_round` | Numeric | Round numbers with precision |
| `array_operations` | Array | Array manipulation functions |
| `json_extract` | JSON | Extract values from JSON fields |

### Function Usage

```python
# Date formatting function
date_sql = functions.render_function('date_format', {
    'date_column': 'order_date',
    'format': 'yyyy-MM-dd'
})

# String cleaning function
string_sql = functions.render_function('string_clean', {
    'string_column': 'customer_name',
    'remove_special_chars': True,
    'to_lowercase': True
})

# Numeric rounding function
numeric_sql = functions.render_function('numeric_round', {
    'numeric_column': 'amount',
    'precision': 2
})
```

## 📋 SQL Templates

### Available Templates

| Template | Description | Use Case |
|----------|-------------|----------|
| `bronze_to_silver` | Complete bronze to silver pipeline | Data cleaning workflows |
| `silver_to_gold` | Complete silver to gold pipeline | Aggregation workflows |
| `full_pipeline` | Complete bronze to gold pipeline | End-to-end workflows |
| `data_quality_pipeline` | Data quality validation pipeline | Quality assurance |

### Template Usage

```python
# Bronze to silver template
bronze_silver_sql = templates.render_template('bronze_to_silver', {
    'bronze_table': 'hive_metastore.retail.customers_bronze',
    'silver_table': 'hive_metastore.retail.customers_silver',
    'quality_checks': ['completeness', 'uniqueness']
})

# Full pipeline template
full_pipeline_sql = templates.render_template('full_pipeline', {
    'bronze_table': 'hive_metastore.retail.customers_bronze',
    'silver_table': 'hive_metastore.retail.customers_silver',
    'gold_table': 'hive_metastore.retail.customer_kpis'
})
```

## 🛠️ Advanced Usage

### Custom Pattern Creation

```python
from shared.sql_library.core.sql_patterns import SQLPattern

# Create custom pattern
custom_pattern = SQLPattern(
    name="Custom Ingestion",
    description="Custom ingestion pattern for specific use case",
    sql_template="""
    CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}
    AS SELECT 
        *,
        current_timestamp() as ingestion_time,
        '{source_system}' as source_system
    FROM {source_table}
    WHERE {filter_condition}
    """,
    parameters=["catalog", "schema", "table_name", "source_table", "source_system", "filter_condition"],
    category="custom",
    tags=["custom", "ingestion"]
)

# Add to patterns
patterns.add_pattern("custom_ingestion", custom_pattern)
```

### Pattern Validation

```python
# Validate pattern parameters
try:
    sql = patterns.render_pattern('bronze_ingestion', {
        'catalog': 'hive_metastore',
        'schema': 'retail'
        # Missing required parameters will raise ValueError
    })
except ValueError as e:
    print(f"Validation error: {e}")
```

### Pattern Search

```python
# Search patterns by category
ingestion_patterns = patterns.list_patterns(category='ingestion')

# Search patterns by query
bronze_patterns = patterns.search_patterns('bronze')

# Get pattern examples
examples = patterns.get_pattern_examples('bronze_ingestion')
for example in examples:
    print(f"Example: {example['description']}")
    print(f"Parameters: {example['parameters']}")
```

## 🔗 Integration with Workflows

### SQL-Driven Workflow Integration

```python
from workflows.sql_driven.pipelines.sql_driven_pipeline import SQLDrivenPipeline
from shared.sql_library.core.sql_patterns import SQLPatterns

# Initialize components
pipeline = SQLDrivenPipeline(spark=spark_session, project="retail")
patterns = SQLPatterns()

# Use patterns in pipeline
bronze_sql = patterns.render_pattern('bronze_ingestion', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customers',
    'source_table': 'csv.`/mnt/databricks-datasets/retail-org/customers/`'
})

# Execute in pipeline
pipeline.execute_sql(bronze_sql)
```

### PySpark ETL Workflow Integration

```python
from workflows.pyspark_etl.pipelines.pipeline import Pipeline
from shared.sql_library.core.data_quality import DataQualityChecks

# Initialize components
pipeline = Pipeline(name="data_ingestion", environment="dev")
quality = DataQualityChecks()

# Use quality checks in pipeline
quality_sql = quality.render_check('completeness_check', {
    'table': 'hive_metastore.retail.customers',
    'column': 'email'
})

# Execute quality check
pipeline.execute_sql(quality_sql)
```

## 📚 Best Practices

### 1. Pattern Usage
- Use existing patterns when possible
- Customize patterns for specific needs
- Document custom patterns

### 2. Parameter Management
- Use descriptive parameter names
- Validate parameters before rendering
- Provide default values when appropriate

### 3. Quality Checks
- Run quality checks at each stage
- Set appropriate thresholds
- Monitor quality trends over time

### 4. Performance
- Optimize SQL patterns for performance
- Use appropriate partitioning
- Monitor execution times

## 🔧 Troubleshooting

### Common Issues

1. **Pattern Not Found**
   ```
   Error: Pattern 'custom_pattern' not found
   ```
   **Solution**: Check pattern name and ensure it exists

2. **Missing Parameters**
   ```
   Error: Missing required parameters: ['catalog', 'schema']
   ```
   **Solution**: Provide all required parameters

3. **Invalid SQL**
   ```
   Error: Invalid SQL generated
   ```
   **Solution**: Check parameter values and SQL template

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Debug pattern rendering
sql = patterns.render_pattern('bronze_ingestion', parameters, debug=True)
```

## 🎯 Next Steps

1. **Add more patterns** for common use cases
2. **Implement pattern versioning** for backward compatibility
3. **Add pattern testing framework** for validation
4. **Create pattern marketplace** for sharing custom patterns

---

**For more information, see the [Getting Started](getting_started.md) guide or [CLI Guide](cli_guide.md).**
