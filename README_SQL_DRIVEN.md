# SQL-Driven Pipeline Framework

## Overview

This framework demonstrates how to pass SQL files as arguments to Python code, making the Python code reusable and generalized while maintaining loose coupling between SQL and Python.

## Key Benefits

### 🎯 **Loose Coupling**
- SQL files can be modified without touching Python code
- Python code is reusable across different SQL implementations
- Clear separation of concerns between data logic (SQL) and orchestration (Python)

### 🔄 **Reusability**
- Same Python code can execute different SQL files
- Easy to add new pipeline steps by creating new SQL files
- Parameterized SQL allows for flexible configurations

### 🏗️ **Maintainability**
- SQL logic is isolated in separate files
- Python code follows SOLID principles
- Easy to test SQL and Python independently

### 📈 **Scalability**
- New pipeline types can be added by creating new SQL files
- Existing Python code doesn't need modification
- Supports complex parameter substitution

## Architecture

```
databricks_toolkit/
├── core/
│   └── sql_pipeline_executor.py    # Core SQL execution framework
├── pipelines/
│   └── sql_driven_pipeline.py      # SQL-driven pipeline implementation
├── sql/                            # SQL files (separated from Python)
│   ├── bronze/
│   │   └── ingest_data.sql
│   ├── silver/
│   │   └── transform_data.sql
│   └── gold/
│       └── generate_kpis.sql
├── main_sql_driven.py              # Entry point for SQL-driven pipeline
└── examples/
    └── sql_driven_example.py       # Usage examples
```

## Core Components

### 1. SQLPipelineExecutor

The core class that handles SQL execution with parameter substitution:

```python
from core.sql_pipeline_executor import SQLPipelineExecutor, SQLPipelineBuilder

# Create executor
executor = SQLPipelineExecutor(spark)

# Execute SQL from file with parameters
config = (SQLPipelineBuilder()
         .from_file("sql/bronze/ingest_data.sql")
         .with_parameters({
             'input_path': '/data/raw/orders.csv',
             'bronze_table_name': 'orders_bronze',
             'file_format': 'csv'
         })
         .write_to_table('orders_bronze', format='delta')
         .build())

executor.execute_sql(config)
```

### 2. SQLDrivenPipeline

High-level pipeline that orchestrates SQL execution:

```python
from pipelines.sql_driven_pipeline import SQLDrivenPipeline

# Create pipeline
pipeline = SQLDrivenPipeline(spark, sql_base_path="sql")

# Run full pipeline
results = pipeline.run_full_pipeline(
    input_path='/data/raw/orders.csv',
    bronze_table='orders_bronze',
    silver_table='orders_silver',
    gold_table='orders_gold',
    vendor_filter=123
)
```

## SQL File Examples

### Bronze Layer (sql/bronze/ingest_data.sql)

```sql
-- Bronze Layer: Data Ingestion
-- Parameters: ${input_path}, ${bronze_table_name}, ${file_format}

CREATE TABLE IF NOT EXISTS ${bronze_table_name}
USING ${file_format}
AS SELECT 
    *,
    current_timestamp() as ingestion_timestamp,
    'bronze_layer' as data_layer
FROM ${input_path}
```

### Silver Layer (sql/silver/transform_data.sql)

```sql
-- Silver Layer: Data Transformation
-- Parameters: ${bronze_table_name}, ${silver_table_name}, ${vendor_filter_condition}

CREATE TABLE IF NOT EXISTS ${silver_table_name}
USING delta
AS SELECT 
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    quantity * unit_price as calculated_total,
    CASE 
        WHEN quantity > 0 AND unit_price > 0 THEN 'valid'
        ELSE 'invalid'
    END as data_quality_flag,
    current_timestamp() as transformation_timestamp,
    'silver_layer' as data_layer
FROM ${bronze_table_name}
WHERE 1=1
    ${vendor_filter_condition}
```

### Gold Layer (sql/gold/generate_kpis.sql)

```sql
-- Gold Layer: KPI Generation
-- Parameters: ${silver_table_name}, ${gold_table_name}, ${vendor_filter_condition}

CREATE TABLE IF NOT EXISTS ${gold_table_name}
USING delta
AS SELECT 
    customer_id,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_spend,
    AVG(total_amount) as avg_order_value,
    current_timestamp() as kpi_generation_timestamp,
    'gold_layer' as data_layer
FROM ${silver_table_name}
WHERE 1=1
    ${vendor_filter_condition}
GROUP BY customer_id
```

## Usage Examples

### Basic Usage

```bash
python main_sql_driven.py \
    --input_table /data/raw/orders.csv \
    --bronze_path orders_bronze \
    --silver_path orders_silver \
    --gold_path orders_gold \
    --vendor_filter 123
```

### Custom SQL Base Path

```bash
python main_sql_driven.py \
    --input_table /data/raw/customers.json \
    --bronze_path customers_bronze \
    --silver_path customers_silver \
    --gold_path customers_gold \
    --sql_base_path custom_sql
```

### Programmatic Usage

```python
from pipelines.sql_driven_pipeline import run_sql_pipeline

# Execute pipeline
results = run_sql_pipeline(
    spark=spark,
    input_table='/data/raw/orders.csv',
    bronze_path='orders_bronze',
    silver_path='orders_silver',
    gold_path='orders_gold',
    format='delta',
    vendor_filter=123
)

if results['success']:
    print(f"Pipeline completed: {results['steps_completed']}")
else:
    print(f"Pipeline failed: {results['errors']}")
```

## Parameter Substitution

The framework supports parameter substitution in SQL files using `${parameter_name}` syntax:

```sql
-- SQL template with parameters
SELECT * FROM ${input_table}
WHERE vendor_id = ${vendor_id}
AND order_date >= '${start_date}'
```

```python
# Parameters to substitute
parameters = {
    'input_table': 'orders_silver',
    'vendor_id': 123,
    'start_date': '2024-01-01'
}
```

## Adding New Pipeline Steps

To add a new pipeline step, simply create a new SQL file:

1. **Create SQL file** (e.g., `sql/quality/data_quality_check.sql`):
```sql
-- Data Quality Check
-- Parameters: ${input_table}, ${quality_threshold}

SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN data_quality_flag = 'valid' THEN 1 END) as valid_records,
    COUNT(CASE WHEN data_quality_flag = 'invalid' THEN 1 END) as invalid_records
FROM ${input_table}
```

2. **Add to pipeline** (no Python code changes needed):
```python
# The existing Python code can execute this new SQL file
# by simply passing the file path as a parameter
```

## Benefits for Different "Ilities"

### 🔄 **Scalability**
- Easy to add new pipeline types by creating new SQL files
- Python code scales horizontally without modification

### 🛡️ **Reliability**
- SQL files can be version controlled independently
- Error handling and logging built into the framework

### 📊 **Observability**
- Comprehensive logging of SQL execution
- Pipeline step tracking and error reporting

### 🔧 **Maintainability**
- Clear separation between SQL logic and Python orchestration
- Easy to modify SQL without touching Python code

### 🔌 **Extensibility**
- New SQL files can be added without Python changes
- Framework supports custom parameter substitution

### 🧪 **Testability**
- SQL files can be tested independently
- Python code can be unit tested with mock SQL

### 🔒 **Security**
- Parameter substitution prevents SQL injection
- Safe templating approach

## Comparison with Traditional Approach

### Traditional Approach (Tightly Coupled)
```python
# SQL embedded in Python code
def run_bronze(spark, input_path, bronze_table):
    sql = f"""
    CREATE TABLE {bronze_table}
    AS SELECT * FROM {input_path}
    """
    spark.sql(sql)
```

### SQL-Driven Approach (Loosely Coupled)
```python
# SQL in separate file, Python code is reusable
def run_bronze(spark, input_path, bronze_table):
    config = SQLPipelineBuilder()\
        .from_file("sql/bronze/ingest_data.sql")\
        .with_parameters({
            'input_path': input_path,
            'bronze_table_name': bronze_table
        })\
        .build()
    
    executor.execute_sql(config)
```

## Best Practices

1. **SQL File Organization**
   - Organize SQL files by layer (bronze, silver, gold)
   - Use descriptive file names
   - Include parameter documentation in SQL comments

2. **Parameter Naming**
   - Use descriptive parameter names
   - Document parameter types and expected values
   - Use consistent naming conventions

3. **Error Handling**
   - Use the `continue_on_error` option when appropriate
   - Log SQL execution for debugging
   - Validate parameters before SQL execution

4. **Testing**
   - Test SQL files independently
   - Mock SQL execution in Python unit tests
   - Use parameter validation in SQL files

## Conclusion

This SQL-driven framework achieves the goal of making Python code reusable and generalized while maintaining loose coupling between SQL and Python. The key insight is that SQL files become arguments to the Python code, allowing for:

- **Flexibility**: Different SQL files for different use cases
- **Reusability**: Same Python code works with different SQL
- **Maintainability**: SQL and Python can evolve independently
- **Scalability**: Easy to add new pipeline types

This approach follows the "ilities" principles and SOLID design patterns, making the codebase more robust, maintainable, and extensible. 