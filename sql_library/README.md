# SQL Library

A comprehensive SQL library for Databricks providing standardized patterns, functions, templates, and data quality checks.

## 📚 Overview

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
from sql_library.core.sql_patterns import SQLPatterns
from sql_library.core.data_quality import DataQualityChecks
from sql_library.core.sql_functions import SQLFunctions
from sql_library.core.sql_templates import SQLTemplates

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
python sql_library/cli/sql_library_cli.py list-patterns

# Render a pattern with parameters
python sql_library/cli/sql_library_cli.py render-pattern bronze_ingestion \
  --parameters catalog=hive_metastore schema=retail table_name=customers

# Create a function library
python sql_library/cli/sql_library_cli.py create-function-library --output-file functions.sql

# Search the library
python sql_library/cli/sql_library_cli.py search "bronze"
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
    'table_name': 'customers',
    'columns': 'customer_id, customer_name, email, phone, address, city, state, country',
    'quality_conditions': 'customer_id IS NOT NULL AND customer_name IS NOT NULL'
})
```

## ⚙️ SQL Functions

### Available Functions

#### String Functions
- `clean_string()` - Clean and standardize string values
- `validate_email()` - Validate email format using regex
- `extract_domain()` - Extract domain from email address

#### Date/Time Functions
- `age_in_days()` - Calculate age in days between two dates
- `business_days_between()` - Calculate business days between dates
- `is_weekend()` - Check if a date falls on weekend

#### Numeric Functions
- `calculate_percentage()` - Calculate percentage with null handling
- `safe_division()` - Safe division with null handling
- `is_in_range()` - Check if value is within specified range

#### Quality Functions
- `data_quality_score()` - Calculate data quality score based on multiple checks
- `null_percentage()` - Calculate percentage of null values in a column

#### Business Functions
- `customer_segment()` - Segment customers based on total spend
- `order_status()` - Determine order status based on business rules

### Function Usage

```python
# Generate function definition
clean_string_func = functions.render_function_definition('clean_string')

# Create complete function library
library_sql = functions.create_function_library('functions.sql')
```

## 📋 SQL Templates

### Available Templates

| Template | Category | Description |
|----------|----------|-------------|
| `bronze_to_silver_pipeline` | pipeline | Complete bronze to silver pipeline |
| `incremental_merge_pipeline` | pipeline | Incremental merge operations |
| `customer_analytics` | analytics | Customer analytics dashboard |
| `sales_analytics` | analytics | Sales analytics and KPIs |
| `comprehensive_quality_check` | quality | Complete data quality assessment |
| `executive_summary` | reporting | Executive summary reports |
| `table_optimization` | maintenance | Table optimization and performance |
| `data_retention` | maintenance | Data retention policy implementation |

### Template Usage

```python
# Bronze to silver pipeline template
pipeline_sql = templates.render_template('bronze_to_silver_pipeline', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customers',
    'quality_conditions': 'customer_id IS NOT NULL AND customer_name IS NOT NULL',
    'transformations': '''
        customer_id,
        TRIM(customer_name) as customer_name,
        LOWER(TRIM(email)) as email,
        REGEXP_REPLACE(phone, '[^0-9]', '') as phone_clean,
        INITCAP(TRIM(city)) as city,
        UPPER(TRIM(state)) as state,
        UPPER(TRIM(country)) as country
    '''
})
```

## 🔍 Data Quality Framework

### Quality Check Categories

#### Completeness Checks
- `null_check` - Check for null values in critical columns
- `empty_string_check` - Check for empty strings in text columns

#### Accuracy Checks
- `range_check` - Check if numeric values are within expected range
- `format_check` - Check if values match expected format using regex

#### Consistency Checks
- `case_consistency` - Check for consistent case in text columns
- `data_type_consistency` - Check if all values in a column are of the same data type

#### Validity Checks
- `domain_check` - Check if values are within expected domain
- `length_check` - Check if string values are within expected length range

#### Uniqueness Checks
- `primary_key_uniqueness` - Check if primary key values are unique
- `composite_key_uniqueness` - Check if composite key combinations are unique

#### Timeliness Checks
- `data_freshness` - Check if data is fresh (not older than expected)
- `update_frequency` - Check if data is updated frequently enough

### Quality Check Usage

```python
# Run a null check
null_check_sql = quality.render_check('null_check', {
    'table': 'hive_metastore.retail.customers_bronze',
    'column': 'customer_id'
})

# Run comprehensive quality suite
quality_suite = quality.run_quality_suite('customers_bronze', [
    'null_check', 'uniqueness_check', 'format_check'
], {
    'column': 'customer_id',
    'regex_pattern': '^[0-9]+$'
})
```

## 🛠️ CLI Reference

### Commands

#### Patterns
```bash
# List all patterns
python sql_library/cli/sql_library_cli.py list-patterns

# List patterns by category
python sql_library/cli/sql_library_cli.py list-patterns --category ingestion

# Render a pattern
python sql_library/cli/sql_library_cli.py render-pattern bronze_ingestion \
  --parameters catalog=hive_metastore schema=retail table_name=customers
```

#### Quality Checks
```bash
# List all quality checks
python sql_library/cli/sql_library_cli.py list-quality-checks

# List checks by category
python sql_library/cli/sql_library_cli.py list-quality-checks --category completeness

# Render a quality check
python sql_library/cli/sql_library_cli.py render-quality-check null_check \
  --parameters table=customers_bronze column=customer_id
```

#### Functions
```bash
# List all functions
python sql_library/cli/sql_library_cli.py list-functions

# List functions by category
python sql_library/cli/sql_library_cli.py list-functions --category string

# Render a function
python sql_library/cli/sql_library_cli.py render-function clean_string

# Create function library
python sql_library/cli/sql_library_cli.py create-function-library --output-file functions.sql
```

#### Templates
```bash
# List all templates
python sql_library/cli/sql_library_cli.py list-templates

# List templates by category
python sql_library/cli/sql_library_cli.py list-templates --category pipeline

# Render a template
python sql_library/cli/sql_library_cli.py render-template bronze_to_silver_pipeline \
  --parameters catalog=hive_metastore schema=retail table_name=customers

# Create template library
python sql_library/cli/sql_library_cli.py create-template-library --output-file templates.sql
```

#### Search
```bash
# Search across all components
python sql_library/cli/sql_library_cli.py search "bronze"
```

## 📖 Examples

### Complete Data Pipeline

```python
from sql_library.core.sql_patterns import SQLPatterns
from sql_library.core.sql_templates import SQLTemplates

patterns = SQLPatterns()
templates = SQLTemplates()

# Step 1: Bronze ingestion
bronze_sql = patterns.render_pattern('bronze_ingestion', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customers',
    'source_table': 'csv.`/mnt/databricks-datasets/retail-org/customers/`'
})

# Step 2: Bronze to silver pipeline
silver_sql = templates.render_template('bronze_to_silver_pipeline', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'table_name': 'customers',
    'quality_conditions': 'customer_id IS NOT NULL AND customer_name IS NOT NULL',
    'transformations': '''
        customer_id,
        TRIM(customer_name) as customer_name,
        LOWER(TRIM(email)) as email,
        REGEXP_REPLACE(phone, '[^0-9]', '') as phone_clean,
        INITCAP(TRIM(city)) as city,
        UPPER(TRIM(state)) as state,
        UPPER(TRIM(country)) as country
    '''
})

# Step 3: Data quality check
from sql_library.core.data_quality import DataQualityChecks
quality = DataQualityChecks()

quality_sql = quality.render_check('null_check', {
    'table': 'hive_metastore.retail.customers_silver',
    'column': 'customer_id'
})
```

### Customer Analytics Dashboard

```python
from sql_library.core.sql_templates import SQLTemplates

templates = SQLTemplates()

# Generate customer analytics
analytics_sql = templates.render_template('customer_analytics', {
    'catalog': 'hive_metastore',
    'schema': 'retail',
    'start_date': '2024-01-01',
    'end_date': '2024-12-31'
})
```

## 🧪 Testing

### Run SQL Library Tests

```bash
# Run the SQL library test suite
python test_sql_library.py
```

### Test Individual Components

```python
# Test patterns
patterns = SQLPatterns()
all_patterns = patterns.list_patterns()
print(f"Found {len(all_patterns)} patterns")

# Test quality checks
quality = DataQualityChecks()
all_checks = quality.list_checks()
print(f"Found {len(all_checks)} quality checks")

# Test functions
functions = SQLFunctions()
all_functions = functions.list_functions()
print(f"Found {len(all_functions)} functions")

# Test templates
templates = SQLTemplates()
all_templates = templates.list_templates()
print(f"Found {len(all_templates)} templates")
```

## 🔧 Configuration

### Environment Variables

The SQL library uses the same configuration as the main Databricks Toolkit:

- `DATABRICKS_HOST` - Databricks workspace URL
- `DATABRICKS_TOKEN` - Databricks access token
- `DATABRICKS_CLUSTER_ID` - Target cluster ID

### Configuration Files

- `.databrickscfg` - Databricks CLI configuration
- `requirements.txt` - Python dependencies

## 🤝 Contributing

### Adding New Patterns

1. Add the pattern to `sql_library/core/sql_patterns.py`
2. Include proper documentation and examples
3. Add tests in `test_sql_library.py`

### Adding New Functions

1. Add the function to `sql_library/core/sql_functions.py`
2. Include proper documentation and examples
3. Add tests in `test_sql_library.py`

### Adding New Templates

1. Add the template to `sql_library/core/sql_templates.py`
2. Include proper documentation and examples
3. Add tests in `test_sql_library.py`

### Adding New Quality Checks

1. Add the check to `sql_library/core/data_quality.py`
2. Include proper documentation and examples
3. Add tests in `test_sql_library.py`

## 📄 License

This project is part of the Databricks Toolkit and follows the same license terms.

## 🆘 Support

For issues and questions:

1. Check the main project documentation
2. Review the examples in this README
3. Run the test suite to verify functionality
4. Check the CLI help for available commands

---

**Version**: 1.0.0  
**Last Updated**: 2024-12-31  
**Maintainer**: Databricks Toolkit Team 