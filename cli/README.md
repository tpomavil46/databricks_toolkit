# CLI Tools for Data Exploration and Analysis

This directory contains command-line tools for exploring and analyzing datasets in your Databricks environment.

## 🎯 Overview

The CLI tools provide a complete workflow for:
1. **Data Discovery** - Find and explore available datasets
2. **Data Preview** - Preview data files and understand structure
3. **Data Analysis** - Comprehensive analysis with data quality metrics

## 🚀 Getting Started

**First, activate your virtual environment:**
```bash
. .venv/bin/activate
```

**Then use any of the commands below without the activation prefix.**

## 📋 Available Tools

### 1. `dbfs_cli.py` - Dataset Discovery
**Purpose**: Explore DBFS paths recursively to find datasets.

```bash
# List top-level datasets
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/ --no-recursive

# Drill deeper into specific dataset (limited results)
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/retail-org/ --limit 20

# List only files (no directories) with pagination
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/ --files-only --limit 10

# Paginate through results
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/retail-org/ --limit 10 --offset 20
```

### 2. `query_file.py` - Data Preview
**Purpose**: Preview data files to understand structure and content.

```bash
# Preview CSV files
python cli/query_file.py --path /databricks-datasets/retail-org/customers/ --format csv --limit 10

# Preview Parquet files
python cli/query_file.py --path /databricks-datasets/nyctaxi/tripdata/ --format parquet --limit 5

# Preview JSON files
python cli/query_file.py --path /databricks-datasets/events/ --format json --limit 3
```

### 3. `analyze_dataset.py` - Comprehensive Analysis
**Purpose**: Deep analysis with data quality metrics, schema info, and statistics.

```bash
# Analyze a DBFS dataset
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/ --name "Retail Customers"

# Analyze a table
python cli/analyze_dataset.py --path retail_customers_bronze --name "Bronze Table"

# Analyze with custom row limit for performance
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/nyctaxi/tripdata/ --max-rows 2000
```

### 4. `bronze_ingestion.py` - Bronze Layer Ingestion
**Purpose**: Ingest raw data into bronze tables with metadata tracking.

```bash
# Ingest from DBFS
python cli/bronze_ingestion.py --source-path dbfs:/databricks-datasets/retail-org/customers/ --bronze-table-name retail_customers_bronze

# Ingest from existing table
python cli/bronze_ingestion.py --source-path existing_table --bronze-table-name new_bronze_table --project-name ecommerce
```

### 5. `drop_table.py` - Table Management
**Purpose**: Safely drop tables to avoid schema conflicts.

```bash
# Drop a specific table
python cli/drop_table.py --table-name retail_customers_bronze

# Drop test tables
python cli/drop_table.py --table-name test_table_001
```

## 🚀 Complete EDA Workflow

### Step 1: Discover Available Datasets
```bash
# Find all available datasets
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/ --no-recursive

# Explore specific dataset structure (limited results)
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/retail-org/ --limit 20
```

### Step 2: Preview Data Files
```bash
# Preview the data structure
python cli/query_file.py --path /databricks-datasets/retail-org/customers/ --format csv --limit 10
```

### Step 3: Comprehensive Analysis
```bash
# Deep analysis with data quality metrics
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/ --name "Retail Customers" --max-rows 5000
```

### Step 4: Bronze Layer Ingestion
```bash
# Ingest data into bronze table
python cli/bronze_ingestion.py --source-path dbfs:/databricks-datasets/retail-org/customers/ --bronze-table-name retail_customers_bronze --project-name retail
```

## 📊 What You Get from Analysis

### Basic Information
- Total rows and columns
- Column names and data types
- Dataset structure overview

### Schema Analysis
- Detailed column information
- Data type specifications
- Nullable field information

### Data Quality Metrics
- Null value counts and percentages
- Distinct value counts
- Empty string detection
- Data completeness assessment

### Sample Data
- First 5 rows
- Last 5 rows
- Data format verification

### Statistical Summary
- Numeric column statistics
- Min, max, quartiles
- Distribution analysis

### Column Analysis
- Top values for string columns
- Value ranges for numeric columns
- Data distribution patterns

## 🎯 Use Cases

### 1. **New Dataset Exploration**
```bash
# Discover what's available
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/ --no-recursive

# Drill into interesting dataset (limited results)
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/retail-org/ --limit 20

# Preview the data
python cli/query_file.py --path /databricks-datasets/retail-org/customers/ --format csv --limit 5

# Full analysis
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/ --name "Retail Customers"
```

### 2. **Data Quality Assessment**
```bash
# Analyze existing table
python cli/analyze_dataset.py --path retail_customers_bronze --name "Bronze Table Quality Check"
```

### 3. **Schema Understanding**
```bash
# Quick preview
python cli/query_file.py --path /databricks-datasets/nyctaxi/tripdata/ --format parquet --limit 3

# Detailed schema analysis
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/nyctaxi/tripdata/ --name "NYC Taxi Data"
```

### 4. **Bronze Layer Management**
```bash
# Ingest new data
python cli/bronze_ingestion.py --source-path dbfs:/databricks-datasets/retail-org/customers/ --bronze-table-name retail_customers_bronze

# Clean up test tables
python cli/drop_table.py --table-name test_table_001
```

## 🔧 Performance Tips

### For Large Datasets
```bash
# Use smaller sample for analysis
python cli/analyze_dataset.py --path dbfs:/large-dataset/ --max-rows 1000

# Preview first few rows only
python cli/query_file.py --path /large-dataset/ --format parquet --limit 3
```

### For Quick Discovery
```bash
# List only files (faster, limited results)
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/ --files-only --limit 10

# Non-recursive for top-level only
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/ --no-recursive

# Paginate through large directories
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/retail-org/ --limit 10 --offset 0
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/retail-org/ --limit 10 --offset 10
```

## 📝 Best Practices

1. **Start with Discovery** - Use `dbfs_cli.py` to find datasets
2. **Preview Before Analysis** - Use `query_file.py` for quick structure check
3. **Analyze with Purpose** - Use `analyze_dataset.py` for detailed insights
4. **Use Performance Limits** - Set `--max-rows` for large datasets
5. **Document Findings** - Keep notes on data quality issues

## 🆘 Troubleshooting

### Common Issues

1. **Cluster Not Found**: Update cluster ID in the script
2. **Path Not Found**: Check the DBFS path exists
3. **Format Issues**: Try different formats (csv, parquet, json, delta)
4. **Performance**: Reduce `--max-rows` for large datasets

### Getting Help

```bash
# Check tool help
python cli/dbfs_cli.py --help
python cli/query_file.py --help
python cli/analyze_dataset.py --help
```

## 📚 Integration with EDA Workflow

These CLI tools complement the EDA workflow in the `eda/` folder:

1. **CLI Tools**: Quick exploration and discovery
2. **EDA Tools**: Comprehensive analysis and bronze ingestion
3. **Pipeline Tools**: Full pipeline execution

Use CLI tools for initial exploration, then move to EDA tools for detailed analysis and ingestion. 