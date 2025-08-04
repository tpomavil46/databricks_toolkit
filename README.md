# Databricks Toolkit

A comprehensive toolkit for building data pipelines with loose coupling between SQL and Python, following the Medallion Architecture.

## 🎯 Overview

This toolkit provides:
- **Exploratory Data Analysis (EDA)** tools for understanding your data
- **Bronze Layer Ingestion** for raw data ingestion
- **SQL-Driven Pipelines** with loose coupling between SQL and Python
- **Project Organization** for multiple domains (retail, ecommerce, healthcare)
- **Reusable Framework** for easy pipeline generation

## 🚀 Quick Start

### 1. Data Discovery & Exploration

```bash
# List available datasets
make run JOB=find_datasets

# Explore a specific dataset
DATASET_PATH="dbfs:/databricks-datasets/retail-org/customers/" \
DATASET_NAME="Retail Customers" \
make run JOB=data_explorer
```

### 2. Bronze Layer Ingestion

```bash
# Create bronze table from raw data
make run JOB=bronze_ingestion \
  --source_path="dbfs:/databricks-datasets/retail-org/customers/" \
  --bronze_table_name="retail_customers_bronze" \
  --project_name="retail"
```

### 3. Run Pipelines

```bash
# Retail pipeline
make run JOB=retail_pipeline

# Ecommerce pipeline (with DBFS data)
make run JOB=dbfs_ecommerce_ingestion
make run JOB=ecommerce_pipeline

# Healthcare pipeline (with DBFS data)
make run JOB=dbfs_healthcare_ingestion
make run JOB=healthcare_pipeline
```

## 📁 Project Structure

```
databricks_toolkit/
├── eda/                          # Exploratory Data Analysis
│   ├── data_explorer.py          # Comprehensive data exploration
│   ├── bronze_ingestion.py       # Bronze layer ingestion
│   └── README.md                 # EDA workflow guide
├── pipelines/                    # Pipeline implementations
│   ├── sql_driven_pipeline.py   # Core framework class
│   ├── retail_pipeline.py       # Retail domain pipeline
│   ├── ecommerce_pipeline.py    # Ecommerce domain pipeline
│   └── healthcare_pipeline.py   # Healthcare domain pipeline
├── sql/                         # SQL files organized by project
│   ├── bronze/retail/           # Retail bronze layer SQL
│   ├── silver/retail/           # Retail silver layer SQL
│   ├── gold/retail/             # Retail gold layer SQL
│   ├── bronze/ecommerce/        # Ecommerce bronze layer SQL
│   ├── silver/ecommerce/        # Ecommerce silver layer SQL
│   ├── gold/ecommerce/          # Ecommerce gold layer SQL
│   ├── bronze/healthcare/       # Healthcare bronze layer SQL
│   ├── silver/healthcare/       # Healthcare silver layer SQL
│   └── gold/healthcare/         # Healthcare gold layer SQL
├── core/                        # Core framework components
├── utils/                       # Utility functions
└── local_runner.py              # Pipeline execution runner
```

## 🔍 EDA Workflow

### Step 1: Data Discovery
```bash
# Find available datasets
make run JOB=find_datasets
```

### Step 2: Data Exploration
```bash
# Explore dataset structure and quality
DATASET_PATH="your_dataset_path" \
DATASET_NAME="Your Dataset Name" \
make run JOB=data_explorer
```

**What you'll get:**
- 📊 Basic information (rows, columns, data types)
- 📋 Detailed schema analysis
- 🔍 Data quality metrics (nulls, distinct values, empty strings)
- 📄 Sample data (first and last 5 rows)
- 📈 Statistical summary for numeric columns
- 🔬 Column-by-column analysis

### Step 3: Bronze Ingestion
```bash
# Create bronze table from raw data
make run JOB=bronze_ingestion \
  --source_path="your_source_path" \
  --bronze_table_name="your_bronze_table" \
  --project_name="your_project"
```

## 🏗️ Pipeline Framework

### Core Features

1. **Loose Coupling**: SQL separated from Python orchestration
2. **Reusable Framework**: Same `SQLDrivenPipeline` class works for all projects
3. **Project Organization**: Dedicated pipelines in `pipelines/` folder
4. **Dynamic SQL Mapping**: Each project has its own SQL file names
5. **Multiple Data Sources**: Real DBFS data transformed for different domains

### Available Pipelines

```bash
# Retail Pipeline (with existing retail_customers_bronze)
make run JOB=retail_pipeline

# Ecommerce Pipeline (with DBFS data)
make run JOB=dbfs_ecommerce_ingestion  # Create data from DBFS
make run JOB=ecommerce_pipeline         # Run pipeline

# Healthcare Pipeline (with DBFS data)
make run JOB=dbfs_healthcare_ingestion  # Create data from DBFS
make run JOB=healthcare_pipeline        # Run pipeline
```

## 📊 Understanding Your Data

After running EDA, you should understand:

### 1. **Data Structure**
- What columns are available?
- What are the data types?
- How many rows and columns?

### 2. **Data Quality**
- Are there missing values (nulls)?
- Are there empty strings?
- How many distinct values per column?
- Are there data type issues?

### 3. **Data Distribution**
- What are the most common values?
- What's the range for numeric columns?
- Are there outliers or anomalies?

### 4. **Business Context**
- What does each column represent?
- What are the business rules?
- What are the expected patterns?

## 🎯 Next Steps

After EDA, you're ready to:

1. **Design Silver Layer**
   - Plan data cleaning and transformation
   - Define business rules
   - Create data quality checks

2. **Design Gold Layer**
   - Identify KPIs and metrics
   - Plan aggregations and summaries
   - Define end-user requirements

3. **Create SQL Files**
   - Write bronze ingestion SQL
   - Write silver transformation SQL
   - Write gold KPI generation SQL

## 📚 Documentation

- [EDA Workflow Guide](eda/README.md) - Comprehensive EDA documentation
- [SQL-Driven Pipeline Guide](README_SQL_DRIVEN.md) - Pipeline framework documentation
- [Commands Reference](COMMANDS.md) - Available commands and examples

## 🔧 Customization

### Adding New Projects

1. Create SQL files in `sql/bronze/your_project/`, `sql/silver/your_project/`, `sql/gold/your_project/`
2. Create pipeline file in `pipelines/your_project_pipeline.py`
3. Update `SQLDrivenPipeline` class with your project's SQL file names

### Adding New Data Sources

To explore custom data sources:
1. **DBFS Files**: Use `dbfs:/your/path/`
2. **Tables**: Use table name directly
3. **External Sources**: Modify the `_load_dataset` method in `data_explorer.py`

## 📝 Best Practices

1. **Always explore before ingesting** - Understand your data first
2. **Document your findings** - Keep notes on data quality issues
3. **Plan your transformations** - Know what you'll do in silver layer
4. **Consider business context** - Understand what the data represents
5. **Check data quality** - Look for nulls, duplicates, anomalies

## 🆘 Troubleshooting

### Common Issues

1. **Dataset not found**: Check the path and format
2. **Permission errors**: Verify DBFS access
3. **Schema conflicts**: Drop existing tables if needed
4. **Memory issues**: Use smaller sample for large datasets

### Getting Help

- Check the dataset path exists: `make run JOB=find_datasets`
- Try different file formats (CSV, Parquet, Delta, JSON)
- Verify your Databricks connection: `databricks-connect test`

## 📚 Additional Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Spark SQL Reference](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/)
