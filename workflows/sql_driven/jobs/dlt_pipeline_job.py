#!/usr/bin/env python3
"""
Databricks DLT Pipeline Job

This creates a proper Databricks Job for the DLT pipeline with visualizations.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def create_dlt_pipeline_job():
    """Create a Databricks Job configuration for DLT pipeline."""
    
    job_config = {
        "name": "DLT-Pipeline-Retail-Analytics",
        "description": "Delta Live Tables pipeline for retail analytics with streaming capabilities",
        "email_notifications": {
            "on_start": ["admin@company.com"],
            "on_success": ["admin@company.com"],
            "on_failure": ["admin@company.com", "data-team@company.com"]
        },
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "setup_environment",
                "description": "Setup environment and create test data",
                "notebook_task": {
                    "notebook_path": "/Repos/databricks_toolkit/workflows/sql_driven/jobs/setup_environment",
                    "base_parameters": {
                        "environment": "prod",
                        "project": "retail"
                    }
                },
                "job_cluster_key": "dlt-cluster",
                "timeout_seconds": 600
            },
            {
                "task_key": "bronze_ingestion",
                "description": "Bronze layer data ingestion with Auto Loader",
                "notebook_task": {
                    "notebook_path": "/Repos/databricks_toolkit/workflows/sql_driven/jobs/bronze_ingestion",
                    "base_parameters": {
                        "source_path": "/tmp/streaming_source",
                        "table_name": "retail_bronze",
                        "file_format": "json"
                    }
                },
                "job_cluster_key": "dlt-cluster",
                "depends_on": [
                    {
                        "task_key": "setup_environment"
                    }
                ],
                "timeout_seconds": 1200
            },
            {
                "task_key": "silver_transformation",
                "description": "Silver layer data transformation with quality checks",
                "notebook_task": {
                    "notebook_path": "/Repos/databricks_toolkit/workflows/sql_driven/jobs/silver_transformation",
                    "base_parameters": {
                        "bronze_table": "retail_bronze",
                        "silver_table": "retail_silver"
                    }
                },
                "job_cluster_key": "dlt-cluster",
                "depends_on": [
                    {
                        "task_key": "bronze_ingestion"
                    }
                ],
                "timeout_seconds": 1200
            },
            {
                "task_key": "gold_aggregation",
                "description": "Gold layer analytics and aggregations",
                "notebook_task": {
                    "notebook_path": "/Repos/databricks_toolkit/workflows/sql_driven/jobs/gold_aggregation",
                    "base_parameters": {
                        "silver_table": "retail_silver",
                        "gold_table": "retail_analytics"
                    }
                },
                "job_cluster_key": "dlt-cluster",
                "depends_on": [
                    {
                        "task_key": "silver_transformation"
                    }
                ],
                "timeout_seconds": 1200
            },
            {
                "task_key": "create_dashboard",
                "description": "Create and update analytics dashboard",
                "notebook_task": {
                    "notebook_path": "/Repos/databricks_toolkit/workflows/sql_driven/jobs/create_dashboard",
                    "base_parameters": {
                        "gold_table": "retail_analytics",
                        "dashboard_name": "Retail Analytics Dashboard"
                    }
                },
                "job_cluster_key": "dlt-cluster",
                "depends_on": [
                    {
                        "task_key": "gold_aggregation"
                    }
                ],
                "timeout_seconds": 600
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "dlt-cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2,
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.databricks.delta.liveTable.enabled": "true"
                    },
                    "aws_attributes": {
                        "availability": "SPOT"
                    }
                }
            }
        ]
    }
    
    return job_config

def create_job_notebooks():
    """Create the notebook files for the job tasks."""
    
    notebooks = {
        "setup_environment": """
# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Environment
# MAGIC 
# MAGIC This notebook sets up the environment and creates test data for the DLT pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC - environment: The environment to run in (dev, staging, prod)
# MAGIC - project: The project name

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("project", "retail")

environment = dbutils.widgets.get("environment")
project = dbutils.widgets.get("project")

print(f"Setting up environment: {environment}")
print(f"Project: {project}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create test data
test_data = [
    {"id": 1, "name": "Product A", "price": 10.99, "category": "electronics", "timestamp": "2024-01-01"},
    {"id": 2, "name": "Product B", "price": 25.50, "category": "clothing", "timestamp": "2024-01-02"},
    {"id": 3, "name": "Product C", "price": 5.99, "category": "books", "timestamp": "2024-01-03"},
    {"id": 4, "name": "Product D", "price": 150.00, "category": "electronics", "timestamp": "2024-01-04"},
    {"id": 5, "name": "Product E", "price": 75.25, "category": "clothing", "timestamp": "2024-01-05"}
]

df = spark.createDataFrame(test_data)
df.write.mode("overwrite").json("dbfs:/tmp/streaming_source/initial_batch")

print("✅ Created test data in dbfs:/tmp/streaming_source/initial_batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup Complete
# MAGIC 
# MAGIC The environment is now ready for the DLT pipeline to process the data.
""",
        
        "bronze_ingestion": """
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC 
# MAGIC This notebook handles the bronze layer data ingestion using Auto Loader.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC - source_path: Path to source data
# MAGIC - table_name: Name of the bronze table
# MAGIC - file_format: Format of source files

# COMMAND ----------

dbutils.widgets.text("source_path", "/tmp/streaming_source")
dbutils.widgets.text("table_name", "retail_bronze")
dbutils.widgets.text("file_format", "json")

source_path = dbutils.widgets.get("source_path")
table_name = dbutils.widgets.get("table_name")
file_format = dbutils.widgets.get("file_format")

print(f"Source path: {source_path}")
print(f"Table name: {table_name}")
print(f"File format: {file_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer SQL
# MAGIC 
# MAGIC This creates a streaming table that processes new data as it arrives.

# COMMAND ----------

bronze_sql = f'''
CREATE OR REFRESH STREAMING TABLE {table_name}
  (CONSTRAINT valid_timestamp EXPECT (processing_time IS NOT NULL) ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_source EXPECT (source_file IS NOT NULL) ON VIOLATION DROP)
COMMENT "Bronze layer streaming ingestion using Auto Loader for incremental processing"
TBLPROPERTIES ("quality" = "bronze", "layer" = "bronze")
AS
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file,
  _metadata.file_size AS file_size,
  _metadata.file_modification_time AS file_timestamp
FROM cloud_files("dbfs:{source_path}", "{file_format}", map("cloudFiles.inferColumnTypes", "true", "cloudFiles.schemaLocation", "dbfs:/tmp/schema_location"))
'''

print("Executing Bronze Layer SQL...")
spark.sql(bronze_sql)
print("✅ Bronze layer ingestion completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Complete
# MAGIC 
# MAGIC The bronze layer is now ready to process new data as it arrives.
""",
        
        "silver_transformation": """
# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformation
# MAGIC 
# MAGIC This notebook handles the silver layer data transformation with quality checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC - bronze_table: Source bronze table
# MAGIC - silver_table: Target silver table

# COMMAND ----------

dbutils.widgets.text("bronze_table", "retail_bronze")
dbutils.widgets.text("silver_table", "retail_silver")

bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")

print(f"Bronze table: {bronze_table}")
print(f"Silver table: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer SQL
# MAGIC 
# MAGIC This creates a streaming table with data quality constraints.

# COMMAND ----------

silver_sql = f'''
CREATE OR REFRESH STREAMING TABLE {silver_table}
  (CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE)
  (CONSTRAINT valid_timestamp EXPECT (timestamp > "2021-01-01") ON VIOLATION DROP)
  (CONSTRAINT valid_price EXPECT (price >= 0) ON VIOLATION DROP)
COMMENT "Silver layer streaming transformation with data quality enforcement"
TBLPROPERTIES ("quality" = "silver", "layer" = "silver")
AS
SELECT 
  id,
  name,
  price,
  category,
  timestamp,
  CAST(timestamp AS TIMESTAMP) AS order_timestamp,
  CAST(price AS DECIMAL(10,2)) AS clean_price,
  CASE 
    WHEN price >= 100 THEN 'high_value'
    WHEN price >= 50 THEN 'medium_value'
    ELSE 'low_value'
  END AS product_category,
  CASE 
    WHEN timestamp > "2021-01-01" THEN 'valid_date'
    ELSE 'invalid_date'
  END AS date_quality_flag,
  CASE 
    WHEN price >= 0 THEN 'valid_price'
    ELSE 'invalid_price'
  END AS price_quality_flag,
  current_timestamp() AS silver_processing_time,
  'silver_layer' AS data_layer
FROM STREAM(LIVE.{bronze_table})
WHERE timestamp > "2021-01-01"
  AND price >= 0
  AND id IS NOT NULL
'''

print("Executing Silver Layer SQL...")
spark.sql(silver_sql)
print("✅ Silver layer transformation completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Complete
# MAGIC 
# MAGIC The silver layer is now ready with data quality checks.
""",
        
        "gold_aggregation": """
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Aggregation
# MAGIC 
# MAGIC This notebook handles the gold layer analytics and aggregations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC - silver_table: Source silver table
# MAGIC - gold_table: Target gold table

# COMMAND ----------

dbutils.widgets.text("silver_table", "retail_silver")
dbutils.widgets.text("gold_table", "retail_analytics")

silver_table = dbutils.widgets.get("silver_table")
gold_table = dbutils.widgets.get("gold_table")

print(f"Silver table: {silver_table}")
print(f"Gold table: {gold_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer SQL
# MAGIC 
# MAGIC This creates a materialized view for real-time analytics.

# COMMAND ----------

gold_sql = f'''
CREATE OR REFRESH MATERIALIZED VIEW {gold_table}
COMMENT "Gold layer materialized view for real-time business insights"
TBLPROPERTIES ("quality" = "gold", "layer" = "gold")
AS
SELECT 
  date(order_timestamp) AS order_date,
  hour(order_timestamp) AS order_hour,
  id,
  name,
  category,
  COUNT(*) AS total_products,
  SUM(clean_price) AS total_revenue,
  AVG(clean_price) AS avg_price,
  MAX(clean_price) AS max_price,
  MIN(clean_price) AS min_price,
  product_category,
  COUNT(CASE WHEN product_category = 'high_value' THEN 1 END) AS high_value_products,
  COUNT(CASE WHEN product_category = 'medium_value' THEN 1 END) AS medium_value_products,
  COUNT(CASE WHEN product_category = 'low_value' THEN 1 END) AS low_value_products,
  COUNT(CASE WHEN date_quality_flag = 'valid_date' THEN 1 END) AS valid_date_products,
  COUNT(CASE WHEN price_quality_flag = 'valid_price' THEN 1 END) AS valid_price_products,
  SUM(CASE WHEN product_category = 'high_value' THEN clean_price ELSE 0 END) AS high_value_revenue,
  SUM(CASE WHEN product_category = 'medium_value' THEN clean_price ELSE 0 END) AS medium_value_revenue,
  SUM(CASE WHEN product_category = 'low_value' THEN clean_price ELSE 0 END) AS low_value_revenue,
  current_timestamp() AS gold_processing_time,
  'gold_layer' AS data_layer
FROM LIVE.{silver_table}
WHERE order_timestamp IS NOT NULL
GROUP BY 
  date(order_timestamp),
  hour(order_timestamp),
  id,
  name,
  category,
  product_category
'''

print("Executing Gold Layer SQL...")
spark.sql(gold_sql)
print("✅ Gold layer aggregation completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Complete
# MAGIC 
# MAGIC The gold layer is now ready with real-time analytics.
""",
        
        "create_dashboard": """
# Databricks notebook source
# MAGIC %md
# MAGIC # Create Analytics Dashboard
# MAGIC 
# MAGIC This notebook creates and updates the analytics dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC - gold_table: Source gold table
# MAGIC - dashboard_name: Name of the dashboard

# COMMAND ----------

dbutils.widgets.text("gold_table", "retail_analytics")
dbutils.widgets.text("dashboard_name", "Retail Analytics Dashboard")

gold_table = dbutils.widgets.get("gold_table")
dashboard_name = dbutils.widgets.get("dashboard_name")

print(f"Gold table: {gold_table}")
print(f"Dashboard name: {dashboard_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Analytics Data

# COMMAND ----------

# Query the analytics data
analytics_df = spark.sql(f"SELECT * FROM {gold_table} LIMIT 100")
display(analytics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Visualizations

# COMMAND ----------

# Revenue by category
revenue_by_category = spark.sql(f'''
SELECT 
  category,
  SUM(total_revenue) as total_revenue,
  COUNT(*) as product_count
FROM {gold_table}
GROUP BY category
ORDER BY total_revenue DESC
''')

display(revenue_by_category)

# COMMAND ----------

# Product category distribution
category_distribution = spark.sql(f'''
SELECT 
  product_category,
  COUNT(*) as product_count,
  SUM(total_revenue) as total_revenue
FROM {gold_table}
GROUP BY product_category
ORDER BY product_count DESC
''')

display(category_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Complete
# MAGIC 
# MAGIC The analytics dashboard has been created with visualizations.
# MAGIC 
# MAGIC **Key Metrics:**
# MAGIC - Total Revenue by Category
# MAGIC - Product Distribution
# MAGIC - Quality Metrics
# MAGIC - Real-time Analytics
"""
    }
    
    return notebooks

def save_job_config():
    """Save the job configuration to a file."""
    job_config = create_dlt_pipeline_job()
    
    # Create jobs directory if it doesn't exist
    jobs_dir = Path(__file__).parent
    jobs_dir.mkdir(exist_ok=True)
    
    # Save job config
    config_file = jobs_dir / "dlt_pipeline_job.json"
    with open(config_file, 'w') as f:
        json.dump(job_config, f, indent=2)
    
    print(f"✅ Job configuration saved to: {config_file}")
    
    # Save notebook files
    notebooks = create_job_notebooks()
    notebooks_dir = jobs_dir / "notebooks"
    notebooks_dir.mkdir(exist_ok=True)
    
    for name, content in notebooks.items():
        notebook_file = notebooks_dir / f"{name}.py"
        with open(notebook_file, 'w') as f:
            f.write(content)
        print(f"✅ Notebook saved: {notebook_file}")
    
    return config_file

if __name__ == "__main__":
    print("=" * 60)
    print("🔧 CREATING DATABRICKS JOB")
    print("=" * 60)
    
    config_file = save_job_config()
    
    print("\n📋 Job Configuration Summary:")
    print("  - Job Name: DLT-Pipeline-Retail-Analytics")
    print("  - Tasks: 5 (Setup → Bronze → Silver → Gold → Dashboard)")
    print("  - Cluster: Standard_DS3_v2 with 2 workers")
    print("  - Features: Auto Loader, Streaming Tables, Materialized Views")
    
    print("\n🚀 To deploy this job:")
    print("  1. Upload the notebooks to your Databricks workspace")
    print("  2. Use the Databricks CLI or REST API to create the job")
    print("  3. Monitor the job runs in the Databricks Jobs UI")
    
    print("\n📊 The job will create:")
    print("  - Bronze table with Auto Loader")
    print("  - Silver table with data quality")
    print("  - Gold table with real-time analytics")
    print("  - Interactive dashboard with visualizations")
    
    print("=" * 60) 