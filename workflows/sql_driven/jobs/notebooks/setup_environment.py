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
    {
        "id": 1,
        "name": "Product A",
        "price": 10.99,
        "category": "electronics",
        "timestamp": "2024-01-01",
    },
    {
        "id": 2,
        "name": "Product B",
        "price": 25.50,
        "category": "clothing",
        "timestamp": "2024-01-02",
    },
    {
        "id": 3,
        "name": "Product C",
        "price": 5.99,
        "category": "books",
        "timestamp": "2024-01-03",
    },
    {
        "id": 4,
        "name": "Product D",
        "price": 150.00,
        "category": "electronics",
        "timestamp": "2024-01-04",
    },
    {
        "id": 5,
        "name": "Product E",
        "price": 75.25,
        "category": "clothing",
        "timestamp": "2024-01-05",
    },
]

df = spark.createDataFrame(test_data)
df.write.mode("overwrite").json("dbfs:/tmp/streaming_source/initial_batch")

print("✅ Created test data in dbfs:/tmp/streaming_source/initial_batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup Complete
# MAGIC
# MAGIC The environment is now ready for the DLT pipeline to process the data.
