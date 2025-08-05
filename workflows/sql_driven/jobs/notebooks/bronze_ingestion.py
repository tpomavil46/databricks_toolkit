
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
