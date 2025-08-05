
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
