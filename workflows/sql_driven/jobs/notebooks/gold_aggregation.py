
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
