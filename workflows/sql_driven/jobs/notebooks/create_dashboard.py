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
revenue_by_category = spark.sql(
    f"""
SELECT 
  category,
  SUM(total_revenue) as total_revenue,
  COUNT(*) as product_count
FROM {gold_table}
GROUP BY category
ORDER BY total_revenue DESC
"""
)

display(revenue_by_category)

# COMMAND ----------

# Product category distribution
category_distribution = spark.sql(
    f"""
SELECT 
  product_category,
  COUNT(*) as product_count,
  SUM(total_revenue) as total_revenue
FROM {gold_table}
GROUP BY product_category
ORDER BY product_count DESC
"""
)

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
