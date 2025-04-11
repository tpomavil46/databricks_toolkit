# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Management and Governance with Unity Catalog
# MAGIC
# MAGIC
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 1 | [Populating the Metastore]($./1 - Populating the Metastore) |
# MAGIC | 2 | [Navigating the Metastore]($./2L - Navigating the Metastore) |
# MAGIC | 3 | [Upgrading Tables to Unity Catalog]($./3 - Upgrading Tables to Unity Catalog) |
# MAGIC | 4 | [Controlling Access to Data]($./4 - Controlling Access to Data) |
# MAGIC | 5 | [Securing Data in Unity Catalog]($./5L - Securing Data in Unity Catalog) |
# MAGIC | 6 | [6 BONUS - Lakehouse Monitoring]($./6 BONUS - Lakehouse Monitoring) |
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **15.4.x-scala2.12**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>