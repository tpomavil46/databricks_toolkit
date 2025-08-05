# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-3.1

# COMMAND ----------

dbutils.jobs.taskValues.set(key="bad_records", value=5)
print("Clean Data Task")
