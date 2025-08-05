# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-3.1

# COMMAND ----------


def test_parameter_value():
    dbutils.widgets.text(name="test_value", defaultValue="")
    if dbutils.widgets.get("test_value") == "Succeed":
        return True
    return False


assert test_parameter_value() == True
