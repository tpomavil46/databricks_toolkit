from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Perform a simple action
print(spark.range(100).collect())
