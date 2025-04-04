# utils/session.py
from databricks.connect import DatabricksSession

def get_spark():
    return DatabricksSession.builder.getOrCreate()