# utils/session.py

from databricks.connect import DatabricksSession  # noqa: F811
from utils.logger import log_function_call


class MyDatabricksSession:
    @staticmethod
    @log_function_call
    def get_spark():
        """
        Initialize and return the SparkSession using Databricks Connect
        """
        spark = (
            DatabricksSession.builder.profile("databricks")
            .clusterId("0401-190124-nho4m8l8")
            .getOrCreate()
        )

        print(
            spark.range(100).collect()
        )  # Get the Spark session using Databricks Connect
        return spark
