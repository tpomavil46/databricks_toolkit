# utils/session.py

import os

from databricks.connect import DatabricksSession  # noqa: F811
from shared.utils.logger import log_function_call


class MyDatabricksSession:
    @staticmethod
    @log_function_call
    def get_spark():
        """
        Initialize and return the SparkSession using Databricks Connect.

        Auth priority (matches Databricks SDK unified auth):
        1. DATABRICKS_HOST + DATABRICKS_TOKEN env vars — used when set,
           bypasses ~/.databrickscfg entirely.  Set these in .env for local
           dev against a specific workspace.
        2. 'databricks' profile in ~/.databrickscfg — fallback when env
           vars are not set.

        Cluster is resolved from DATABRICKS_CLUSTER_ID env var; falls back
        to the profile's cluster_id when the env var is absent.
        """
        host = os.getenv("DATABRICKS_HOST")
        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")

        if host:
            # Env-var auth: DatabricksSession picks up DATABRICKS_HOST and
            # DATABRICKS_TOKEN automatically when no profile is specified.
            builder = DatabricksSession.builder
        else:
            builder = DatabricksSession.builder.profile("databricks")

        if cluster_id:
            builder = builder.clusterId(cluster_id)

        spark = builder.getOrCreate()
        print(spark.range(100).collect())
        return spark
