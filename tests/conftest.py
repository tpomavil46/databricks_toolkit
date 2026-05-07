"""
tests/conftest.py — Shared pytest fixtures for local unit tests.

SparkSession strategy
---------------------
databricks-connect >= 13 patches pyspark so that SparkSession.builder.getOrCreate()
blocks local mode and requires a Databricks cluster.  To work around this while
keeping tests runnable without a live cluster, the fixture below tries two paths:

1. Spark Connect "local" mode (Spark 3.4+, no cluster required).
   Uses pyspark.sql.connect.session.SparkSession, which is a different class
   from the patched pyspark.sql.session.SparkSession and is not blocked.
   This starts a local Spark Connect server in-process.

2. Databricks Connect (requires DATABRICKS_HOST env var + valid credentials).
   Falls back to DatabricksSession.builder when local Connect is unavailable.

If neither works, PySpark transformation tests are skipped with a clear message.
SQL generation and config-validation tests always pass — they have no spark fixture.

Delta Lake write operations (apply_scd*, DeltaTable.merge, saveAsTable) require
a Delta-capable environment and are not unit-tested here.  See tests/integration/.
"""

import os

import pytest


def _make_spark():
    """Return (session, error_str).  session is None when unavailable."""
    # Path 1: local Spark Connect (no cluster needed, Spark 3.4+)
    try:
        from pyspark.sql.connect.session import SparkSession as ConnectSession  # noqa: PLC0415
        session = (
            ConnectSession.builder
            .remote("local")
            .appName("databricks_toolkit_unit_tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        return session, None
    except Exception as e1:
        pass

    # Path 2: Databricks Connect (needs credentials)
    if os.getenv("DATABRICKS_HOST"):
        try:
            from databricks.connect import DatabricksSession  # noqa: PLC0415
            session = DatabricksSession.builder.getOrCreate()
            return session, None
        except Exception as e2:
            return None, f"Databricks Connect failed: {e2}"

    return None, (
        "SparkSession not available locally.  "
        "Set DATABRICKS_HOST + DATABRICKS_TOKEN to run against a Databricks cluster, "
        "or install a standalone spark-connect server."
    )


_SPARK, _SPARK_ERROR = _make_spark()


@pytest.fixture(scope="session")
def spark():
    if _SPARK is None:
        pytest.skip(_SPARK_ERROR)
    yield _SPARK
