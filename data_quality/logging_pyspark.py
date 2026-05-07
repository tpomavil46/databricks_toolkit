"""
data_quality/logging_pyspark.py — DQ Result Logging to Delta (PySpark).

Module   : data_quality.logging_pyspark
Concept  : Persist DQ check results to a Delta audit table for trend monitoring
When     : After running DQ checks on any pipeline batch, regardless of whether
           the checks passed or failed.

What it is
----------
DQ logging writes one row per check result to a Delta table that accumulates
results across all pipeline runs.  This enables:
  - Trend analysis: is data quality improving or degrading over time?
  - Alerting: trigger notifications when failure_rate exceeds a threshold.
  - Audit trail: prove that DQ checks ran and what they found.
  - Dashboard: Grafana/Databricks SQL dashboard showing DQ KPIs by table/layer.

DQ log table schema
-------------------
run_ts          TIMESTAMP    When the check ran (UTC).
table_name      STRING       Table validated.
layer           STRING       Medallion layer (bronze/silver/gold).
pipeline_name   STRING       Pipeline identifier.
check_name      STRING       Check type (not_null, unique, value_range, ...).
column_name     STRING       Column checked (empty for table-level checks).
passed          BOOLEAN      Whether the check passed.
failing_count   BIGINT       Number of failing rows.
total_count     BIGINT       Total rows evaluated.
failure_rate    DOUBLE       failing_count / total_count.
details         STRING       Human-readable failure description.
env             STRING       Deployment environment.

Create the table with:
    CREATE TABLE IF NOT EXISTS <dq_log_table> (
        <schema above>
    ) USING DELTA
    PARTITIONED BY (layer, DATE(run_ts))
    TBLPROPERTIES (delta.enableChangeDataFeed = true);

Public API
----------
build_dq_log_schema() -> StructType
log_dq_results(spark, results, config, run_ts) -> None
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_quality.config import DQConfig, DQResult


def build_dq_log_schema() -> StructType:
    """
    Return the StructType schema for the DQ log Delta table.

    Returns
    -------
    StructType
    """
    return StructType([
        StructField("run_ts", TimestampType(), False),
        StructField("table_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("pipeline_name", StringType(), True),
        StructField("check_name", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("passed", BooleanType(), False),
        StructField("failing_count", LongType(), False),
        StructField("total_count", LongType(), False),
        StructField("failure_rate", DoubleType(), False),
        StructField("details", StringType(), True),
        StructField("env", StringType(), True),
    ])


def build_create_dq_log_table_sql(
    dq_log_table: str,
    partitioned: bool = True,
) -> str:
    """
    Generate CREATE TABLE IF NOT EXISTS SQL for the DQ log table.

    Parameters
    ----------
    dq_log_table : str
        Fully-qualified Delta table name.
    partitioned : bool
        When True, partitions by layer and run date for query efficiency.

    Returns
    -------
    str
    """
    partition_clause = (
        "\nPARTITIONED BY (layer, DATE(run_ts))" if partitioned else ""
    )
    return f"""CREATE TABLE IF NOT EXISTS {dq_log_table} (
  run_ts          TIMESTAMP NOT NULL,
  table_name      STRING NOT NULL,
  layer           STRING NOT NULL,
  pipeline_name   STRING,
  check_name      STRING NOT NULL,
  column_name     STRING,
  passed          BOOLEAN NOT NULL,
  failing_count   BIGINT NOT NULL,
  total_count     BIGINT NOT NULL,
  failure_rate    DOUBLE NOT NULL,
  details         STRING,
  env             STRING
) USING DELTA{partition_clause}
TBLPROPERTIES (delta.enableChangeDataFeed = true)"""


def results_to_rows(
    results: List[DQResult],
    config: DQConfig,
    run_ts: Optional[datetime] = None,
) -> List[dict]:
    """
    Convert a list of DQResults to row dicts for Delta logging.

    Pure function — no Spark required.

    Parameters
    ----------
    results : list[DQResult]
    config : DQConfig
    run_ts : datetime | None
        Timestamp for all rows in this batch.  Defaults to now (UTC).

    Returns
    -------
    list[dict]
    """
    ts = run_ts or datetime.now(timezone.utc)
    return [r.to_dict(config, ts) for r in results]


def log_dq_results(
    spark: SparkSession,
    results: List[DQResult],
    config: DQConfig,
    run_ts: Optional[datetime] = None,
) -> None:
    """
    Append DQ check results to the configured Delta log table.

    Creates a DataFrame from the results and appends it to dq_log_table.
    The log table must already exist (create with build_create_dq_log_table_sql).

    Parameters
    ----------
    spark : SparkSession
    results : list[DQResult]
    config : DQConfig
        dq_log_table must be set.  If empty, this function is a no-op.
    run_ts : datetime | None

    Returns
    -------
    None
    """
    if not config.dq_log_table:
        return

    rows = results_to_rows(results, config, run_ts)
    schema = build_dq_log_schema()
    df = spark.createDataFrame(rows, schema=schema)
    df.write.format("delta").mode("append").saveAsTable(config.dq_log_table)
