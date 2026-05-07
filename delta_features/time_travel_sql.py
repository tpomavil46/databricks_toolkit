"""
delta_features/time_travel_sql.py — Delta Time Travel (Spark SQL).

Module   : delta_features.time_travel_sql
Concept  : Read historical Delta versions and restore tables using SQL syntax
When     : Same as time_travel_pyspark.  Prefer SQL for notebook workflows,
           ad-hoc audits in Databricks SQL, or when the statement must appear
           verbatim in query history.

Public API
----------
build_version_sql(table, version, select_cols) -> str
build_timestamp_sql(table, timestamp, select_cols) -> str
build_restore_to_version_sql(table, version) -> str
build_restore_to_timestamp_sql(table, timestamp) -> str
build_describe_history_sql(table, limit) -> str
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import SparkSession


def build_version_sql(
    table: str,
    version: int,
    select_cols: Optional[List[str]] = None,
) -> str:
    """
    Generate SELECT ... FROM <table> VERSION AS OF <version>.

    Parameters
    ----------
    table : str
        Fully-qualified Delta table name.
    version : int
        Historical version number to read.
    select_cols : list[str] | None
        Columns to select.  None = SELECT *.

    Returns
    -------
    str
        Time-travel SELECT SQL.

    Examples
    --------
        sql = build_version_sql("catalog.gold.dim_customer", version=5)
        # SELECT * FROM catalog.gold.dim_customer VERSION AS OF 5
    """
    cols = ", ".join(select_cols) if select_cols else "*"
    return f"SELECT {cols} FROM {table} VERSION AS OF {version}"


def build_timestamp_sql(
    table: str,
    timestamp: str,
    select_cols: Optional[List[str]] = None,
) -> str:
    """
    Generate SELECT ... FROM <table> TIMESTAMP AS OF '<timestamp>'.

    Parameters
    ----------
    table : str
    timestamp : str
        ISO 8601 timestamp or date string.  Quoted in the generated SQL.
    select_cols : list[str] | None

    Returns
    -------
    str

    Examples
    --------
        sql = build_timestamp_sql("catalog.gold.dim_customer", "2024-01-15")
        # SELECT * FROM catalog.gold.dim_customer TIMESTAMP AS OF '2024-01-15'
    """
    cols = ", ".join(select_cols) if select_cols else "*"
    return f"SELECT {cols} FROM {table} TIMESTAMP AS OF '{timestamp}'"


def build_restore_to_version_sql(table: str, version: int) -> str:
    """
    Generate RESTORE TABLE ... TO VERSION AS OF <version>.

    RESTORE rolls the table's current state back to a historical version.
    It adds a new transaction log entry — it does not delete history.
    The table version after restore is (current_version + 1).

    Parameters
    ----------
    table : str
    version : int
        Target version to restore to.

    Returns
    -------
    str

    Notes
    -----
    RESTORE requires the underlying data files to still exist (i.e., not
    vacuumed away).  Always verify with DESCRIBE HISTORY before restoring.
    RESTORE TABLE requires Delta Lake 0.8+ / DBR 7.4+.
    """
    return f"RESTORE TABLE {table} TO VERSION AS OF {version}"


def build_restore_to_timestamp_sql(table: str, timestamp: str) -> str:
    """
    Generate RESTORE TABLE ... TO TIMESTAMP AS OF '<timestamp>'.

    Parameters
    ----------
    table : str
    timestamp : str

    Returns
    -------
    str
    """
    return f"RESTORE TABLE {table} TO TIMESTAMP AS OF '{timestamp}'"


def build_describe_history_sql(
    table: str,
    limit: Optional[int] = None,
) -> str:
    """
    Generate DESCRIBE HISTORY <table> to inspect the transaction log.

    DESCRIBE HISTORY returns one row per committed transaction with:
    version, timestamp, operation (WRITE, MERGE, DELETE, OPTIMIZE, VACUUM, ...),
    operationParameters, operationMetrics, userName, clusterId, and more.

    Parameters
    ----------
    table : str
    limit : int | None
        Restrict output to the N most recent versions.  None = all history.

    Returns
    -------
    str

    Examples
    --------
        sql = build_describe_history_sql("catalog.gold.dim_customer", limit=10)
        # DESCRIBE HISTORY catalog.gold.dim_customer LIMIT 10
    """
    sql = f"DESCRIBE HISTORY {table}"
    if limit is not None:
        sql += f" LIMIT {limit}"
    return sql


def build_diff_versions_sql(
    table: str,
    version_a: int,
    version_b: int,
    key_cols: List[str],
) -> str:
    """
    Generate SQL that returns rows changed between two versions.

    Uses an anti-join on the key columns with an except-based approach:
    selects rows from version_b that have no identical row in version_a.

    Parameters
    ----------
    table : str
    version_a : int
        Baseline version.
    version_b : int
        Comparison version.
    key_cols : list[str]
        Business key columns used to align rows.

    Returns
    -------
    str
        SQL using EXCEPT to identify changed rows.
    """
    join_cond = " AND ".join(f"a.{k} = b.{k}" for k in key_cols)
    return f"""SELECT b.*
FROM {table} VERSION AS OF {version_b} AS b
LEFT ANTI JOIN {table} VERSION AS OF {version_a} AS a
  ON {join_cond}"""


def apply_time_travel_sql(spark: SparkSession, sql: str):
    """
    Execute a time-travel SQL statement and return the resulting DataFrame.

    Parameters
    ----------
    spark : SparkSession
    sql : str
        Any of the SQL strings produced by this module.

    Returns
    -------
    DataFrame
    """
    return spark.sql(sql)
