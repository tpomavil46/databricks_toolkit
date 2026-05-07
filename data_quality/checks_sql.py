"""
data_quality/checks_sql.py — SQL DQ Checks.

Module   : data_quality.checks_sql
Concept  : Generate SQL queries that compute DQ metrics against Delta tables
When     : Scheduled post-write DQ jobs that run against a registered table or
           temp view.  The SQL is auditable in Databricks query history.

SQL check output schema
-----------------------
Each generated SQL returns exactly one row with these columns:
    check_name    VARCHAR  — check identifier
    column_name   VARCHAR  — column checked (empty for table-level checks)
    total_count   BIGINT   — rows evaluated
    failing_count BIGINT   — rows that violated the check
    details       VARCHAR  — human-readable summary

Run via spark.sql(sql).collect()[0] and convert to DQResult.

Public API
----------
build_null_check_sql(table, col) -> str
build_uniqueness_check_sql(table, col_or_cols) -> str
build_range_check_sql(table, col, min_val, max_val) -> str
build_regex_check_sql(table, col, pattern) -> str
build_accepted_values_check_sql(table, col, accepted) -> str
build_row_count_check_sql(table, min_rows, max_rows) -> str
build_completeness_report_sql(table, cols) -> str
run_sql_check(spark, sql) -> DQResult
"""

from __future__ import annotations

from typing import Any, List, Optional, Union

from pyspark.sql import SparkSession

from data_quality.config import DQResult


def build_null_check_sql(table: str, col: str) -> str:
    """
    Generate SQL that counts NULL values in a column.

    Parameters
    ----------
    table : str
        Registered table name or temp view.
    col : str

    Returns
    -------
    str
        SELECT returning (check_name, column_name, total_count, failing_count, details).

    Examples
    --------
        sql = build_null_check_sql("catalog.gold.dim_customer", "customer_id")
        row = spark.sql(sql).collect()[0]
    """
    return f"""SELECT
  'not_null' AS check_name,
  '{col}' AS column_name,
  COUNT(*) AS total_count,
  SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS failing_count,
  CONCAT(
    CAST(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS STRING),
    ' null values in {col}'
  ) AS details
FROM {table}"""


def build_uniqueness_check_sql(
    table: str,
    col_or_cols: Union[str, List[str]],
) -> str:
    """
    Generate SQL that counts rows involved in duplicate key violations.

    Parameters
    ----------
    table : str
    col_or_cols : str | list[str]

    Returns
    -------
    str
    """
    cols = [col_or_cols] if isinstance(col_or_cols, str) else col_or_cols
    col_list = ", ".join(cols)
    col_label = col_list

    return f"""SELECT
  'unique' AS check_name,
  '{col_label}' AS column_name,
  COUNT(*) AS total_count,
  COUNT(*) - COUNT(DISTINCT {col_list}) AS failing_count,
  CONCAT(
    CAST(COUNT(*) - COUNT(DISTINCT {col_list}) AS STRING),
    ' duplicate rows on ({col_label})'
  ) AS details
FROM {table}"""


def build_range_check_sql(
    table: str,
    col: str,
    min_val: Optional[Any] = None,
    max_val: Optional[Any] = None,
) -> str:
    """
    Generate SQL that counts values outside [min_val, max_val].

    Parameters
    ----------
    table : str
    col : str
    min_val : numeric | None
    max_val : numeric | None

    Returns
    -------
    str
    """
    conditions = []
    if min_val is not None:
        conditions.append(f"{col} < {min_val}")
    if max_val is not None:
        conditions.append(f"{col} > {max_val}")

    out_of_range = " OR ".join(conditions) if conditions else "FALSE"
    bounds = f"[{min_val}, {max_val}]"

    return f"""SELECT
  'value_range' AS check_name,
  '{col}' AS column_name,
  COUNT(*) AS total_count,
  SUM(CASE WHEN ({out_of_range}) THEN 1 ELSE 0 END) AS failing_count,
  CONCAT(
    CAST(SUM(CASE WHEN ({out_of_range}) THEN 1 ELSE 0 END) AS STRING),
    ' values outside {bounds} in {col}'
  ) AS details
FROM {table}"""


def build_regex_check_sql(
    table: str,
    col: str,
    pattern: str,
) -> str:
    """
    Generate SQL that counts non-null values not matching a regex pattern.

    Parameters
    ----------
    table : str
    col : str
    pattern : str
        Java-compatible regex.

    Returns
    -------
    str
    """
    escaped = pattern.replace("'", "\\'")
    return f"""SELECT
  'regex_match' AS check_name,
  '{col}' AS column_name,
  COUNT(*) AS total_count,
  SUM(CASE WHEN {col} IS NOT NULL AND NOT REGEXP_LIKE({col}, '{escaped}') THEN 1 ELSE 0 END) AS failing_count,
  CONCAT(
    CAST(SUM(CASE WHEN {col} IS NOT NULL AND NOT REGEXP_LIKE({col}, '{escaped}') THEN 1 ELSE 0 END) AS STRING),
    ' values in {col} not matching pattern'
  ) AS details
FROM {table}"""


def build_accepted_values_check_sql(
    table: str,
    col: str,
    accepted: List[Any],
) -> str:
    """
    Generate SQL that counts values not in the accepted set.

    Parameters
    ----------
    table : str
    col : str
    accepted : list
        Accepted values — quoted as strings in the IN clause.

    Returns
    -------
    str
    """
    quoted = ", ".join(f"'{v}'" for v in accepted)
    return f"""SELECT
  'accepted_values' AS check_name,
  '{col}' AS column_name,
  COUNT(*) AS total_count,
  SUM(CASE WHEN {col} IS NOT NULL AND {col} NOT IN ({quoted}) THEN 1 ELSE 0 END) AS failing_count,
  CONCAT(
    CAST(SUM(CASE WHEN {col} IS NOT NULL AND {col} NOT IN ({quoted}) THEN 1 ELSE 0 END) AS STRING),
    ' values in {col} not in accepted set'
  ) AS details
FROM {table}"""


def build_row_count_check_sql(
    table: str,
    min_rows: Optional[int] = None,
    max_rows: Optional[int] = None,
) -> str:
    """
    Generate SQL that checks whether the row count is within bounds.

    Returns failing_count = 0 when in bounds, 1 when out of bounds.

    Parameters
    ----------
    table : str
    min_rows : int | None
    max_rows : int | None

    Returns
    -------
    str
    """
    min_check = f"cnt < {min_rows}" if min_rows is not None else "FALSE"
    max_check = f"cnt > {max_rows}" if max_rows is not None else "FALSE"
    out_of_range = f"({min_check}) OR ({max_check})"
    bounds = f"[{min_rows}, {max_rows}]"

    return f"""WITH _cnt AS (SELECT COUNT(*) AS cnt FROM {table})
SELECT
  'row_count' AS check_name,
  '' AS column_name,
  cnt AS total_count,
  CASE WHEN {out_of_range} THEN 1 ELSE 0 END AS failing_count,
  CASE WHEN {out_of_range}
    THEN CONCAT('row count ', CAST(cnt AS STRING), ' outside {bounds}')
    ELSE ''
  END AS details
FROM _cnt"""


def build_completeness_report_sql(
    table: str,
    cols: List[str],
) -> str:
    """
    Generate SQL that reports the null rate for each column in a single query.

    Unlike the individual check functions (which return one row per check),
    this returns one row per column — useful for a quick completeness dashboard.

    Parameters
    ----------
    table : str
    cols : list[str]

    Returns
    -------
    str
        UNION ALL of null-count SELECTs, one per column.
    """
    unions = []
    for col in cols:
        unions.append(
            f"""SELECT '{col}' AS column_name,
  COUNT(*) AS total_count,
  SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count,
  ROUND(
    100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2
  ) AS null_pct
FROM {table}"""
        )
    return "\nUNION ALL\n".join(unions)


def run_sql_check(spark: SparkSession, sql: str) -> DQResult:
    """
    Execute a single DQ check SQL and return a DQResult.

    Parameters
    ----------
    spark : SparkSession
    sql : str
        Any SQL generated by this module's build_* functions.

    Returns
    -------
    DQResult
    """
    row = spark.sql(sql).collect()[0]
    failing = int(row["failing_count"])
    total = int(row["total_count"])
    return DQResult(
        check_name=str(row["check_name"]),
        column=str(row["column_name"]),
        passed=failing == 0,
        failing_count=failing,
        total_count=total,
        details=str(row["details"]) if row["details"] else "",
    )
