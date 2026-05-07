"""
data_quality/checks_pyspark.py — DataFrame DQ Checks (PySpark).

Module   : data_quality.checks_pyspark
Concept  : Run data quality assertions on a DataFrame and collect structured results
When     : Before writing to a target table, or as part of a DLT expectation layer.

What it is
----------
Each check function takes a DataFrame, evaluates one quality dimension, and returns
a DQResult.  Results are collected into a list and optionally:
  (a) logged to a Delta audit table (logging_pyspark.log_dq_results),
  (b) used to split passing/failing records (quarantine_pyspark),
  (c) used to halt the pipeline (assert_dq_results with fail_on_error=True).

Check taxonomy
--------------
Completeness    not_null — are required columns always populated?
Uniqueness      unique — are business keys truly unique?
Validity        value_range — numeric columns within expected bounds?
                regex_match — string columns match expected patterns?
                accepted_values — column values within a known set?
Consistency     referential_integrity — FK values exist in a reference table?
Timeliness      freshness — latest row is recent enough?
Volume          row_count — table has the expected number of rows?

Trade-offs: PySpark vs SQL checks
-----------------------------------
PySpark (this module): operates on an in-memory DataFrame.  Best for checks
  run before writing, on streaming batches, or inside foreachBatch.
  Requires a live SparkSession.

SQL (checks_sql.py): generates SELECT queries run against registered tables or
  temp views.  Best for scheduled DQ jobs that run post-write.  The SQL is
  auditable and replayable from query history.

Public API
----------
check_not_null(df, col) -> DQResult
check_unique(df, col_or_cols) -> DQResult
check_value_range(df, col, min_val, max_val) -> DQResult
check_regex(df, col, pattern) -> DQResult
check_accepted_values(df, col, accepted) -> DQResult
check_referential_integrity(df, col, ref_df, ref_col) -> DQResult
check_row_count(df, min_rows, max_rows) -> DQResult
check_freshness(df, ts_col, max_age_hours) -> DQResult
run_checks(df, check_fns) -> list[DQResult]
"""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, List, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from data_quality.config import DQResult


def check_not_null(df: DataFrame, col: str) -> DQResult:
    """
    Check that a column has no NULL values.

    Parameters
    ----------
    df : DataFrame
    col : str
        Column name to check.

    Returns
    -------
    DQResult
        passed=True when failing_count == 0.
    """
    total = df.count()
    failing = df.filter(F.col(col).isNull()).count()
    return DQResult(
        check_name="not_null",
        column=col,
        passed=failing == 0,
        failing_count=failing,
        total_count=total,
        details=f"{failing} null values in '{col}'" if failing > 0 else "",
    )


def check_unique(
    df: DataFrame,
    col_or_cols: Union[str, List[str]],
) -> DQResult:
    """
    Check that a column (or combination of columns) has no duplicate values.

    Parameters
    ----------
    df : DataFrame
    col_or_cols : str | list[str]
        Single column name or composite key column list.

    Returns
    -------
    DQResult
        failing_count = number of rows involved in duplicates.

    Notes
    -----
    A duplicate count of N means N rows share a key with at least one other row.
    The total number of distinct duplicate keys is lower.
    """
    cols = [col_or_cols] if isinstance(col_or_cols, str) else col_or_cols
    col_label = ", ".join(cols)

    total = df.count()
    duplicate_keys = (
        df.groupBy(*cols)
        .count()
        .filter(F.col("count") > 1)
    )
    failing = (
        df.join(duplicate_keys.select(*cols), on=cols, how="left_semi")
        .count()
    )
    return DQResult(
        check_name="unique",
        column=col_label,
        passed=failing == 0,
        failing_count=failing,
        total_count=total,
        details=f"{failing} rows with duplicate key ({col_label})" if failing > 0 else "",
    )


def check_value_range(
    df: DataFrame,
    col: str,
    min_val: Optional[Any] = None,
    max_val: Optional[Any] = None,
) -> DQResult:
    """
    Check that a numeric column's values fall within [min_val, max_val].

    Parameters
    ----------
    df : DataFrame
    col : str
    min_val : numeric | None
        Inclusive lower bound.  None = no lower bound check.
    max_val : numeric | None
        Inclusive upper bound.  None = no upper bound check.

    Returns
    -------
    DQResult
    """
    total = df.count()

    out_of_range = F.lit(False)
    if min_val is not None:
        out_of_range = out_of_range | (F.col(col) < F.lit(min_val))
    if max_val is not None:
        out_of_range = out_of_range | (F.col(col) > F.lit(max_val))

    failing = df.filter(out_of_range | F.col(col).isNull()).count() if (
        min_val is not None or max_val is not None
    ) else 0

    bounds = f"[{min_val}, {max_val}]"
    return DQResult(
        check_name="value_range",
        column=col,
        passed=failing == 0,
        failing_count=failing,
        total_count=total,
        details=f"{failing} values in '{col}' outside {bounds}" if failing > 0 else "",
    )


def check_regex(df: DataFrame, col: str, pattern: str) -> DQResult:
    """
    Check that non-null values in a column match a regular expression.

    Null values are excluded from the match check (they are not treated as
    failures by this check — use check_not_null separately for completeness).

    Parameters
    ----------
    df : DataFrame
    col : str
    pattern : str
        Java-compatible regular expression (Spark uses Java regex).
        Example: r'^[A-Z]{2}\\d{6}$'

    Returns
    -------
    DQResult
    """
    total = df.count()
    non_null = df.filter(F.col(col).isNotNull())
    failing = non_null.filter(~F.col(col).rlike(pattern)).count()
    return DQResult(
        check_name="regex_match",
        column=col,
        passed=failing == 0,
        failing_count=failing,
        total_count=total,
        details=f"{failing} values in '{col}' do not match pattern '{pattern}'" if failing > 0 else "",
    )


def check_accepted_values(
    df: DataFrame,
    col: str,
    accepted: List[Any],
) -> DQResult:
    """
    Check that all non-null values in a column belong to an accepted set.

    Parameters
    ----------
    df : DataFrame
    col : str
    accepted : list
        Set of acceptable values.

    Returns
    -------
    DQResult
    """
    total = df.count()
    failing = df.filter(
        F.col(col).isNotNull() & ~F.col(col).isin(accepted)
    ).count()
    return DQResult(
        check_name="accepted_values",
        column=col,
        passed=failing == 0,
        failing_count=failing,
        total_count=total,
        details=f"{failing} values in '{col}' not in accepted set" if failing > 0 else "",
    )


def check_referential_integrity(
    df: DataFrame,
    col: str,
    ref_df: DataFrame,
    ref_col: str,
) -> DQResult:
    """
    Check that all non-null values in col exist in ref_df.ref_col.

    Parameters
    ----------
    df : DataFrame
        Table to validate (e.g., fact table).
    col : str
        Foreign key column in df.
    ref_df : DataFrame
        Reference (dimension) table.
    ref_col : str
        Primary key column in ref_df.

    Returns
    -------
    DQResult
        failing_count = rows in df whose col value is absent from ref_df.ref_col.
    """
    total = df.count()
    ref_keys = ref_df.select(F.col(ref_col).alias(col)).distinct()
    failing = (
        df.filter(F.col(col).isNotNull())
        .join(ref_keys, on=col, how="left_anti")
        .count()
    )
    return DQResult(
        check_name="referential_integrity",
        column=col,
        passed=failing == 0,
        failing_count=failing,
        total_count=total,
        details=(
            f"{failing} values in '{col}' have no match in reference column '{ref_col}'"
            if failing > 0 else ""
        ),
    )


def check_row_count(
    df: DataFrame,
    min_rows: Optional[int] = None,
    max_rows: Optional[int] = None,
) -> DQResult:
    """
    Check that the DataFrame has an acceptable number of rows.

    Parameters
    ----------
    df : DataFrame
    min_rows : int | None
        Minimum acceptable row count.
    max_rows : int | None
        Maximum acceptable row count.

    Returns
    -------
    DQResult
        total_count = actual row count.  failing_count = 0 if in range, else 1.
    """
    total = df.count()
    failed = False
    reasons = []
    if min_rows is not None and total < min_rows:
        failed = True
        reasons.append(f"row count {total} < min {min_rows}")
    if max_rows is not None and total > max_rows:
        failed = True
        reasons.append(f"row count {total} > max {max_rows}")
    return DQResult(
        check_name="row_count",
        column="",
        passed=not failed,
        failing_count=1 if failed else 0,
        total_count=total,
        details="; ".join(reasons),
    )


def check_freshness(
    df: DataFrame,
    ts_col: str,
    max_age_hours: float,
) -> DQResult:
    """
    Check that the most recent row is not older than max_age_hours.

    Parameters
    ----------
    df : DataFrame
    ts_col : str
        Timestamp column to evaluate.
    max_age_hours : float
        Maximum acceptable age of the latest record in hours.

    Returns
    -------
    DQResult
        failing_count = 0 if fresh, 1 if stale.
    """
    total = df.count()
    if total == 0:
        return DQResult(
            check_name="freshness",
            column=ts_col,
            passed=False,
            failing_count=1,
            total_count=0,
            details="Table is empty — cannot assess freshness",
        )

    row = df.agg(F.max(F.col(ts_col)).alias("latest_ts")).collect()[0]
    latest_ts = row["latest_ts"]
    now = datetime.now(timezone.utc)

    if latest_ts is None:
        return DQResult(
            check_name="freshness",
            column=ts_col,
            passed=False,
            failing_count=1,
            total_count=total,
            details=f"Latest value in '{ts_col}' is NULL",
        )

    if latest_ts.tzinfo is None:
        latest_ts = latest_ts.replace(tzinfo=timezone.utc)
    age_hours = (now - latest_ts).total_seconds() / 3600
    passed = age_hours <= max_age_hours
    return DQResult(
        check_name="freshness",
        column=ts_col,
        passed=passed,
        failing_count=0 if passed else 1,
        total_count=total,
        details=(
            f"Latest '{ts_col}' is {age_hours:.1f}h old (max allowed: {max_age_hours}h)"
            if not passed else ""
        ),
    )


def run_checks(
    df: DataFrame,
    check_fns: List[Callable[[DataFrame], DQResult]],
) -> List[DQResult]:
    """
    Run a list of check functions against a DataFrame and collect results.

    Parameters
    ----------
    df : DataFrame
    check_fns : list[callable]
        Each callable takes a DataFrame and returns a DQResult.
        Use lambdas or functools.partial to pre-bind column names / params.

    Returns
    -------
    list[DQResult]

    Examples
    --------
        from functools import partial

        results = run_checks(df, [
            lambda df: check_not_null(df, "customer_id"),
            lambda df: check_unique(df, "customer_id"),
            lambda df: check_value_range(df, "age", 0, 120),
            lambda df: check_row_count(df, min_rows=1),
        ])
    """
    return [fn(df) for fn in check_fns]
