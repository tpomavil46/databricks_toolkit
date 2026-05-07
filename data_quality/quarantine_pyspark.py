"""
data_quality/quarantine_pyspark.py — Quarantine Pattern (PySpark).

Module   : data_quality.quarantine_pyspark
Concept  : Route failing records to a quarantine table instead of dropping them
When     : Any ingestion or transformation pipeline where bad records should be
           isolated for later review rather than failing the entire pipeline.

What it is
----------
The quarantine pattern separates incoming data into two streams:
  - Good records  → written to the target Delta table as normal.
  - Bad records   → written to a quarantine Delta table with a failure reason.

Quarantining is always preferable to dropping bad records or failing the pipeline:
1. Dropped records are unrecoverable without re-running the source.
2. Pipeline failures block ALL records including valid ones.
3. Quarantine tables provide an audit trail of data quality issues.

Design
------
Each function tags rows with a _dq_fail_reason column (string):
  NULL     → row passes all checks (good record).
  non-null → human-readable reason the row failed (bad record).

split_good_bad() then separates the tagged DataFrame into two DataFrames.
The _dq_fail_reason column is dropped from the good DataFrame before writing.

Multiple check composition
---------------------------
For multiple checks, use tag_with_all_failures() which evaluates all conditions
and returns the FIRST failing reason (or NULL if all pass).  The order of checks
in the list determines precedence.

Public API
----------
tag_null_failures(df, required_cols, fail_col) -> DataFrame
tag_range_failures(df, col, min_val, max_val, fail_col) -> DataFrame
tag_regex_failures(df, col, pattern, fail_col) -> DataFrame
tag_accepted_value_failures(df, col, accepted, fail_col) -> DataFrame
tag_with_all_failures(df, tag_fns, fail_col) -> DataFrame
split_good_bad(df, fail_col) -> (DataFrame, DataFrame)
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DQ_FAIL_COL = "_dq_fail_reason"


def tag_null_failures(
    df: DataFrame,
    required_cols: List[str],
    fail_col: str = DQ_FAIL_COL,
) -> DataFrame:
    """
    Tag rows that have a NULL in any of the required columns.

    Parameters
    ----------
    df : DataFrame
    required_cols : list[str]
        Columns that must be non-null.  A row fails if any is null.
    fail_col : str
        Name of the failure reason column to add.

    Returns
    -------
    DataFrame
        Input DataFrame with fail_col appended.
        fail_col = NULL means the row passed; non-null = failure reason string.

    Examples
    --------
        df_tagged = tag_null_failures(df, ["customer_id", "email"])
        df_good, df_bad = split_good_bad(df_tagged)
    """
    null_condition = F.lit(False)
    for col in required_cols:
        null_condition = null_condition | F.col(col).isNull()

    null_reason = "NULL in required column(s): " + ", ".join(required_cols)

    return df.withColumn(
        fail_col,
        F.when(null_condition, F.lit(null_reason)).otherwise(F.lit(None).cast("string")),
    )


def tag_range_failures(
    df: DataFrame,
    col: str,
    min_val: Optional[Any] = None,
    max_val: Optional[Any] = None,
    fail_col: str = DQ_FAIL_COL,
) -> DataFrame:
    """
    Tag rows where a numeric column is outside [min_val, max_val].

    Parameters
    ----------
    df : DataFrame
    col : str
    min_val : numeric | None
    max_val : numeric | None
    fail_col : str

    Returns
    -------
    DataFrame
    """
    out_of_range = F.lit(False)
    if min_val is not None:
        out_of_range = out_of_range | (F.col(col) < F.lit(min_val))
    if max_val is not None:
        out_of_range = out_of_range | (F.col(col) > F.lit(max_val))

    bounds = f"[{min_val}, {max_val}]"
    reason = F.concat(
        F.lit(f"'{col}' value "),
        F.col(col).cast("string"),
        F.lit(f" outside {bounds}"),
    )

    return df.withColumn(
        fail_col,
        F.when(out_of_range, reason).otherwise(F.lit(None).cast("string")),
    )


def tag_regex_failures(
    df: DataFrame,
    col: str,
    pattern: str,
    fail_col: str = DQ_FAIL_COL,
) -> DataFrame:
    """
    Tag rows where a string column does not match the expected regex pattern.

    Null values are not tagged as failures by this function — combine with
    tag_null_failures() to enforce both completeness and format.

    Parameters
    ----------
    df : DataFrame
    col : str
    pattern : str
        Java-compatible regular expression.
    fail_col : str

    Returns
    -------
    DataFrame
    """
    fails_regex = F.col(col).isNotNull() & ~F.col(col).rlike(pattern)
    reason = F.concat(
        F.lit(f"'{col}' value '"),
        F.col(col).cast("string"),
        F.lit(f"' does not match pattern '{pattern}'"),
    )

    return df.withColumn(
        fail_col,
        F.when(fails_regex, reason).otherwise(F.lit(None).cast("string")),
    )


def tag_accepted_value_failures(
    df: DataFrame,
    col: str,
    accepted: List[Any],
    fail_col: str = DQ_FAIL_COL,
) -> DataFrame:
    """
    Tag rows where a column's value is not in the accepted set.

    Parameters
    ----------
    df : DataFrame
    col : str
    accepted : list
    fail_col : str

    Returns
    -------
    DataFrame
    """
    not_accepted = F.col(col).isNotNull() & ~F.col(col).isin(accepted)
    accepted_str = str(accepted)
    reason = F.concat(
        F.lit(f"'{col}' value '"),
        F.col(col).cast("string"),
        F.lit(f"' not in accepted set {accepted_str}"),
    )

    return df.withColumn(
        fail_col,
        F.when(not_accepted, reason).otherwise(F.lit(None).cast("string")),
    )


def tag_with_all_failures(
    df: DataFrame,
    tag_fns: List[Callable[[DataFrame], DataFrame]],
    fail_col: str = DQ_FAIL_COL,
) -> DataFrame:
    """
    Apply multiple tagging functions in sequence, keeping the FIRST failure reason.

    Each tagging function must use the same fail_col name.  The first function
    that tags a row as failing wins — subsequent functions do not overwrite an
    existing failure reason.

    Parameters
    ----------
    df : DataFrame
    tag_fns : list[callable]
        Each is (DataFrame) -> DataFrame.  Use lambdas or functools.partial to
        pre-bind column names / parameters.
    fail_col : str

    Returns
    -------
    DataFrame

    Examples
    --------
        df_tagged = tag_with_all_failures(df, [
            lambda df: tag_null_failures(df, ["customer_id", "email"]),
            lambda df: tag_range_failures(df, "age", 0, 120),
            lambda df: tag_regex_failures(df, "email", r".+@.+\\..+"),
        ])
        df_good, df_bad = split_good_bad(df_tagged)
    """
    TEMP_COL = "__dq_temp_reason__"
    result = df

    for fn in tag_fns:
        tagged = fn(result.drop(TEMP_COL) if TEMP_COL in result.columns else result)
        tagged = tagged.withColumnRenamed(fail_col, TEMP_COL)

        if fail_col not in result.columns:
            result = tagged.withColumnRenamed(TEMP_COL, fail_col)
        else:
            result = tagged.withColumn(
                fail_col,
                F.coalesce(F.col(fail_col), F.col(TEMP_COL)),
            ).drop(TEMP_COL)

    if fail_col not in result.columns:
        result = result.withColumn(fail_col, F.lit(None).cast("string"))

    return result


def split_good_bad(
    df: DataFrame,
    fail_col: str = DQ_FAIL_COL,
) -> Tuple[DataFrame, DataFrame]:
    """
    Split a tagged DataFrame into passing and quarantine DataFrames.

    Parameters
    ----------
    df : DataFrame
        Must have fail_col column (from any tag_* function).
    fail_col : str

    Returns
    -------
    (df_good, df_bad)
        df_good : rows where fail_col IS NULL, with fail_col dropped.
        df_bad  : rows where fail_col IS NOT NULL, with fail_col retained.
    """
    df_good = df.filter(F.col(fail_col).isNull()).drop(fail_col)
    df_bad = df.filter(F.col(fail_col).isNotNull())
    return df_good, df_bad
