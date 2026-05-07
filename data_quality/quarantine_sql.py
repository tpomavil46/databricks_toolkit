"""
data_quality/quarantine_sql.py — Quarantine Pattern (Spark SQL).

Module   : data_quality.quarantine_sql
Concept  : SQL-based quarantine: INSERT failing records and SELECT passing ones
When     : Post-write DQ enforcement via scheduled SQL jobs, or when the source
           is a registered table / temp view rather than a live DataFrame.

Public API
----------
build_quarantine_insert_sql(source, quarantine_table, fail_condition, fail_reason) -> str
build_good_records_sql(source, fail_condition, select_cols) -> str
build_null_quarantine_sql(source, quarantine_table, required_cols) -> str
build_range_quarantine_sql(source, quarantine_table, col, min_val, max_val) -> str
"""

from __future__ import annotations

from typing import Any, List, Optional, Union


def build_quarantine_insert_sql(
    source: str,
    quarantine_table: str,
    fail_condition: str,
    fail_reason: str,
    dq_fail_col: str = "_dq_fail_reason",
) -> str:
    """
    Generate INSERT INTO quarantine SQL for rows matching fail_condition.

    Parameters
    ----------
    source : str
        Registered table name or temp view to read from.
    quarantine_table : str
        Delta quarantine table to write failing rows to.
    fail_condition : str
        SQL boolean expression that evaluates to TRUE for failing rows.
        Example: "customer_id IS NULL OR age < 0"
    fail_reason : str
        Literal string to populate the _dq_fail_reason column.
    dq_fail_col : str
        Name of the failure reason column in the quarantine table.

    Returns
    -------
    str
        INSERT INTO ... SELECT SQL.

    Examples
    --------
        sql = build_quarantine_insert_sql(
            source="v_incoming",
            quarantine_table="catalog.dq.quarantine_bronze",
            fail_condition="customer_id IS NULL",
            fail_reason="null customer_id",
        )
    """
    return f"""INSERT INTO {quarantine_table}
SELECT *, '{fail_reason}' AS {dq_fail_col}
FROM {source}
WHERE {fail_condition}"""


def build_good_records_sql(
    source: str,
    fail_condition: str,
    select_cols: Optional[List[str]] = None,
) -> str:
    """
    Generate SELECT SQL for rows that do NOT match the fail_condition.

    Parameters
    ----------
    source : str
    fail_condition : str
        Same condition used in build_quarantine_insert_sql.
        Passing records are those where NOT (fail_condition).
    select_cols : list[str] | None
        Columns to select.  None = SELECT *.

    Returns
    -------
    str
    """
    cols = ", ".join(select_cols) if select_cols else "*"
    return f"""SELECT {cols}
FROM {source}
WHERE NOT ({fail_condition})"""


def build_null_quarantine_sql(
    source: str,
    quarantine_table: str,
    required_cols: List[str],
    dq_fail_col: str = "_dq_fail_reason",
) -> str:
    """
    Generate INSERT INTO quarantine SQL for rows with any NULL in required_cols.

    Parameters
    ----------
    source : str
    quarantine_table : str
    required_cols : list[str]
        Columns that must be non-null.

    Returns
    -------
    str
    """
    null_conditions = " OR ".join(f"{c} IS NULL" for c in required_cols)
    cols_str = ", ".join(required_cols)
    reason = f"NULL in required column(s): {cols_str}"
    return build_quarantine_insert_sql(
        source, quarantine_table, null_conditions, reason, dq_fail_col
    )


def build_range_quarantine_sql(
    source: str,
    quarantine_table: str,
    col: str,
    min_val: Optional[Any] = None,
    max_val: Optional[Any] = None,
    dq_fail_col: str = "_dq_fail_reason",
) -> str:
    """
    Generate INSERT INTO quarantine SQL for rows outside a numeric range.

    Parameters
    ----------
    source : str
    quarantine_table : str
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

    fail_condition = " OR ".join(conditions) if conditions else "FALSE"
    reason = f"'{col}' outside [{min_val}, {max_val}]"
    return build_quarantine_insert_sql(
        source, quarantine_table, fail_condition, reason, dq_fail_col
    )
