"""
dlt/expectations.py — Delta Live Tables Expectation Builders.

Module   : dlt.expectations
Concept  : DLT data quality constraints that control what happens when rows fail
When     : Any DLT table that needs data quality enforcement.

DLT Expectation Modes
---------------------
@dlt.expect("name", "condition")
    Records violation metrics but KEEPS all rows (including failures).
    Use for monitoring / alerting without dropping data.
    Failure rate visible in DLT pipeline UI and event log.

@dlt.expect_or_drop("name", "condition")
    Rows violating the condition are DROPPED silently.
    Equivalent to WHERE condition in the SQL.
    Use when bad rows are unrecoverable and downstream tables must be clean.

@dlt.expect_or_fail("name", "condition")
    The pipeline UPDATE FAILS if any row violates the condition.
    Use when data quality is a hard contract — no bad data may proceed.
    Be careful: a single bad row stops the entire pipeline.

@dlt.expect_all(expectations_dict)
    Apply multiple expect() constraints at once (monitor only).

@dlt.expect_all_or_drop(expectations_dict)
    Apply multiple expect_or_drop() constraints at once.

@dlt.expect_all_or_fail(expectations_dict)
    Apply multiple expect_or_fail() constraints at once.

Choosing a mode
---------------
Bronze   → expect()          Raw data lands as-is; violations are counted but kept.
Silver   → expect_or_drop()  Cleanse layer: drop records that fail business rules.
Gold     → expect_or_fail()  Hard contract: if Gold is dirty, fail loudly.

Public API
----------
build_expect(name, condition) -> dict
build_expect_or_drop(name, condition) -> dict
build_expect_or_fail(name, condition) -> dict
build_expect_all(expectations) -> dict
build_not_null_expectation(col) -> dict
build_range_expectation(col, min_val, max_val) -> dict
build_accepted_values_expectation(col, values) -> dict
build_regex_expectation(col, pattern) -> dict
expectations_to_decorator_args(expectations, mode) -> dict
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


VALID_MODES = {"expect", "expect_or_drop", "expect_or_fail"}


def build_expect(name: str, condition: str) -> dict:
    """
    Build a monitor-only DLT expectation.

    Parameters
    ----------
    name : str
        Human-readable constraint name shown in the DLT UI.
    condition : str
        SQL boolean expression.  Rows where condition is FALSE are violations.
        Example: "customer_id IS NOT NULL"

    Returns
    -------
    dict
        {"name": str, "condition": str, "mode": "expect"}
    """
    return {"name": name, "condition": condition, "mode": "expect"}


def build_expect_or_drop(name: str, condition: str) -> dict:
    """
    Build a drop-on-fail DLT expectation.

    Parameters
    ----------
    name : str
    condition : str

    Returns
    -------
    dict
        {"name": str, "condition": str, "mode": "expect_or_drop"}
    """
    return {"name": name, "condition": condition, "mode": "expect_or_drop"}


def build_expect_or_fail(name: str, condition: str) -> dict:
    """
    Build a fail-on-violation DLT expectation.

    Parameters
    ----------
    name : str
    condition : str

    Returns
    -------
    dict
        {"name": str, "condition": str, "mode": "expect_or_fail"}
    """
    return {"name": name, "condition": condition, "mode": "expect_or_fail"}


def build_expect_all(expectations: Dict[str, str]) -> dict:
    """
    Build an expect_all config dict (monitor-only, multiple constraints).

    Parameters
    ----------
    expectations : dict
        {constraint_name: sql_condition} mapping.

    Returns
    -------
    dict
        {"expectations": {name: condition}, "mode": "expect_all"}

    Raises
    ------
    ValueError
        If expectations is empty.
    """
    if not expectations:
        raise ValueError("expectations must not be empty")
    return {"expectations": dict(expectations), "mode": "expect_all"}


def build_not_null_expectation(
    col: str,
    mode: str = "expect_or_drop",
) -> dict:
    """
    Build a not-null constraint expectation for a column.

    Parameters
    ----------
    col : str
    mode : str
        'expect', 'expect_or_drop', or 'expect_or_fail'.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If mode is not valid.
    """
    _validate_mode(mode)
    return {
        "name": f"{col}_not_null",
        "condition": f"{col} IS NOT NULL",
        "mode": mode,
    }


def build_range_expectation(
    col: str,
    min_val: Optional[Any] = None,
    max_val: Optional[Any] = None,
    mode: str = "expect_or_drop",
) -> dict:
    """
    Build a range constraint expectation for a numeric column.

    Parameters
    ----------
    col : str
    min_val : numeric | None
    max_val : numeric | None
    mode : str

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If neither min_val nor max_val is provided, or mode is invalid.
    """
    _validate_mode(mode)
    conditions = []
    if min_val is not None:
        conditions.append(f"{col} >= {min_val}")
    if max_val is not None:
        conditions.append(f"{col} <= {max_val}")
    if not conditions:
        raise ValueError("At least one of min_val or max_val must be provided")
    condition = " AND ".join(conditions)
    bounds = f"[{min_val}, {max_val}]"
    return {
        "name": f"{col}_in_range_{min_val}_{max_val}",
        "condition": condition,
        "mode": mode,
    }


def build_accepted_values_expectation(
    col: str,
    values: List[Any],
    mode: str = "expect_or_drop",
) -> dict:
    """
    Build an accepted-values constraint expectation.

    Parameters
    ----------
    col : str
    values : list
    mode : str

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If values is empty.
    """
    _validate_mode(mode)
    if not values:
        raise ValueError("values must not be empty")
    quoted = ", ".join(f"'{v}'" for v in values)
    return {
        "name": f"{col}_accepted_values",
        "condition": f"{col} IN ({quoted})",
        "mode": mode,
    }


def build_regex_expectation(
    col: str,
    pattern: str,
    mode: str = "expect_or_drop",
) -> dict:
    """
    Build a regex-match constraint expectation.

    Parameters
    ----------
    col : str
    pattern : str
        Java-compatible regex.
    mode : str

    Returns
    -------
    dict
    """
    _validate_mode(mode)
    return {
        "name": f"{col}_matches_pattern",
        "condition": f"regexp_like({col}, '{pattern}')",
        "mode": mode,
    }


def expectations_to_decorator_args(
    expectations: List[dict],
    mode: Optional[str] = None,
) -> dict:
    """
    Convert a list of expectation dicts to @dlt.expect_all / expect_all_or_drop
    / expect_all_or_fail argument dict.

    When mode is None, each expectation's own mode is used (returns a grouped
    dict that the caller must split by mode before applying decorators).

    Parameters
    ----------
    expectations : list[dict]
        From build_expect(), build_not_null_expectation(), etc.
    mode : str | None
        Override mode for all expectations.

    Returns
    -------
    dict
        {constraint_name: condition} mapping suitable for dlt.expect_all(…).
    """
    return {e["name"]: e["condition"] for e in expectations}


def _validate_mode(mode: str) -> None:
    if mode not in VALID_MODES:
        raise ValueError(f"mode must be one of {VALID_MODES}, got '{mode}'")
