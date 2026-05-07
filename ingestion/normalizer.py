"""
ingestion/normalizer.py — Column name normalization and mapping.

Raw source data rarely arrives with the column names you want in your
lakehouse.  Vendors change field names between schema versions; different
source systems call the same concept 'cust_id', 'customer_id', or
'CUSTOMER_ID'.  This module provides tools to standardize column names
before data lands in Bronze.

Two complementary strategies
-----------------------------
1. Explicit mapping (apply_column_mapping)
   Supply a {source_name: target_name} dict.  Only the listed columns are
   renamed.  Use when you know the source schema and want precise control.
   Best for multiplex ingestion where each source has its own mapping.

2. Auto snake_case (to_snake_case)
   Converts all column names to lowercase snake_case regardless of origin.
   Use as a lightweight default when you don't have an explicit mapping and
   just want consistent naming conventions.

Functions
---------
apply_column_mapping(df, mapping, drop_unmapped) -> DataFrame
    Rename columns per mapping dict.  Optionally discard columns not in map.

to_snake_case(df) -> DataFrame
    Lowercase all column names, replace spaces and hyphens with underscores,
    strip leading/trailing underscores.

normalize(df, mapping, drop_unmapped, snake_case_fallback) -> DataFrame
    Convenience wrapper: applies mapping first, then optionally snake_case
    on any remaining columns not covered by the mapping.
"""

from __future__ import annotations

import re
from typing import Dict, Optional

from pyspark.sql import DataFrame


def apply_column_mapping(
    df: DataFrame,
    mapping: Dict[str, str],
    drop_unmapped: bool = False,
) -> DataFrame:
    """
    Rename columns according to a mapping dict.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    mapping : dict[str, str]
        {original_column_name: new_column_name}.  Names are matched
        case-insensitively against the DataFrame's actual column names.
    drop_unmapped : bool
        When True, any column NOT present in the mapping is dropped from
        the result.  When False (default), unmapped columns pass through
        unchanged.

    Returns
    -------
    DataFrame
        DataFrame with renamed (and optionally filtered) columns.

    Notes
    -----
    Matching is case-insensitive: a mapping key 'VendorID' matches an
    actual column 'vendorid' or 'VENDORID'.  The original casing of the
    column is used for the lookup; the value is used exactly as given.
    """
    if not mapping:
        return df

    # Build a case-insensitive lookup: lower(col_name) -> actual_col_name
    actual_cols = {c.lower(): c for c in df.columns}
    lower_mapping = {k.lower(): v for k, v in mapping.items()}

    renamed_actual: Dict[str, str] = {}  # actual_col -> target_name
    for lower_key, target in lower_mapping.items():
        if lower_key in actual_cols:
            renamed_actual[actual_cols[lower_key]] = target

    # Determine which actual columns to keep
    if drop_unmapped:
        keep = set(renamed_actual.keys())
        select_exprs = [
            df[col].alias(renamed_actual[col]) for col in df.columns if col in keep
        ]
        return df.select(select_exprs)

    # Keep all columns, rename mapped ones
    select_exprs = [
        df[col].alias(renamed_actual[col]) if col in renamed_actual else df[col]
        for col in df.columns
    ]
    return df.select(select_exprs)


def to_snake_case(df: DataFrame) -> DataFrame:
    """
    Convert all column names to lowercase snake_case.

    Transformation rules:
    - Lowercase everything
    - Replace spaces, hyphens, and dots with underscores
    - Collapse consecutive underscores to one
    - Strip leading/trailing underscores

    Example: 'Total Revenue ($)' → 'total_revenue_'  → 'total_revenue'
    """
    def _snake(name: str) -> str:
        s = name.lower()
        s = re.sub(r"[\s\-\.]+", "_", s)
        s = re.sub(r"[^\w]", "_", s)
        s = re.sub(r"_+", "_", s)
        return s.strip("_")

    select_exprs = [df[c].alias(_snake(c)) for c in df.columns]
    return df.select(select_exprs)


def normalize(
    df: DataFrame,
    mapping: Optional[Dict[str, str]] = None,
    drop_unmapped: bool = False,
    snake_case_fallback: bool = True,
) -> DataFrame:
    """
    Apply column mapping then optionally snake_case remaining columns.

    This is the recommended single entry point for column normalization.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    mapping : dict[str, str] | None
        Explicit rename map.  When None, only snake_case_fallback applies.
    drop_unmapped : bool
        Passed to apply_column_mapping.
    snake_case_fallback : bool
        When True, any column NOT covered by mapping is snake_cased.
        When False, unmapped columns are left as-is.

    Returns
    -------
    DataFrame
        Normalized DataFrame.

    Examples
    --------
    Full explicit mapping, no extra normalization:
        df = normalize(df, mapping={"VendorID": "vendor_id"}, snake_case_fallback=False)

    Explicit mapping + snake_case everything else:
        df = normalize(df, mapping={"tpep_pickup_datetime": "pickup_at"})

    Snake_case only (no explicit mapping):
        df = normalize(df, snake_case_fallback=True)
    """
    if mapping:
        df = apply_column_mapping(df, mapping, drop_unmapped=drop_unmapped)

    if snake_case_fallback:
        # Only snake_case columns that were NOT already renamed by the mapping
        mapped_targets = set(mapping.values()) if mapping else set()
        non_mapped = [c for c in df.columns if c not in mapped_targets]

        def _snake(name: str) -> str:
            s = name.lower()
            s = re.sub(r"[\s\-\.]+", "_", s)
            s = re.sub(r"[^\w]", "_", s)
            s = re.sub(r"_+", "_", s)
            return s.strip("_")

        select_exprs = [
            df[c].alias(_snake(c)) if c in non_mapped else df[c]
            for c in df.columns
        ]
        df = df.select(select_exprs)

    return df
