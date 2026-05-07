"""
optimization/bloom_filters_sql.py — Bloom Filter Indexes for Delta Tables.

Module   : optimization.bloom_filters_sql
Concept  : Probabilistic data-skipping for high-cardinality point lookups
When     : Tables that are frequently queried with equality filters on a column
           that has too many distinct values for min/max statistics to help.

What bloom filters are
----------------------
A bloom filter is a probabilistic data structure stored per Parquet file.  When
a query filters on a bloom-filter column (e.g., WHERE user_id = 'abc123'), Spark
can quickly determine whether the value MIGHT be in a file (read it) or
DEFINITELY is NOT (skip it) — without reading the file itself.

False positives are possible (the filter says "maybe" but the value isn't there).
False negatives are impossible (if the filter says "no", the value is definitely
not in that file).  The false-positive probability (fpp) controls the trade-off
between index size and skip effectiveness.

When bloom filters help
-----------------------
- High-cardinality columns (user_id, session_id, device_id, UUIDs).
- Equality filters (=, IN) — bloom filters do NOT help range queries (>, <).
- Tables where min/max stats can't prune files (too many distinct values per file).

When bloom filters do NOT help
-------------------------------
- Low-cardinality columns (status, country) — min/max stats already prune well.
- Range queries — bloom filters only answer "is this exact value here?".
- Very small tables — overhead exceeds benefit.
- Streaming writes with autoOptimize disabled — bloom filters are built by OPTIMIZE.

false_positive_probability (fpp)
---------------------------------
0.01 (1%) is a good default: 1% of file reads are false positives.
Lower fpp → larger index → less false positives → more storage overhead.
Higher fpp → smaller index → more false positives → less effective skipping.

Public API
----------
build_create_bloom_filter_sql(table, col, fpp, num_items) -> str
build_drop_bloom_filter_sql(table, col) -> str
build_set_bloom_filter_tblproperties_sql(table, col, fpp) -> str
build_bloom_filter_for_all_cols_sql(table, cols, fpp) -> list[str]
"""

from __future__ import annotations

from typing import List, Optional

DEFAULT_FPP = 0.01
DEFAULT_NUM_ITEMS = 1_000_000


def build_create_bloom_filter_sql(
    table: str,
    col: str,
    fpp: float = DEFAULT_FPP,
    num_items: int = DEFAULT_NUM_ITEMS,
) -> str:
    """
    Generate CREATE BLOOMFILTER INDEX SQL for a single column.

    The index is materialised on the next OPTIMIZE run, not immediately.

    Parameters
    ----------
    table : str
    col : str
        Column to index.  Must be a STRING, LONG, INT, or BINARY type.
    fpp : float
        False-positive probability.  Range: (0, 1).  Default 0.01 (1%).
    num_items : int
        Expected number of distinct values per file.  Affects index size.
        Set to ~cardinality / num_files.  Over-estimating is safe.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If fpp is not in (0, 1) or num_items <= 0.
    """
    if not 0 < fpp < 1:
        raise ValueError(f"fpp must be between 0 and 1 (exclusive), got {fpp}")
    if num_items <= 0:
        raise ValueError(f"num_items must be positive, got {num_items}")
    return (
        f"CREATE BLOOMFILTER INDEX ON TABLE {table}\n"
        f"FOR COLUMNS ({col} OPTIONS (fpp={fpp}, numItems={num_items}))"
    )


def build_drop_bloom_filter_sql(
    table: str,
    col: str,
) -> str:
    """
    Generate DROP BLOOMFILTER INDEX SQL for a single column.

    Parameters
    ----------
    table : str
    col : str

    Returns
    -------
    str
    """
    return f"DROP BLOOMFILTER INDEX ON TABLE {table}\nFOR COLUMNS ({col})"


def build_set_bloom_filter_tblproperties_sql(
    table: str,
    col: str,
    fpp: float = DEFAULT_FPP,
) -> str:
    """
    Set bloom filter via TBLPROPERTIES (alternative to CREATE BLOOMFILTER INDEX).

    Some Databricks versions configure bloom filters through table properties
    rather than the CREATE BLOOMFILTER INDEX DDL.  This generates the equivalent
    TBLPROPERTIES approach for compatibility.

    Parameters
    ----------
    table : str
    col : str
    fpp : float

    Returns
    -------
    str
    """
    if not 0 < fpp < 1:
        raise ValueError(f"fpp must be between 0 and 1 (exclusive), got {fpp}")
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES (\n"
        f"  'delta.bloomFilter.{col}.enabled' = 'true',\n"
        f"  'delta.bloomFilter.{col}.fpp' = '{fpp}',\n"
        f"  'delta.bloomFilter.{col}.numItems' = '{DEFAULT_NUM_ITEMS}'\n"
        f")"
    )


def build_bloom_filter_for_all_cols_sql(
    table: str,
    cols: List[str],
    fpp: float = DEFAULT_FPP,
    num_items: int = DEFAULT_NUM_ITEMS,
) -> List[str]:
    """
    Generate CREATE BLOOMFILTER INDEX SQL for each column in a list.

    Parameters
    ----------
    table : str
    cols : list[str]
    fpp : float
    num_items : int

    Returns
    -------
    list[str]
        One SQL string per column.

    Raises
    ------
    ValueError
        If cols is empty.
    """
    if not cols:
        raise ValueError("cols must not be empty")
    return [
        build_create_bloom_filter_sql(table, col, fpp, num_items)
        for col in cols
    ]
