"""
optimization/partitioning_sql.py — Partition Strategy and ZORDER SQL.

Module   : optimization.partitioning_sql
Concept  : Choose the right data layout so Spark skips as much data as possible
When     : Table creation, major schema changes, or when query profiling reveals
           full table scans on large Delta tables.

Decision guide: Hive partitions vs Liquid Clustering vs ZORDER
---------------------------------------------------------------
Hive partitions (PARTITIONED BY)
  Use when: One low-cardinality column drives >90% of queries (e.g., date, region).
  Avoid when: >10K partitions expected — each partition is a directory; too many
              causes driver OOM and slow listing.  The "small files" problem is
              worse per partition when write volume is low.

Liquid Clustering (CLUSTER BY — DBR 13.3+)
  Use when: Multiple query patterns, high-cardinality columns, or uncertain access.
  Benefit: No partition explosion, incremental re-clustering on write, supports
           up to 4 clustering columns.
  Limit: 1–4 columns only.  Still OPTIMIZE to materialise clustering.

ZORDER BY (inside OPTIMIZE)
  Use when: You have Hive partitions but also filter on a second high-cardinality
            column within each partition (e.g., PARTITION BY date, ZORDER BY user_id).
  Avoid when: Table is very small (<1 GB) — ZORDER overhead exceeds benefit.
  Note: ZORDER is per-partition; it does not cross partition boundaries.

Public API
----------
build_partition_by_sql(table, cols) -> str
build_liquid_clustering_alter_sql(table, cols) -> str
build_zorder_sql(table, cols, where_predicate) -> str
build_optimize_sql(table, where_predicate, zorder_cols) -> str
build_analyze_sql(table, cols) -> str
build_show_partitions_sql(table) -> str
build_describe_detail_sql(table) -> str
"""

from __future__ import annotations

from typing import List, Optional


def build_partition_by_sql(
    table: str,
    cols: List[str],
) -> str:
    """
    Generate ALTER TABLE ... REPLACE COLUMNS ... PARTITIONED BY SQL.

    Note: In Databricks, you typically declare partitioning at CREATE TABLE time.
    This generates the PARTITIONED BY clause for embedding in CREATE TABLE DDL.

    Parameters
    ----------
    table : str
    cols : list[str]
        Partition column names (low-cardinality, already in table schema).

    Returns
    -------
    str
        CREATE TABLE ... PARTITIONED BY (...) fragment.

    Raises
    ------
    ValueError
        If cols is empty.
    """
    if not cols:
        raise ValueError("cols must not be empty — at least one partition column required")
    col_list = ", ".join(cols)
    return f"-- Add to CREATE TABLE {table}:\nPARTITIONED BY ({col_list})"


def build_liquid_clustering_alter_sql(
    table: str,
    cols: List[str],
) -> str:
    """
    Generate ALTER TABLE ... CLUSTER BY SQL (Liquid Clustering).

    Converts an existing Delta table to Liquid Clustering.  Existing data is
    not immediately re-clustered — run OPTIMIZE afterward to materialise the
    new layout.

    Parameters
    ----------
    table : str
    cols : list[str]
        1–4 clustering columns.  Order matters: put the highest-selectivity
        column first (the one used in the most WHERE clauses).

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If cols is empty or has more than 4 elements.
    """
    if not cols:
        raise ValueError("cols must not be empty")
    if len(cols) > 4:
        raise ValueError(
            f"Liquid Clustering supports at most 4 columns, got {len(cols)}"
        )
    col_list = ", ".join(cols)
    return f"ALTER TABLE {table} CLUSTER BY ({col_list})"


def build_zorder_sql(
    table: str,
    cols: List[str],
    where_predicate: Optional[str] = None,
) -> str:
    """
    Generate OPTIMIZE ... ZORDER BY SQL.

    ZORDER co-locates values of the target columns within each file, so Spark
    can skip more files when those columns appear in a WHERE clause.

    Parameters
    ----------
    table : str
    cols : list[str]
        Columns to ZORDER by.  1–4 columns; effectiveness drops beyond 2.
    where_predicate : str | None
        Partition filter to scope OPTIMIZE to a single partition.
        Example: "date = '2024-06-01'" — limits rewrite to that partition only.
        Omit to OPTIMIZE the full table (expensive on large tables).

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If cols is empty.
    """
    if not cols:
        raise ValueError("cols must not be empty for ZORDER")
    where = f"\nWHERE {where_predicate}" if where_predicate else ""
    col_list = ", ".join(cols)
    return f"OPTIMIZE {table}{where}\nZORDER BY ({col_list})"


def build_optimize_sql(
    table: str,
    where_predicate: Optional[str] = None,
    zorder_cols: Optional[List[str]] = None,
) -> str:
    """
    Generate OPTIMIZE SQL, optionally scoped to a partition and with ZORDER.

    Parameters
    ----------
    table : str
    where_predicate : str | None
        Partition filter (see build_zorder_sql).
    zorder_cols : list[str] | None
        When provided, appends ZORDER BY clause.

    Returns
    -------
    str
    """
    where = f"\nWHERE {where_predicate}" if where_predicate else ""
    zorder = f"\nZORDER BY ({', '.join(zorder_cols)})" if zorder_cols else ""
    return f"OPTIMIZE {table}{where}{zorder}"


def build_analyze_sql(
    table: str,
    cols: Optional[List[str]] = None,
) -> str:
    """
    Generate ANALYZE TABLE ... COMPUTE STATISTICS SQL.

    Databricks uses column statistics for join strategy decisions (broadcast
    threshold) and partition pruning.  Run after major writes when AQE is
    disabled or when the query planner makes poor join choices.

    Parameters
    ----------
    table : str
    cols : list[str] | None
        Specific columns to collect stats for.  None = table-level stats only
        (row count, file sizes).  Providing cols also collects min/max/nullCount
        per column — required for Z-order effectiveness measurement.

    Returns
    -------
    str
    """
    if cols:
        col_list = ", ".join(cols)
        return f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR COLUMNS {col_list}"
    return f"ANALYZE TABLE {table} COMPUTE STATISTICS"


def build_show_partitions_sql(table: str) -> str:
    """
    Generate SHOW PARTITIONS SQL to list all partition values.

    Use to audit partition explosion — if output has >10K rows, consider
    switching to Liquid Clustering.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return f"SHOW PARTITIONS {table}"


def build_describe_detail_sql(table: str) -> str:
    """
    Generate DESCRIBE DETAIL SQL to inspect table layout and statistics.

    Returns numFiles, sizeInBytes, partitionColumns, clusteringColumns, and more.
    Use to verify that OPTIMIZE / Liquid Clustering / ZORDER took effect.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return f"DESCRIBE DETAIL {table}"
