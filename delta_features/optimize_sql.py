"""
delta_features/optimize_sql.py — OPTIMIZE, Z-Ordering, and Liquid Clustering.

Module   : delta_features.optimize_sql
Concept  : Compact small files and co-locate related data for faster queries
When     : After ingestion runs that produce many small files, or when query
           performance on high-cardinality filter columns degrades over time.

What it is
----------
Delta tables accumulate small Parquet files over time as streaming writes,
incremental appends, and MERGE operations each create one or more new files.
Small files hurt query performance: more files = more file-open overhead,
more metadata scanning, less data-local execution.

Three tools address this:

OPTIMIZE
    Compacts small files within each partition into larger files (target ~1 GB).
    Rewrites the data; produces a new Delta version.  Safe to run at any time —
    concurrent reads see either the old or the new files (not both), thanks to
    Delta's MVCC.

    OPTIMIZE is idempotent: running it twice on an already-optimized table is a
    no-op (no new files are generated when all files are already large enough).

ZORDER BY
    Applied as part of OPTIMIZE.  Sorts and co-locates data within files by the
    specified columns, so that a query filtering on those columns reads fewer files.
    Z-ordering is a multi-dimensional space-filling curve — it outperforms simple
    sorting on multi-column filter predicates.

    Trade-offs:
    - OPTIMIZE ZORDER BY is significantly slower than plain OPTIMIZE (must sort data).
    - Each run rewrites all files, not just new ones (unless Auto-Optimize is used).
    - Best on low-cardinality to medium-cardinality columns with frequent filter predicates.
    - Ineffective for very high cardinality columns (e.g., UUIDs) — use Bloom filters.
    - Only applies within the same partition. Always partition by high-cardinality
      date/category column first, then ZORDER on the next level.

Liquid Clustering (DBR 13.3+)
    Next-generation replacement for Z-ordering.  Uses a clustering key to
    incrementally re-cluster only new/changed data files, rather than rewriting
    the entire table on every OPTIMIZE run.

    Trade-offs vs Z-ordering:
    + Incremental: only newly written files are clustered (OPTIMIZE is much faster).
    + No manual partition management — no need to decide partition column.
    + Supports up to 4 clustering columns.
    - Cannot be used on a table that already has partitions (unless converted).
    - Requires DBR 13.3+ / Delta 3.1+.
    - CLUSTER BY column(s) is set once at table creation or via ALTER TABLE.

Public API
----------
build_optimize_sql(table, zorder_cols, where_clause) -> str
build_liquid_clustering_sql(table, cluster_cols) -> str
build_disable_liquid_clustering_sql(table) -> str
build_analyze_sql(table, cols) -> str
apply_optimize(spark, table, zorder_cols, where_clause) -> None
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import SparkSession


def build_optimize_sql(
    table: str,
    zorder_cols: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
) -> str:
    """
    Generate OPTIMIZE SQL, optionally with ZORDER BY and partition predicate.

    Parameters
    ----------
    table : str
        Fully-qualified Delta table name.
    zorder_cols : list[str] | None
        Columns to Z-order by.  None = plain file compaction with no reordering.
        Choose columns that appear frequently in WHERE / JOIN conditions.
    where_clause : str | None
        Partition predicate to limit OPTIMIZE to specific partitions.
        Example: "date >= '2024-01-01'"
        Only applies to partitioned tables.  Speeds up targeted OPTIMIZE runs
        on recently-written partitions rather than the full table.

    Returns
    -------
    str
        OPTIMIZE SQL statement.

    Examples
    --------
        # Plain compaction
        build_optimize_sql("catalog.gold.fact_orders")
        # OPTIMIZE catalog.gold.fact_orders

        # Z-order on query columns
        build_optimize_sql("catalog.gold.fact_orders", zorder_cols=["customer_id", "order_date"])
        # OPTIMIZE catalog.gold.fact_orders ZORDER BY (customer_id, order_date)

        # Targeted to recent partition
        build_optimize_sql("catalog.gold.fact_orders", where_clause="date = '2024-01-15'")
        # OPTIMIZE catalog.gold.fact_orders WHERE date = '2024-01-15'
    """
    sql = f"OPTIMIZE {table}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        sql += f" ZORDER BY ({cols})"
    return sql


def build_liquid_clustering_sql(
    table: str,
    cluster_cols: List[str],
) -> str:
    """
    Generate ALTER TABLE ... CLUSTER BY (...) to enable Liquid Clustering.

    Call this once on an existing table to convert it to Liquid Clustering.
    For new tables, use CREATE TABLE ... CLUSTER BY (...) instead.

    After setting the cluster key, run OPTIMIZE (without ZORDER BY) to trigger
    incremental clustering of new files.

    Parameters
    ----------
    table : str
    cluster_cols : list[str]
        1–4 columns to cluster by.  Choose high-selectivity filter columns.

    Returns
    -------
    str
        ALTER TABLE ... CLUSTER BY SQL.

    Notes
    -----
    - Cannot enable Liquid Clustering on a table that has partition columns.
    - Requires Delta Lake 3.1+ / DBR 13.3+.
    - To cluster incrementally: run OPTIMIZE (not ZORDER) after each data write.
    """
    if not cluster_cols:
        raise ValueError("cluster_cols must contain at least one column")
    if len(cluster_cols) > 4:
        raise ValueError(f"Liquid Clustering supports at most 4 columns, got {len(cluster_cols)}")
    cols = ", ".join(cluster_cols)
    return f"ALTER TABLE {table} CLUSTER BY ({cols})"


def build_disable_liquid_clustering_sql(table: str) -> str:
    """
    Generate ALTER TABLE ... CLUSTER BY NONE to remove Liquid Clustering.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return f"ALTER TABLE {table} CLUSTER BY NONE"


def build_analyze_sql(
    table: str,
    cols: Optional[List[str]] = None,
) -> str:
    """
    Generate ANALYZE TABLE SQL to compute column statistics for the optimizer.

    Running ANALYZE after large writes or OPTIMIZE helps the Spark optimizer
    choose better join strategies and avoid data skew.

    Parameters
    ----------
    table : str
    cols : list[str] | None
        Specific columns to analyze.  None = compute statistics for all columns.

    Returns
    -------
    str
    """
    if cols:
        col_list = ", ".join(cols)
        return f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR COLUMNS {col_list}"
    return f"ANALYZE TABLE {table} COMPUTE STATISTICS"


def apply_optimize(
    spark: SparkSession,
    table: str,
    zorder_cols: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
) -> None:
    """
    Execute OPTIMIZE (and optionally ZORDER BY) against a live Delta table.

    Parameters
    ----------
    spark : SparkSession
    table : str
    zorder_cols : list[str] | None
    where_clause : str | None

    Returns
    -------
    None
    """
    spark.sql(build_optimize_sql(table, zorder_cols, where_clause))
