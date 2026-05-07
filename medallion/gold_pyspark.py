"""
medallion/gold_pyspark.py — Gold Layer (PySpark).

Module   : medallion.gold_pyspark
Concept  : Business-facing aggregates — optimised for query, not for storage
When     : After Silver is clean.  Compute KPIs, dimensional views, ML features.

What it is
----------
Gold is the "consumption" layer.  Unlike Bronze (append) and Silver (upsert),
Gold is typically rebuilt from Silver on each run.  The idempotency pattern for
Gold is replaceWhere (partition replacement) rather than MERGE:

    df.write.format("delta")
      .mode("overwrite")
      .option("replaceWhere", f"report_date = '{date}'")
      .saveAsTable(config.gold_table)

This overwrites exactly the affected partition and leaves other partitions intact.
It is idempotent because re-running with the same date produces the same partition.

When NOT to use replaceWhere
----------------------------
- Tables with no natural partition boundary (use full OVERWRITE instead).
- Slowly-changing aggregations where history must be preserved (use MERGE).
- Real-time dashboards where Gold is a streaming aggregation (use complete mode).

Public API
----------
write_gold_replace_where(df, config, partition_value) -> None
write_gold_overwrite(df, config) -> None
make_gold_agg_batch_fn(config, agg_fn, partition_col, partition_value_fn) -> Callable
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from pyspark.sql import DataFrame, SparkSession

from medallion.config import MedallionConfig


def write_gold_replace_where(
    df: DataFrame,
    config: MedallionConfig,
    partition_value: Any,
) -> None:
    """
    Write Gold data for a single partition value, replacing it atomically.

    Uses Delta's replaceWhere option to overwrite exactly one partition's worth
    of data.  All other partitions are untouched.  Safe to re-run — the result
    is identical for the same input.

    Requires config.partition_col to be set.

    Parameters
    ----------
    df : DataFrame
        Aggregated Gold records for a single partition value.
    config : MedallionConfig
        partition_col must be non-empty.
    partition_value : any
        The value of partition_col for this batch.
        Example: "2024-06-01" for a date partition.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If config.partition_col is empty.
    """
    if not config.partition_col:
        raise ValueError(
            "MedallionConfig.partition_col must be set to use write_gold_replace_where"
        )
    predicate = f"{config.partition_col} = '{partition_value}'"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(config.gold_table)
    )


def write_gold_overwrite(
    df: DataFrame,
    config: MedallionConfig,
) -> None:
    """
    Overwrite the entire Gold table.

    Use when Gold has no natural partition (small lookup/summary tables) or
    when rebuilding from scratch after a Silver schema change.

    Parameters
    ----------
    df : DataFrame
    config : MedallionConfig

    Returns
    -------
    None
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config.gold_table)
    )


def make_gold_agg_batch_fn(
    config: MedallionConfig,
    agg_fn: Callable[[DataFrame], DataFrame],
    partition_col: Optional[str] = None,
    partition_value_fn: Optional[Callable[[DataFrame], Any]] = None,
) -> Callable[[DataFrame, int], None]:
    """
    Return a foreachBatch function that aggregates Silver data and writes Gold.

    The returned function:
      1. Applies agg_fn to the Silver batch (groupBy + agg, window, etc.).
      2. Writes the result to Gold using replaceWhere (if partition_col provided)
         or full overwrite (if not).

    Parameters
    ----------
    config : MedallionConfig
    agg_fn : callable
        (DataFrame) -> DataFrame.  Performs the aggregation logic.
        Input is the raw Silver batch from the streaming read.
    partition_col : str | None
        Column to partition Gold by.  If set, partition_value_fn must also be set.
    partition_value_fn : callable | None
        (DataFrame) -> any.  Extracts the partition value from the aggregated df.
        Example: lambda df: df.first()["report_date"]

    Returns
    -------
    Callable[[DataFrame, int], None]
    """
    col = partition_col or config.partition_col

    def _fn(df_batch: DataFrame, batch_id: int) -> None:
        gold_df = agg_fn(df_batch)
        if col and partition_value_fn is not None:
            pval = partition_value_fn(gold_df)
            write_gold_replace_where(gold_df, config, pval)
        else:
            write_gold_overwrite(gold_df, config)

    return _fn
