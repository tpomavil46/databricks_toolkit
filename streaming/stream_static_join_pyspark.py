"""
streaming/stream_static_join_pyspark.py — Stream-Static Join (PySpark).

Module   : streaming.stream_static_join_pyspark
Concept  : Enrich a streaming DataFrame with data from a static dimension table
When     : Each streaming event needs additional attributes from a slowly-changing
           reference table (product catalog, customer profile, geo lookup) without
           introducing a second streaming source.

What it is
----------
A stream-static join combines:
- A streaming DataFrame (e.g., live events from Kafka or Delta CDF).
- A static DataFrame (e.g., a Delta dimension table read via spark.table()).

Only the streaming side drives micro-batch execution.  The static side is read
once (or refreshed at configurable intervals) and broadcast to executors.

When to use
-----------
- Enriching events with slow-changing dimension attributes.
- Adding lookup data (country codes, product categories, price lists) to a feed.
- Any pipeline where the enrichment source changes rarely relative to the event
  stream frequency.

When NOT to use
---------------
- Both sides change frequently → stream-stream join (requires watermarks on both).
- The static table is very large and broadcast is not feasible → pre-join in Delta
  during a batch job, then read the enriched table as a stream.

How the static side is refreshed
---------------------------------
By default, Spark caches the static DataFrame at the start of the streaming query
and re-uses the same snapshot for every micro-batch.  This is fast but means the
stream does NOT see updates to the dimension table until the query is restarted.

To pick up dimension updates without restarting:
  Option 1 — Restart the stream.  Simple; appropriate for AvailableNow jobs.
  Option 2 — spark.catalog.refreshTable(table_name) inside a foreachBatch
             function, re-joining each batch against a freshly read dimension.
  Option 3 — Make the dimension a streaming source too (stream-stream join),
             which requires both sides to have watermarks.

Broadcast hint
--------------
Spark automatically broadcasts the static side when it is small enough
(spark.sql.autoBroadcastJoinThreshold, default 10 MB).  For larger tables,
either increase the threshold or use explicit F.broadcast().

Delta / Databricks considerations
----------------------------------
- Read the static dimension with spark.table() (not spark.read()) inside the
  streaming query setup so Spark knows it's a batch read, not a second stream.
- Delta time travel works on the static side: spark.read.option("versionAsOf", n).
- Requires DBR 7.3+ / Delta Lake 0.8+.

Public API
----------
enrich_stream(df_stream, df_dim, join_keys, join_type) -> DataFrame
broadcast_enrich_stream(df_stream, df_dim, join_keys, join_type) -> DataFrame
refresh_and_enrich(spark, table_name, df_stream, join_keys) -> Callable
"""

from __future__ import annotations

from functools import reduce
from typing import Callable, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def enrich_stream(
    df_stream: DataFrame,
    df_dim: DataFrame,
    join_keys: List[str],
    join_type: str = "left",
) -> DataFrame:
    """
    Join a streaming DataFrame with a static dimension DataFrame.

    Parameters
    ----------
    df_stream : DataFrame
        Streaming DataFrame (e.g., events).
    df_dim : DataFrame
        Static dimension DataFrame (e.g., spark.table("catalog.dim.products")).
    join_keys : list[str]
        Column names used in the join condition.  Must exist in both DataFrames.
    join_type : str
        Join type: 'left' (default), 'inner', 'left_semi', 'left_anti'.
        'left' preserves all streaming events even when the dimension has no match.

    Returns
    -------
    DataFrame
        Streaming DataFrame enriched with dimension columns.

    Notes
    -----
    Column name conflicts: if df_dim has columns with the same name as df_stream
    (other than join_keys), rename them before joining to avoid ambiguity.
    Use df_dim.select([F.col(c).alias(f"dim_{c}") for c in dim_cols]).
    """
    join_condition = reduce(
        lambda a, b: a & b,
        [df_stream[k] == df_dim[k] for k in join_keys],
    )
    dim_extra_cols = [c for c in df_dim.columns if c not in join_keys]
    return df_stream.join(df_dim.select(*join_keys, *dim_extra_cols), join_condition, join_type)


def broadcast_enrich_stream(
    df_stream: DataFrame,
    df_dim: DataFrame,
    join_keys: List[str],
    join_type: str = "left",
) -> DataFrame:
    """
    Join a streaming DataFrame with a broadcast-hinted static dimension.

    Forces a broadcast hash join for the static side regardless of its size.
    Use for dimensions that are small enough to fit in executor memory but
    exceed the autoBroadcastJoinThreshold.

    Parameters
    ----------
    df_stream : DataFrame
    df_dim : DataFrame
    join_keys : list[str]
    join_type : str

    Returns
    -------
    DataFrame

    Notes
    -----
    Typical safe broadcast size: < 100 MB (executor-memory dependent).
    For larger dimensions, consider pre-joining in a batch job.
    """
    join_condition = reduce(
        lambda a, b: a & b,
        [df_stream[k] == df_dim[k] for k in join_keys],
    )
    dim_extra_cols = [c for c in df_dim.columns if c not in join_keys]
    return df_stream.join(
        F.broadcast(df_dim.select(*join_keys, *dim_extra_cols)),
        join_condition,
        join_type,
    )


def make_refresh_enrich_batch_fn(
    spark: SparkSession,
    dim_table: str,
    upsert_config,
    join_keys: List[str],
) -> Callable:
    """
    Return a foreachBatch function that refreshes the dimension each batch then enriches.

    Call spark.catalog.refreshTable() before each batch to pick up dimension
    updates without restarting the stream.  This adds a metadata read overhead
    per batch but ensures the dimension is always current.

    Parameters
    ----------
    spark : SparkSession
    dim_table : str
        Fully-qualified Delta dimension table to refresh each batch.
    upsert_config : UpsertConfig
        Config for the MERGE after enrichment.
    join_keys : list[str]
        Keys for the stream-static join (not the MERGE keys — those are in upsert_config).

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None
    """
    def _fn(df_batch, batch_id):
        spark.catalog.refreshTable(dim_table)
        df_dim = spark.table(dim_table)
        df_enriched = enrich_stream(df_batch, df_dim, join_keys)
        from upserts.basic_merge_pyspark import apply_basic_merge
        apply_basic_merge(spark, df_enriched, upsert_config)

    return _fn
