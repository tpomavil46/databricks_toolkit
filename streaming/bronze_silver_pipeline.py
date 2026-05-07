"""
streaming/bronze_silver_pipeline.py — Bronze → Silver Streaming Pipeline Templates.

Module   : streaming.bronze_silver_pipeline
Concept  : Composable templates that wire together readers, transforms, and writers
When     : You need a reference implementation of a full Bronze → Silver streaming
           pipeline, not just individual primitives.

Patterns
--------
Pattern 1 — Auto Loader → Delta Append (Bronze landing)
    Reads raw files from a cloud landing zone.  Applies Bronze metadata columns.
    Writes to a Bronze Delta table in append mode.
    Use for: raw data ingestion with schema inference.

Pattern 2 — Delta CDF → MERGE (Silver upsert)
    Reads change data from a Bronze Delta table using Change Data Feed.
    Applies optional transforms (parsing, type casting).
    Writes to a Silver Delta table via foreachBatch MERGE.
    Use for: Bronze → Silver CDC propagation.

Pattern 3 — Auto Loader → foreachBatch MERGE (combined Bronze+Silver)
    Reads files, deduplicates within each batch, and MERGEs directly to Silver.
    Skips the separate Bronze layer.
    Use for: smaller pipelines where raw preservation is not required.

Pattern 4 — Delta Stream → Watermark → Window Agg → Append (Silver aggregation)
    Reads a Bronze event table.  Applies watermark + tumbling window aggregation.
    Appends finalized windows to a Silver aggregation table.
    Use for: metrics, KPIs, pre-aggregated reporting layers.

Design principles
-----------------
- Each builder returns a StreamingQuery, not None — callers block with
  .awaitTermination() or manage the query lifecycle.
- transform_fn is an optional pure-transform hook: (DataFrame) -> DataFrame.
  Use it to add Bronze metadata columns, cast types, or drop nulls.
- All pipelines are config-driven; no hardcoded paths or table names.

Public API
----------
build_autoloader_bronze_pipeline(spark, config, transform_fn) -> StreamingQuery
build_delta_cdf_silver_pipeline(spark, config, upsert_config, transform_fn) -> StreamingQuery
build_autoloader_silver_pipeline(spark, config, upsert_config, ts_col, transform_fn) -> StreamingQuery
build_window_agg_pipeline(spark, config, agg_exprs, group_cols) -> StreamingQuery
"""

from __future__ import annotations

from typing import Callable, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.config import StreamingConfig
from streaming.readers_pyspark import read_autoloader, read_delta_stream
from streaming.writers_pyspark import write_stream_to_delta


def build_autoloader_bronze_pipeline(
    spark: SparkSession,
    config: StreamingConfig,
    transform_fn: Optional[Callable[[DataFrame], DataFrame]] = None,
) -> StreamingQuery:
    """
    Auto Loader → optional transform → Delta append (Bronze landing zone).

    Parameters
    ----------
    spark : SparkSession
    config : StreamingConfig
        source_path: landing zone directory.
        source_format: 'json', 'csv', 'parquet', etc.
        target_table: Bronze Delta table.
        output_mode: should be 'append' for Bronze.
    transform_fn : callable | None
        Optional: (DataFrame) -> DataFrame.  Apply Bronze metadata, cast types,
        or filter records before writing.  If None, raw schema is written as-is.

    Returns
    -------
    StreamingQuery

    Examples
    --------
        from ingestion.metadata import add_bronze_columns

        config = StreamingConfig(
            source_path="abfss://landing@storage.dfs.core.windows.net/customers/",
            source_format="json",
            target_table="catalog.bronze.customers_raw",
            checkpoint_base="dbfs:/checkpoints",
            pipeline_name="customers_autoloader_bronze",
            trigger_mode="available_now",
        )

        query = build_autoloader_bronze_pipeline(spark, config, add_bronze_columns)
        query.awaitTermination()
    """
    df = read_autoloader(spark, config)
    if transform_fn:
        df = transform_fn(df)
    return write_stream_to_delta(df, config)


def build_delta_cdf_silver_pipeline(
    spark: SparkSession,
    config: StreamingConfig,
    upsert_config,
    transform_fn: Optional[Callable[[DataFrame], DataFrame]] = None,
) -> StreamingQuery:
    """
    Delta CDF stream → optional transform → foreachBatch MERGE (Silver upsert).

    Reads change data from a Bronze Delta table (Change Data Feed must be enabled
    on the source: TBLPROPERTIES delta.enableChangeDataFeed = true).

    Parameters
    ----------
    spark : SparkSession
    config : StreamingConfig
        source_path: Bronze Delta table name or path.
        source_format: 'delta'.
    upsert_config : UpsertConfig
        MERGE configuration for the Silver target.
    transform_fn : callable | None
        Optional transform applied between reading and writing.

    Returns
    -------
    StreamingQuery

    Notes
    -----
    CDF adds '_change_type', '_commit_version', '_commit_timestamp' columns.
    Filter these out in transform_fn before writing to Silver if your schema
    does not include them.
    """
    from streaming.foreach_batch_pyspark import make_upsert_batch_fn

    df = read_delta_stream(spark, config.source_path, config)
    if transform_fn:
        df = transform_fn(df)
    batch_fn = make_upsert_batch_fn(upsert_config)
    return write_stream_to_delta(df, config, batch_fn=batch_fn)


def build_autoloader_silver_pipeline(
    spark: SparkSession,
    config: StreamingConfig,
    upsert_config,
    ts_col: str,
    transform_fn: Optional[Callable[[DataFrame], DataFrame]] = None,
) -> StreamingQuery:
    """
    Auto Loader → dedup → foreachBatch MERGE (combined Bronze+Silver).

    Reads raw files, deduplicates within each micro-batch by ts_col, then
    MERGEs directly into a Silver Delta table.  Skips a separate Bronze layer.

    Parameters
    ----------
    spark : SparkSession
    config : StreamingConfig
        source_path, source_format, target_table, checkpoint settings.
    upsert_config : UpsertConfig
        MERGE keys and target table for Silver.
    ts_col : str
        Timestamp/sequence column used to pick the winning duplicate.
    transform_fn : callable | None

    Returns
    -------
    StreamingQuery
    """
    from streaming.foreach_batch_pyspark import make_dedup_upsert_batch_fn

    df = read_autoloader(spark, config)
    if transform_fn:
        df = transform_fn(df)
    batch_fn = make_dedup_upsert_batch_fn(upsert_config, ts_col)
    return write_stream_to_delta(df, config, batch_fn=batch_fn)


def build_window_agg_pipeline(
    spark: SparkSession,
    config: StreamingConfig,
    agg_exprs: List[Column],
    group_cols: Optional[List[str]] = None,
) -> StreamingQuery:
    """
    Delta stream → watermark → tumbling window aggregation → Delta append.

    Reads events from a Bronze Delta table, applies a watermark, groups into
    tumbling windows, and appends finalized windows to a Silver aggregation table.

    Parameters
    ----------
    spark : SparkSession
    config : StreamingConfig
        source_path: Bronze event table.
        watermark_col: event timestamp column.
        watermark_delay: max late arrival (e.g., '10 minutes').
        trigger_interval: window finalization cadence.
        output_mode: 'append' (emit only finalized windows) or 'update'.
        target_table: Silver aggregation table.
    agg_exprs : list[Column]
        Aggregation expressions, e.g. [F.count("*").alias("n")].
    group_cols : list[str] | None
        Additional grouping dimensions beyond the time window.

    Returns
    -------
    StreamingQuery

    Notes
    -----
    watermark_delay should be >= the window size to avoid incomplete windows
    being emitted prematurely in 'append' output mode.
    """
    from streaming.stateful_pyspark import add_watermark, tumbling_window_agg

    df = read_delta_stream(spark, config.source_path, config)
    df = add_watermark(df, config.watermark_col, config.watermark_delay)
    df = tumbling_window_agg(
        df,
        event_time_col=config.watermark_col,
        window_duration=config.trigger_interval,
        agg_exprs=agg_exprs,
        group_cols=group_cols,
    )
    return write_stream_to_delta(df, config)
