"""
ingestion/writers.py — Delta Lake batch and streaming writers.

All data writing for the ingestion layer goes through this module.  It
provides three write patterns and two delivery modes (batch / streaming).

Write modes
-----------
overwrite
    Replaces all existing data in the target table.  Use for full-refresh
    loads where the source is the single source of truth and re-processing
    is safe.  Simple and predictable; no deduplication logic needed.
    Caveat: brief window between delete and write where the table is empty.

append
    Adds new rows to the existing table without touching existing data.
    Use for event streams, log tables, or time-partitioned sources where
    each run delivers only new records.  Requires that the caller ensures
    no duplicate delivery (or that downstream handles deduplication).

merge (MERGE INTO)
    Delta Lake upsert: for each row in the source, if a matching row exists
    in the target (matched on merge_keys) it is updated; otherwise it is
    inserted.  Use for CDC patterns where the source may resend existing
    rows with changed values.  More expensive than overwrite/append because
    it requires a full scan of the match columns.

Delivery modes
--------------
write_batch(df, config)
    Materializes a static DataFrame.  Use after read_batch().

write_stream(df, config)
    Returns a StreamingQuery from a streaming DataFrame.  Use after
    read_stream() / Auto Loader.  Supports trigger modes:
    - trigger_once=True (default): process all available files then stop.
      Equivalent to a scheduled batch job but retains Auto Loader's
      exactly-once guarantees and checkpointing.
    - trigger_once=False: run continuously until the query is stopped.
      Use for low-latency near-real-time pipelines.

Functions
---------
write_batch(df, config, pipeline_name, env) -> None
write_stream(df, config, pipeline_name, env, trigger_once, await_termination) -> StreamingQuery
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from ingestion.config import BronzeWriteConfig
from ingestion.metadata import add_bronze_columns


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _apply_metadata_and_partition(
    df: DataFrame,
    config: BronzeWriteConfig,
    pipeline_name: str,
    env: str,
) -> "pyspark.sql.DataFrameWriter":
    """Attach metadata columns and configure the DataFrameWriter."""
    if config.add_metadata:
        df = add_bronze_columns(df, pipeline_name=pipeline_name, env=env)

    writer = df.write.format("delta")

    if config.partition_by:
        writer = writer.partitionBy(*config.partition_by)

    return writer


def _target_is_path(table_name: str) -> bool:
    """Return True when the target is a DBFS/cloud path rather than a UC table."""
    return table_name.startswith("/") or table_name.startswith("dbfs:")


def _write_or_save(
    writer: "pyspark.sql.DataFrameWriter",
    table_name: str,
    mode: str,
) -> None:
    writer = writer.mode(mode)
    if _target_is_path(table_name):
        writer.save(table_name)
    else:
        writer.saveAsTable(table_name)


# ---------------------------------------------------------------------------
# Batch writers
# ---------------------------------------------------------------------------

def write_batch(
    df: DataFrame,
    config: BronzeWriteConfig,
    pipeline_name: str = "",
    env: str = "dev",
) -> None:
    """
    Write a batch DataFrame to a Delta table.

    Parameters
    ----------
    df : DataFrame
        Static DataFrame produced by read_batch() or any transformation.
    config : BronzeWriteConfig
        Controls target table, write mode, partitioning, and merge keys.
    pipeline_name : str
        Stamped into _bronze_pipeline metadata column.
    env : str
        Stamped into _bronze_env metadata column.

    Notes
    -----
    For write_mode='merge', this function uses the Delta Python API
    (DeltaTable.forName / forPath) to execute MERGE INTO.  The merge
    logic is insert-or-update: matched rows are fully overwritten with
    source values; unmatched rows are inserted.

    For write_mode='overwrite', Delta's replaceWhere is NOT used here —
    the entire table is replaced.  Add partitioning via config.partition_by
    and layer replaceWhere semantics in a dedicated function if you need
    partition-level overwrite.
    """
    if config.write_mode == "merge":
        _merge_batch(df, config, pipeline_name=pipeline_name, env=env)
        return

    writer = _apply_metadata_and_partition(df, config, pipeline_name, env)
    _write_or_save(writer, config.table_name, mode=config.write_mode)


def _merge_batch(
    df: DataFrame,
    config: BronzeWriteConfig,
    pipeline_name: str,
    env: str,
) -> None:
    """Execute a Delta MERGE INTO for upsert semantics."""
    from delta.tables import DeltaTable  # type: ignore[import]
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()

    if config.add_metadata:
        df = add_bronze_columns(df, pipeline_name=pipeline_name, env=env)

    # Ensure target table exists; if not, create it via overwrite first
    try:
        if _target_is_path(config.table_name):
            delta_table = DeltaTable.forPath(spark, config.table_name)
        else:
            delta_table = DeltaTable.forName(spark, config.table_name)
    except Exception:
        # Table doesn't exist yet — bootstrap with overwrite
        writer = df.write.format("delta").mode("overwrite")
        if config.partition_by:
            writer = writer.partitionBy(*config.partition_by)
        _write_or_save(writer, config.table_name, mode="overwrite")
        return

    # Build match condition from merge_keys
    join_cond = " AND ".join(
        f"target.{k} = source.{k}" for k in config.merge_keys
    )

    # Update all columns in matched rows; insert full row for new ones
    update_map = {c: f"source.{c}" for c in df.columns}
    insert_map = {c: f"source.{c}" for c in df.columns}

    (
        delta_table.alias("target")
        .merge(df.alias("source"), join_cond)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )


# ---------------------------------------------------------------------------
# Streaming writers
# ---------------------------------------------------------------------------

def write_stream(
    df: DataFrame,
    config: BronzeWriteConfig,
    pipeline_name: str = "",
    env: str = "dev",
    trigger_once: bool = True,
    await_termination: bool = False,
) -> StreamingQuery:
    """
    Write a streaming DataFrame to a Delta table.

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame (isStreaming must be True) from read_stream().
    config : BronzeWriteConfig
        checkpoint_path is required for streaming writes.
    pipeline_name : str
        Stamped into _bronze_pipeline metadata column.
    env : str
        Stamped into _bronze_env metadata column.
    trigger_once : bool
        True (default): process all available data then stop (availableNow
        trigger).  Behaves like a batch job but retains Auto Loader's
        exactly-once guarantees.  Ideal for scheduled jobs.
        False: run continuously.  Ideal for near-real-time pipelines.
    await_termination : bool
        When True and trigger_once=True, blocks until the query finishes
        before returning.  Useful in notebooks and scripts.  Set False when
        running multiple parallel streams (await them externally).

    Returns
    -------
    StreamingQuery
        Handle to the running stream.  Call .awaitTermination() or
        .stop() on it as needed.

    Raises
    ------
    ValueError
        When config.checkpoint_path is empty (required for all streaming
        writes).
    """
    if not config.checkpoint_path:
        raise ValueError(
            "BronzeWriteConfig.checkpoint_path is required for streaming writes. "
            "Set it to a cloud storage path that persists between runs."
        )

    if config.add_metadata:
        df = add_bronze_columns(df, pipeline_name=pipeline_name, env=env)

    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path)
    )

    if config.partition_by:
        writer = writer.partitionBy(*config.partition_by)

    if trigger_once:
        writer = writer.trigger(availableNow=True)

    if _target_is_path(config.table_name):
        query = writer.start(config.table_name)
    else:
        query = writer.toTable(config.table_name)

    if trigger_once and await_termination:
        query.awaitTermination()

    return query
