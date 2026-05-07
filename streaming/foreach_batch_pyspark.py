"""
streaming/foreach_batch_pyspark.py — foreachBatch Factories (PySpark).

Module   : streaming.foreach_batch_pyspark
Concept  : Use arbitrary Delta operations (MERGE, SCD2) inside a streaming pipeline
When     : The built-in append/update/complete output modes are not sufficient —
           you need Delta MERGE, idempotent writes, or SCD Type 2 from a stream.

What it is
----------
foreachBatch is a streaming sink that calls a user-defined function on each
micro-batch DataFrame.  The batch DataFrame is a regular (non-streaming)
DataFrame — you can run any PySpark or Delta operation on it.

This module provides factory functions that return correctly-typed foreachBatch
functions.  Using factories (rather than inline lambdas) lets you:
1. Pass configuration at setup time without closures over mutable state.
2. Test the factory logic without a running stream.
3. Compose multiple operations (dedup → hash check → MERGE).

Idempotency and exactly-once
-----------------------------
Structured Streaming guarantees that each micro-batch is delivered at least
once to the foreachBatch function.  On failure, the batch may be re-delivered
with the SAME batch_id.

To achieve exactly-once end-to-end semantics, your batch_fn must be idempotent:
- Delta MERGE is naturally idempotent if the source has no duplicates.
- Use deduplicate_source() before MERGE when the source may have duplicates.
- Use add_content_hash() + hash-guarded MERGE for write-amplification-free
  idempotency on high-volume tables.
- Alternatively, filter out already-processed batch IDs using a control table.

Accessing SparkSession inside foreachBatch
-------------------------------------------
The batch DataFrame is created in the streaming executor context.  Access the
SparkSession via df_batch.sparkSession (not spark — that variable is not in scope
inside the closure on the executor side).

Delta / Databricks considerations
----------------------------------
- foreachBatch with .trigger(availableNow=True) is the recommended pattern for
  scheduled batch-style streaming jobs on Databricks.
- DeltaTable.forName() inside foreachBatch requires the target table to exist.
  Create it with CTAS or an empty write before starting the stream.
- Requires DBR 8.0+ / Delta Lake 1.0+.

Public API
----------
make_upsert_batch_fn(upsert_config) -> Callable
make_dedup_upsert_batch_fn(upsert_config, ts_col) -> Callable
make_idempotent_batch_fn(upsert_config, order_col, tracked_columns) -> Callable
make_scd2_batch_fn(scd_config) -> Callable
"""

from __future__ import annotations

from typing import Callable, List, Optional

from upserts.config import UpsertConfig


def make_upsert_batch_fn(upsert_config: UpsertConfig) -> Callable:
    """
    Return a foreachBatch function that executes a basic INSERT/UPDATE MERGE.

    The simplest foreachBatch pattern: write each micro-batch to the target
    Delta table using a basic MERGE.  Assumes the batch has unique merge keys.

    Parameters
    ----------
    upsert_config : UpsertConfig

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None

    Examples
    --------
        config = UpsertConfig(
            source_table="",  # unused — source is the batch df
            target_table="catalog.gold.dim_customer",
            merge_keys=["customer_id"],
        )
        batch_fn = make_upsert_batch_fn(config)
        df.writeStream.foreachBatch(batch_fn).trigger(availableNow=True).start()
    """
    def _fn(df_batch, batch_id):
        from upserts.basic_merge_pyspark import apply_basic_merge
        apply_basic_merge(df_batch.sparkSession, df_batch, upsert_config)

    return _fn


def make_dedup_upsert_batch_fn(
    upsert_config: UpsertConfig,
    ts_col: str,
) -> Callable:
    """
    Return a foreachBatch function that deduplicates the batch then MERGEs.

    Use when the source stream may deliver multiple records for the same merge
    key within a single micro-batch (e.g., high-throughput Kafka topics).
    The deduplication picks the most recent record by ts_col.

    Parameters
    ----------
    upsert_config : UpsertConfig
    ts_col : str
        Timestamp or sequence column.  Most recent value wins.

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None
    """
    def _fn(df_batch, batch_id):
        from upserts.idempotent_pyspark import deduplicate_source
        from upserts.basic_merge_pyspark import apply_basic_merge
        df_deduped = deduplicate_source(df_batch, upsert_config.merge_keys, ts_col)
        apply_basic_merge(df_batch.sparkSession, df_deduped, upsert_config)

    return _fn


def make_idempotent_batch_fn(
    upsert_config: UpsertConfig,
    order_col: str = "",
    tracked_columns: Optional[List[str]] = None,
) -> Callable:
    """
    Return a foreachBatch function that runs a content-hash idempotent MERGE.

    Skips writing rows whose content hash matches what is already in the target.
    Eliminates write amplification on stable rows in high-fanout batches.

    Requires the target table to have the content_hash_col column pre-existing.

    Parameters
    ----------
    upsert_config : UpsertConfig
        content_hash_col is the hash column name (default: '_content_hash').
    order_col : str
        Deduplication order column.  Empty = skip deduplication.
    tracked_columns : list[str] | None
        Columns to hash.  None = all non-key columns resolved at runtime.

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None
    """
    def _fn(df_batch, batch_id):
        from upserts.idempotent_pyspark import apply_idempotent_merge
        apply_idempotent_merge(
            df_batch.sparkSession, df_batch, upsert_config, order_col=order_col
        )

    return _fn


def make_scd2_batch_fn(scd_config) -> Callable:
    """
    Return a foreachBatch function that applies SCD Type 2 to each micro-batch.

    Each micro-batch triggers the full SCD2 two-step:
    1. Expire MERGE: close current rows in the target whose key appears in the batch.
    2. Append new versions: insert new current rows for changed/new keys.

    Parameters
    ----------
    scd_config : SCDConfig
        effective_start_col, effective_end_col, is_current_col must be set.

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None

    Notes
    -----
    SCD2 in streaming is inherently stateful at the table level: the MERGE reads
    the current target state on every batch.  This is correct and safe with Delta,
    which provides snapshot isolation on each read.
    """
    def _fn(df_batch, batch_id):
        from scd.type2_pyspark import apply_scd2
        apply_scd2(df_batch.sparkSession, df_batch, scd_config)

    return _fn


def make_delete_aware_batch_fn(upsert_config: UpsertConfig) -> Callable:
    """
    Return a foreachBatch function that handles CDC inserts, updates, and deletes.

    Parameters
    ----------
    upsert_config : UpsertConfig
        delete_indicator_col and delete_indicator_value must be set.

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None
    """
    def _fn(df_batch, batch_id):
        from upserts.delete_aware_pyspark import apply_delete_aware_merge
        apply_delete_aware_merge(df_batch.sparkSession, df_batch, upsert_config)

    return _fn
