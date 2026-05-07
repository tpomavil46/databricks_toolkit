"""
streaming/writers_pyspark.py — Structured Streaming Sink Writers (PySpark).

Module   : streaming.writers_pyspark
Concept  : Write a streaming DataFrame to a Delta table
When     : The terminal step of every streaming pipeline.  Choose the write
           mode based on whether you need append, update, or foreachBatch MERGE.

Output modes
------------
append
    Only newly generated rows are written each trigger.  Required for stateless
    streaming (no aggregations, no dedup) and for event/fact tables.
    Delta semantics: rows are inserted, never updated or deleted by the stream.

update
    Only rows that changed since the last trigger are written.  Used with
    stateful aggregations (GROUP BY + watermark).  Not supported for all sinks.
    Delta semantics: rows are upserted (INSERT or UPDATE) based on the key
    implied by the aggregation.

complete
    The full result table is rewritten every trigger.  Only for small
    aggregations where the state fits comfortably in memory and the sink
    can tolerate full overwrites.
    Delta semantics: full table overwrite each batch.

foreachBatch
    Escape hatch: gives you a Python function called on each micro-batch
    DataFrame.  Enables arbitrary Delta operations (MERGE, SCD2, dedup) that
    the built-in output modes don't support.  The batch function receives a
    regular (non-streaming) DataFrame and a batch ID.

Delta / Databricks considerations
----------------------------------
- writeStream to Delta with output_mode='append' is equivalent to a streaming
  INSERT — simple, fast, no shuffle required.
- foreachBatch with MERGE gives exactly-once semantics only if the MERGE
  itself is idempotent.  Make it idempotent by using the batch_id as a
  deduplication key, or by using a content hash guard.
- Trigger.AvailableNow() + foreachBatch is the recommended pattern for
  scheduled batch-style streaming jobs on Databricks.

Public API
----------
write_stream_to_delta(df, config, batch_fn) -> StreamingQuery
apply_trigger(writer, config) -> DataStreamWriter
"""

from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from streaming.config import StreamingConfig
from streaming.triggers import build_trigger_kwargs


def apply_trigger(writer, config: StreamingConfig):
    """
    Apply the configured trigger to a DataStreamWriter.

    Parameters
    ----------
    writer : DataStreamWriter
        Result of df.writeStream (or chained options).
    config : StreamingConfig

    Returns
    -------
    DataStreamWriter
        Same writer with trigger applied.
    """
    return writer.trigger(**build_trigger_kwargs(config))


def write_stream_to_delta(
    df: DataFrame,
    config: StreamingConfig,
    batch_fn: Optional[Callable] = None,
) -> StreamingQuery:
    """
    Write a streaming DataFrame to a Delta table.

    If batch_fn is provided, uses foreachBatch (enables MERGE / SCD2).
    Otherwise writes directly using the configured output mode.

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame to write.
    config : StreamingConfig
        target_table: Delta table to write to.
        output_mode: 'append', 'update', or 'complete'.
        trigger_mode / trigger_interval: controls when batches run.
        checkpoint_path: fault-tolerance checkpoint directory.
    batch_fn : callable | None
        foreachBatch function: (df_batch: DataFrame, batch_id: int) -> None.
        When provided, output_mode is forced to 'update' (Delta foreachBatch
        convention) and format is not set on the writer — the batch_fn
        controls how data is written to Delta.

    Returns
    -------
    StreamingQuery
        The running streaming query handle.  Call .awaitTermination() to
        block until the query completes (use with AvailableNow / Once).

    Examples
    --------
    Append mode:
        query = write_stream_to_delta(df, config)
        query.awaitTermination()

    foreachBatch MERGE:
        from streaming.foreach_batch_pyspark import make_upsert_batch_fn
        query = write_stream_to_delta(df, config, batch_fn=make_upsert_batch_fn(upsert_cfg))
        query.awaitTermination()
    """
    if batch_fn is not None:
        writer = (
            df.writeStream
            .option("checkpointLocation", config.checkpoint_path)
            .foreachBatch(batch_fn)
        )
        return apply_trigger(writer, config).start()

    writer = (
        df.writeStream
        .format("delta")
        .outputMode(config.output_mode)
        .option("checkpointLocation", config.checkpoint_path)
    )
    return apply_trigger(writer, config).table(config.target_table)
