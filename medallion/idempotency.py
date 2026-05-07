"""
medallion/idempotency.py — Watermark-based Idempotency for Batch Pipelines.

Module   : medallion.idempotency
Concept  : Track the last successfully processed timestamp so batch jobs can
           resume from exactly where they left off
When     : Batch (non-streaming) medallion pipelines that read from a source
           with a monotonically increasing timestamp column.

What it is
----------
Streaming pipelines achieve idempotency through Structured Streaming checkpoints.
Batch pipelines need their own mechanism.  This module implements a simple
watermark table pattern:

    ┌──────────────────────────────────────────────────────────────┐
    │ pipeline_watermarks (Delta table)                            │
    │  pipeline_name  STRING  PK                                   │
    │  last_ts        TIMESTAMP  last successfully processed ts    │
    │  updated_at     TIMESTAMP  when this row was last written    │
    └──────────────────────────────────────────────────────────────┘

Each pipeline run:
  1. Reads the watermark → last_ts (or MIN_TIMESTAMP if first run).
  2. Queries source WHERE event_ts > last_ts AND event_ts <= now().
  3. Processes the window.
  4. Writes the new watermark on success.

If step 3 fails, the watermark is NOT advanced.  The next run re-processes
the same window — safe because Silver uses MERGE (idempotent).

Why a Delta watermark table?
----------------------------
- Survives cluster restarts and job failures.
- Can be queried / reset manually without code changes.
- Supports multiple concurrent pipelines in the same table (keyed by name).
- CDF on the watermark table gives you a full audit of when each pipeline ran.

Public API
----------
build_watermark_table_sql(watermark_table) -> str
build_get_watermark_sql(watermark_table, pipeline_name) -> str
build_upsert_watermark_sql(watermark_table, pipeline_name, new_ts) -> str
build_incremental_source_sql(source_table, ts_col, watermark_ts, end_ts) -> str
read_watermark(spark, watermark_table, pipeline_name) -> datetime | None
write_watermark(spark, watermark_table, pipeline_name, new_ts) -> None
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession


MIN_TIMESTAMP = "1970-01-01T00:00:00"


def build_watermark_table_sql(watermark_table: str) -> str:
    """
    Generate CREATE TABLE IF NOT EXISTS SQL for the pipeline watermark table.

    Parameters
    ----------
    watermark_table : str
        Fully-qualified Delta table name.

    Returns
    -------
    str
    """
    return f"""CREATE TABLE IF NOT EXISTS {watermark_table} (
  pipeline_name  STRING NOT NULL,
  last_ts        TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)"""


def build_get_watermark_sql(
    watermark_table: str,
    pipeline_name: str,
) -> str:
    """
    Generate SQL that returns the current watermark for a pipeline.

    Returns one row with column last_ts, or no rows if no watermark exists yet.

    Parameters
    ----------
    watermark_table : str
    pipeline_name : str

    Returns
    -------
    str
    """
    return f"""SELECT last_ts
FROM {watermark_table}
WHERE pipeline_name = '{pipeline_name}'"""


def build_upsert_watermark_sql(
    watermark_table: str,
    pipeline_name: str,
    new_ts: str,
) -> str:
    """
    Generate MERGE SQL that advances the watermark for a pipeline.

    Creates the row on first run; updates it on subsequent runs.

    Parameters
    ----------
    watermark_table : str
    pipeline_name : str
    new_ts : str
        New watermark timestamp as an ISO-8601 string.
        Example: '2024-06-01T12:00:00'

    Returns
    -------
    str
    """
    return f"""MERGE INTO {watermark_table} AS t
USING (
  SELECT
    '{pipeline_name}' AS pipeline_name,
    TIMESTAMP '{new_ts}' AS last_ts,
    current_timestamp() AS updated_at
) AS s
ON t.pipeline_name = s.pipeline_name
WHEN MATCHED THEN UPDATE SET t.last_ts = s.last_ts, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (pipeline_name, last_ts, updated_at)
  VALUES (s.pipeline_name, s.last_ts, s.updated_at)"""


def build_incremental_source_sql(
    source_table: str,
    ts_col: str,
    watermark_ts: str,
    end_ts: Optional[str] = None,
) -> str:
    """
    Generate SELECT SQL for records in a half-open timestamp window.

    Selects rows where ts_col > watermark_ts (exclusive lower bound).
    When end_ts is provided, also applies ts_col <= end_ts (inclusive upper bound)
    to prevent reading records that are still being written.

    Parameters
    ----------
    source_table : str
    ts_col : str
    watermark_ts : str
        ISO-8601 timestamp string.  Use MIN_TIMESTAMP for first run.
    end_ts : str | None
        Upper bound.  When None, selects everything after watermark_ts.

    Returns
    -------
    str
    """
    upper = f" AND {ts_col} <= TIMESTAMP '{end_ts}'" if end_ts else ""
    return (
        f"SELECT * FROM {source_table}\n"
        f"WHERE {ts_col} > TIMESTAMP '{watermark_ts}'{upper}"
    )


def read_watermark(
    spark: SparkSession,
    watermark_table: str,
    pipeline_name: str,
) -> Optional[datetime]:
    """
    Return the current watermark datetime for a pipeline, or None if none exists.

    Parameters
    ----------
    spark : SparkSession
    watermark_table : str
    pipeline_name : str

    Returns
    -------
    datetime | None
        UTC datetime of the last processed record, or None on first run.
    """
    sql = build_get_watermark_sql(watermark_table, pipeline_name)
    rows = spark.sql(sql).collect()
    if not rows:
        return None
    ts = rows[0]["last_ts"]
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def write_watermark(
    spark: SparkSession,
    watermark_table: str,
    pipeline_name: str,
    new_ts: datetime,
) -> None:
    """
    Advance the pipeline watermark to new_ts.

    Call this AFTER the pipeline batch has been committed successfully.
    Never call it inside a try/except that swallows write errors — an
    incorrectly advanced watermark causes data loss.

    Parameters
    ----------
    spark : SparkSession
    watermark_table : str
    pipeline_name : str
    new_ts : datetime

    Returns
    -------
    None
    """
    ts_str = new_ts.strftime("%Y-%m-%dT%H:%M:%S")
    sql = build_upsert_watermark_sql(watermark_table, pipeline_name, ts_str)
    spark.sql(sql)
