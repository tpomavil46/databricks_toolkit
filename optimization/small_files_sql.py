"""
optimization/small_files_sql.py — Small File Problem: Detection and Remediation.

Module   : optimization.small_files_sql
Concept  : Prevent and fix the small files problem in Delta tables
When     : Tables with many small writes (streaming, frequent micro-batch appends,
           high-frequency MERGE operations).

The small files problem
-----------------------
Each Delta write (streaming micro-batch, INSERT, MERGE) produces Parquet files.
Frequent writes produce many small files.  Reading 10 000 × 10 MB files is much
slower than reading 100 × 1 GB files because:
  - Each file requires a separate S3/ADLS round trip for metadata.
  - The query planner lists all files before reading — slow on millions of files.
  - Parquet compression is less effective on small files (header overhead).

Target file size: 128 MB–1 GB.  DESCRIBE DETAIL shows average file size.

Fix 1: OPTIMIZE (manual bin-packing)
  Run on a schedule (daily/hourly for hot partitions).
  Can scope to recent partitions: WHERE date >= current_date() - 3.

Fix 2: Auto-compaction (automatic, triggered by Delta on write)
  delta.autoOptimize.autoCompact = true
  Runs asynchronously after writes.  No scheduling needed.
  Produces 128 MB target files (not configurable per-table).

Fix 3: Optimized writes (reduces files at write time)
  delta.autoOptimize.optimizeWrite = true
  Shuffle data before write to produce fewer, fuller files.
  Adds latency to each write — trade-off for streaming micro-batches.

Fix 4: Target file size (configure OPTIMIZE target)
  delta.targetFileSize = <bytes>
  Overrides the 1 GB default for OPTIMIZE.

Public API
----------
build_optimize_sql(table, where_predicate, zorder_cols) -> str  [re-exported]
build_enable_auto_compact_sql(table) -> str
build_disable_auto_compact_sql(table) -> str
build_enable_optimize_write_sql(table) -> str
build_disable_optimize_write_sql(table) -> str
build_set_target_file_size_sql(table, size_bytes) -> str
build_file_count_check_sql(table) -> str
build_vacuum_small_files_sql(table, retain_hours) -> str
"""

from __future__ import annotations

from typing import List, Optional

MINIMUM_RETENTION_HOURS = 168
DEFAULT_TARGET_FILE_SIZE_BYTES = 134_217_728  # 128 MB


def build_enable_auto_compact_sql(table: str) -> str:
    """
    Enable automatic compaction on a Delta table.

    Delta triggers compaction asynchronously after writes when files are
    below the target size.  No maintenance job required.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.autoOptimize.autoCompact' = 'true')"
    )


def build_disable_auto_compact_sql(table: str) -> str:
    """Disable auto-compaction on a Delta table."""
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.autoOptimize.autoCompact' = 'false')"
    )


def build_enable_optimize_write_sql(table: str) -> str:
    """
    Enable optimized writes on a Delta table.

    Shuffles data at write time to produce fewer, larger Parquet files.
    Adds ~10–30% latency per write.  Worth it for tables with many small
    streaming batches where OPTIMIZE runs infrequently.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.autoOptimize.optimizeWrite' = 'true')"
    )


def build_disable_optimize_write_sql(table: str) -> str:
    """Disable optimized writes on a Delta table."""
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.autoOptimize.optimizeWrite' = 'false')"
    )


def build_set_target_file_size_sql(
    table: str,
    size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES,
) -> str:
    """
    Set the target Parquet file size for OPTIMIZE on a Delta table.

    Default is 1 GB (Delta default).  Override to 128 MB for tables that are
    read with high concurrency (more parallelism) or to 256 MB for bulk scans.

    Parameters
    ----------
    table : str
    size_bytes : int
        Target file size in bytes.  Must be > 0.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If size_bytes <= 0.
    """
    if size_bytes <= 0:
        raise ValueError(f"size_bytes must be positive, got {size_bytes}")
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.targetFileSize' = '{size_bytes}')"
    )


def build_file_count_check_sql(table: str) -> str:
    """
    Generate SQL that returns file count and average size for a table.

    Use to diagnose small file problems before and after remediation.
    Output columns: num_files, total_size_mb, avg_file_size_mb.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return f"""SELECT
  numFiles                                           AS num_files,
  ROUND(sizeInBytes / 1024 / 1024, 2)               AS total_size_mb,
  ROUND(sizeInBytes / 1024 / 1024 / numFiles, 2)    AS avg_file_size_mb
FROM (DESCRIBE DETAIL {table})"""


def build_vacuum_small_files_sql(
    table: str,
    retain_hours: int = MINIMUM_RETENTION_HOURS,
) -> str:
    """
    Generate VACUUM SQL to remove obsolete files after OPTIMIZE.

    After OPTIMIZE rewrites files, the old small files are retained until
    VACUUM removes them.  VACUUM also removes files left by failed writes.

    Parameters
    ----------
    table : str
    retain_hours : int
        Retention window in hours.  Cannot be less than 168 (7 days) — Delta
        enforces this minimum to protect concurrent readers.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If retain_hours < MINIMUM_RETENTION_HOURS.
    """
    if retain_hours < MINIMUM_RETENTION_HOURS:
        raise ValueError(
            f"retain_hours {retain_hours} is below Delta minimum of "
            f"{MINIMUM_RETENTION_HOURS}h (7 days). "
            "Set spark.databricks.delta.retentionDurationCheck.enabled = false "
            "only in test environments."
        )
    return f"VACUUM {table} RETAIN {retain_hours} HOURS"
