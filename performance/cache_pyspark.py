"""
performance/cache_pyspark.py — DataFrame Caching and Persistence (PySpark).

Module   : performance.cache_pyspark
Concept  : Cache DataFrames in memory or on disk to avoid recomputation
When     : A DataFrame is used more than once in the same action chain.

When to cache
-------------
Cache when:
  - A DataFrame is used in multiple actions (count + write, or two joins).
  - Computing the DataFrame is expensive (large shuffle, complex aggregation).
  - The DataFrame fits in executor memory (check via Spark UI → Storage tab).

Do NOT cache when:
  - The DataFrame is used only once — caching wastes time and memory.
  - The DataFrame is too large to fit in memory — spill defeats the purpose.
  - You're reading from Delta with data-skipping — Delta's native caching (DBR)
    is more efficient than Spark's in-memory cache for most workloads.
  - The cluster uses Photon — Photon has its own native cache layer.

StorageLevel options
--------------------
MEMORY_ONLY           Fast read; spills nothing — OOM if too large.
MEMORY_AND_DISK       Falls back to disk on spill (recommended default).
DISK_ONLY             Cheap; slower reads; avoids OOM on very large DataFrames.
MEMORY_ONLY_SER       Serialised in memory — less memory, slower reads.
MEMORY_AND_DISK_SER   Serialised + disk fallback.
OFF_HEAP              Uses off-heap memory (requires off-heap configured).

Databricks Delta cache vs Spark cache
---------------------------------------
Databricks' local SSD Delta cache caches Parquet data on disk automatically.
It is enabled by default on most clusters.  Spark's in-memory cache (df.cache())
is different — it caches the *result of a computation* (after filters, joins, etc.)
in executor JVM memory.  Use Spark cache for computed DataFrames; rely on Delta
cache for raw table reads.

Public API
----------
cache_df(df, storage_level, eager) -> DataFrame
unpersist_df(df, blocking) -> DataFrame
checkpoint_df(spark, df, checkpoint_dir, eager) -> DataFrame
estimate_cache_size_mb(df, sample_fraction) -> float
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel


def cache_df(
    df: DataFrame,
    storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    eager: bool = False,
) -> DataFrame:
    """
    Cache a DataFrame at the specified StorageLevel.

    Parameters
    ----------
    df : DataFrame
    storage_level : StorageLevel
        Default: MEMORY_AND_DISK (recommended — safe fallback to disk).
    eager : bool
        When True, triggers an action immediately to materialise the cache.
        When False (default), caching is lazy — materialised on first action.
        Use eager=True when you want to measure cache time explicitly.

    Returns
    -------
    DataFrame
        The same DataFrame with caching registered.
    """
    df.persist(storage_level)
    if eager:
        df.count()
    return df


def unpersist_df(
    df: DataFrame,
    blocking: bool = False,
) -> DataFrame:
    """
    Remove a DataFrame from the cache.

    Parameters
    ----------
    df : DataFrame
    blocking : bool
        When True, waits for the cache to be fully released before returning.
        Useful in tests to ensure memory is freed before the next assertion.

    Returns
    -------
    DataFrame
    """
    df.unpersist(blocking=blocking)
    return df


def checkpoint_df(
    spark: SparkSession,
    df: DataFrame,
    checkpoint_dir: str,
    eager: bool = True,
) -> DataFrame:
    """
    Checkpoint a DataFrame to break the lineage graph.

    Unlike cache(), checkpoint writes to a reliable store (DBFS / S3) and
    truncates the RDD lineage.  Use when:
      - The DataFrame has a very long lineage (iterative algorithms).
      - You want to recover from executor failure without recomputing the full chain.

    Parameters
    ----------
    spark : SparkSession
    df : DataFrame
    checkpoint_dir : str
        Cloud path for checkpoint files.  Example: 'dbfs:/tmp/checkpoints/my_df'.
    eager : bool
        Materialise the checkpoint immediately (default True).

    Returns
    -------
    DataFrame
        Checkpointed DataFrame with truncated lineage.
    """
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    return df.checkpoint(eager=eager)


def estimate_cache_size_mb(
    df: DataFrame,
    sample_fraction: float = 0.01,
) -> float:
    """
    Estimate the uncompressed in-memory size of a DataFrame in MB.

    Samples the DataFrame, serialises the sample, and extrapolates.
    Rough estimate — actual Spark cache size differs due to encoder overhead.

    Parameters
    ----------
    df : DataFrame
    sample_fraction : float
        Fraction of rows to sample.  Larger fraction = better estimate, more cost.

    Returns
    -------
    float
        Estimated uncompressed size in MB.

    Notes
    -----
    This estimate is coarse.  For accurate figures, cache the DataFrame and
    inspect the Spark UI Storage tab or use df.storageLevel after persist().
    """
    import sys  # noqa: PLC0415
    sample = df.sample(withReplacement=False, fraction=min(sample_fraction, 1.0))
    rows = sample.collect()
    if not rows:
        return 0.0
    sample_bytes = sys.getsizeof(rows)
    total_estimated_bytes = sample_bytes / sample_fraction
    return round(total_estimated_bytes / (1024 * 1024), 2)
