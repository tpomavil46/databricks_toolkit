"""
performance/aqe_sql.py — Spark Configuration SET Statements.

Module   : performance.aqe_sql
Concept  : Generate SET statements that tune Spark's runtime behaviour
When     : A notebook or job needs custom tuning for a specific query shape.
           Set session-level configs (SET spark.xxx = yyy) for one query;
           set cluster configs in the cluster policy for all workloads.

Adaptive Query Execution (AQE) — default in Databricks
--------------------------------------------------------
AQE re-optimises the query plan at runtime using actual shuffle statistics.
Three capabilities:

1. Coalesce shuffle partitions
   Merges small post-shuffle partitions to avoid tiny task overhead.
   spark.sql.adaptive.coalescePartitions.enabled (default: true)
   spark.sql.adaptive.advisoryPartitionSizeInBytes (target merge size, default 64MB)

2. Skew join optimisation
   Splits skewed shuffle partitions across multiple tasks.
   spark.sql.adaptive.skewJoin.enabled (default: true)
   spark.sql.adaptive.skewJoin.skewedPartitionFactor (default: 5)
   spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes (default: 256MB)

3. Switch join strategies dynamically
   Can convert sort-merge join → broadcast join when AQE discovers the right
   side is smaller than expected after filtering.
   spark.sql.adaptive.localShuffleReader.enabled (default: true)

Shuffle partitions
------------------
spark.sql.shuffle.partitions (default: 200)
  Too low  → large partitions → OOM / slow tasks.
  Too high → tiny partitions → excessive task overhead / small files.
  Rule of thumb: target 100–200 MB per partition.
  AQE coalesces at runtime, so setting this high (e.g. 2000) and letting AQE
  merge is safe and recommended.

Public API
----------
build_set_sql(key, value) -> str
build_enable_aqe_sql() -> str
build_disable_aqe_sql() -> str
build_set_shuffle_partitions_sql(n) -> str
build_set_broadcast_threshold_sql(bytes) -> str
build_set_advisory_partition_size_sql(bytes) -> str
build_set_skew_factor_sql(factor) -> str
build_aqe_tuning_block(shuffle_partitions, advisory_mb, skew_factor) -> list[str]
"""

from __future__ import annotations

from typing import List, Optional


def build_set_sql(key: str, value: str) -> str:
    """
    Generate a Spark SQL SET statement.

    Parameters
    ----------
    key : str
        Spark config key.
    value : str
        Config value (always a string in SET syntax).

    Returns
    -------
    str
    """
    return f"SET {key} = {value}"


def build_enable_aqe_sql() -> str:
    """Enable Adaptive Query Execution (default in Databricks)."""
    return build_set_sql("spark.sql.adaptive.enabled", "true")


def build_disable_aqe_sql() -> str:
    """
    Disable AQE.

    Rarely necessary.  Consider disabling only when benchmarking deterministic
    query plans or when AQE is producing unstable plans.
    """
    return build_set_sql("spark.sql.adaptive.enabled", "false")


def build_set_shuffle_partitions_sql(n: int) -> str:
    """
    Set the number of shuffle partitions.

    Parameters
    ----------
    n : int
        Partition count.  With AQE enabled, set this high (1000–4000) and let
        AQE coalesce.  Without AQE, tune to ~200 MB per partition.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If n < 1.
    """
    if n < 1:
        raise ValueError(f"shuffle partitions must be >= 1, got {n}")
    return build_set_sql("spark.sql.shuffle.partitions", str(n))


def build_set_broadcast_threshold_sql(bytes_threshold: int) -> str:
    """
    Set the broadcast join threshold in bytes.

    Spark broadcasts tables smaller than this threshold in join operations.
    Default: 10 MB (10_485_760 bytes).  Databricks clusters often set this higher.

    Parameters
    ----------
    bytes_threshold : int
        Threshold in bytes.  Common values: 10_485_760 (10MB), 104_857_600 (100MB).
        Set to -1 to disable broadcast joins.

    Returns
    -------
    str
    """
    return build_set_sql("spark.sql.autoBroadcastJoinThreshold", str(bytes_threshold))


def build_set_advisory_partition_size_sql(bytes_size: int) -> str:
    """
    Set the AQE advisory partition size for coalescing.

    AQE merges shuffle partitions until they reach approximately this size.
    Default: 67_108_864 (64 MB).  Increase for large aggregations.

    Parameters
    ----------
    bytes_size : int

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If bytes_size < 1.
    """
    if bytes_size < 1:
        raise ValueError(f"bytes_size must be >= 1, got {bytes_size}")
    return build_set_sql("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(bytes_size))


def build_set_skew_factor_sql(factor: float) -> str:
    """
    Set the AQE skew join detection factor.

    A partition is considered skewed when its size > factor × median partition size.
    Default: 5.  Lower values detect more partitions as skewed (may over-split).

    Parameters
    ----------
    factor : float
        Must be >= 1.0.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If factor < 1.0.
    """
    if factor < 1.0:
        raise ValueError(f"skew factor must be >= 1.0, got {factor}")
    return build_set_sql("spark.sql.adaptive.skewJoin.skewedPartitionFactor", str(factor))


def build_enable_coalesce_partitions_sql() -> str:
    """Enable AQE partition coalescing (default: true)."""
    return build_set_sql("spark.sql.adaptive.coalescePartitions.enabled", "true")


def build_aqe_tuning_block(
    shuffle_partitions: int = 2000,
    advisory_mb: int = 128,
    skew_factor: float = 5.0,
) -> List[str]:
    """
    Return a set of SET statements that configure AQE for production workloads.

    Parameters
    ----------
    shuffle_partitions : int
        Max shuffle partitions (AQE will coalesce down from this).
    advisory_mb : int
        Target merged partition size in MB.
    skew_factor : float
        Skew detection multiplier.

    Returns
    -------
    list[str]
        Ordered list of SET SQL statements.
    """
    advisory_bytes = advisory_mb * 1024 * 1024
    return [
        build_enable_aqe_sql(),
        build_set_shuffle_partitions_sql(shuffle_partitions),
        build_set_advisory_partition_size_sql(advisory_bytes),
        build_set_skew_factor_sql(skew_factor),
        build_enable_coalesce_partitions_sql(),
    ]
