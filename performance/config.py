"""
performance/config.py — Recommended Spark Configuration Bundles.

Module   : performance.config
Concept  : Curated sets of Spark configs for common workload patterns
When     : Setting up a cluster policy, job cluster, or DLT pipeline config.

Each bundle returns a plain dict of {spark_key: value} pairs suitable for:
  - cluster_spec["spark_conf"] in a job cluster definition
  - DLT pipeline cluster spark_conf
  - spark.conf.set() calls in a notebook

How to apply
------------
    conf = streaming_workload_config()
    for k, v in conf.items():
        spark.conf.set(k, v)   # session-level (notebook)
    # or embed in cluster spec:
    cluster_spec["spark_conf"] = conf

Public API
----------
streaming_workload_config(checkpoint_interval_ms) -> dict
batch_etl_config(shuffle_partitions, broadcast_threshold_mb) -> dict
ml_training_config() -> dict
high_concurrency_config() -> dict
merge_all_configs(*configs) -> dict
"""

from __future__ import annotations

from typing import Dict


def streaming_workload_config(
    checkpoint_interval_ms: int = 30_000,
) -> Dict[str, str]:
    """
    Recommended Spark configs for Structured Streaming workloads.

    Focuses on stability and low latency over throughput.

    Parameters
    ----------
    checkpoint_interval_ms : int
        Interval between checkpoints (milliseconds).  Lower = more durable,
        more overhead.  30 s is a good default.

    Returns
    -------
    dict
    """
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "false",
        "spark.sql.streaming.stateStore.providerClass":
            "com.databricks.sql.acl.delta.DeltaStateStoreProvider",
        "spark.sql.streaming.metricsEnabled": "true",
        f"spark.sql.streaming.checkpointInterval": str(checkpoint_interval_ms),
    }


def batch_etl_config(
    shuffle_partitions: int = 2000,
    broadcast_threshold_mb: int = 100,
) -> Dict[str, str]:
    """
    Recommended Spark configs for large batch ETL jobs.

    Prioritises throughput: high shuffle parallelism + AQE coalescing.

    Parameters
    ----------
    shuffle_partitions : int
        Starting partition count.  AQE will merge small partitions down.
    broadcast_threshold_mb : int
        Auto-broadcast join threshold in MB.

    Returns
    -------
    dict
    """
    broadcast_bytes = broadcast_threshold_mb * 1024 * 1024
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(128 * 1024 * 1024),
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.sql.autoBroadcastJoinThreshold": str(broadcast_bytes),
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
    }


def ml_training_config() -> Dict[str, str]:
    """
    Recommended configs for ML training workloads (feature engineering + training).

    Focuses on memory efficiency and off-heap to reduce GC pressure.

    Returns
    -------
    dict
    """
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": "1000",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        "spark.sql.autoBroadcastJoinThreshold": str(50 * 1024 * 1024),
        "spark.databricks.delta.optimizeWrite.enabled": "true",
    }


def high_concurrency_config() -> Dict[str, str]:
    """
    Recommended configs for High Concurrency / SQL Analytics clusters.

    Limits per-query resource usage to ensure fair sharing between concurrent users.

    Returns
    -------
    dict
    """
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.databricks.preemption.enabled": "true",
        "spark.task.cpus": "1",
        "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
        "spark.databricks.passthrough.enabled": "true",
    }


def merge_all_configs(*configs: Dict[str, str]) -> Dict[str, str]:
    """
    Merge multiple config dicts, last writer wins on conflicts.

    Parameters
    ----------
    *configs : dict
        Config dicts to merge in order.

    Returns
    -------
    dict
    """
    merged: Dict[str, str] = {}
    for cfg in configs:
        merged.update(cfg)
    return merged
