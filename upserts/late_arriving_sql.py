"""
upserts/late_arriving_sql.py — Late-Arriving Data Handling (Spark SQL).

Module   : upserts.late_arriving_sql
Concept  : Recency-guarded MERGE for out-of-order data, SQL implementation
When     : Same as late_arriving_pyspark.  SQL variant for DLT pipelines and
           audit-friendly query history.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from upserts.config import UpsertConfig


def _join_condition(merge_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in merge_keys)


def build_late_arriving_merge_sql(config: UpsertConfig) -> str:
    """
    Generate a MERGE INTO SQL that deduplicates the source and applies a
    recency guard so that stale late-arriving records are silently skipped.

    The source is deduplicated via an inline CTE using ROW_NUMBER().
    The WHEN MATCHED UPDATE clause adds: AND s.event_ts >= t.event_ts.

    Parameters
    ----------
    config : UpsertConfig
        event_ts_col must be set.
        merge_keys drive both the dedup partition and the MERGE ON condition.

    Returns
    -------
    str
        MERGE INTO SQL string.

    Raises
    ------
    ValueError
        If config.event_ts_col is not set.
    """
    if not config.event_ts_col:
        raise ValueError("UpsertConfig.event_ts_col must be set for late-arriving merge")

    join_cond = _join_condition(config.merge_keys)
    partition_cols = ", ".join(config.merge_keys)
    ts = config.event_ts_col

    return f"""MERGE INTO {config.target_table} AS t
USING (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY {partition_cols}
        ORDER BY {ts} DESC
      ) AS _rn
    FROM {config.source_table}
  )
  WHERE _rn = 1
) AS s
ON {join_cond}
WHEN MATCHED AND s.{ts} >= t.{ts} THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def build_sequence_guard_merge_sql(config: UpsertConfig, seq_col: str) -> str:
    """
    Generate a MERGE INTO SQL with a sequence-number recency guard.

    Instead of a timestamp, uses a monotonically increasing sequence number
    (e.g. Kafka offset, database sequence, CDC LSN) as the recency signal.
    Only updates when source sequence number is GREATER than target.

    Parameters
    ----------
    config : UpsertConfig
    seq_col : str
        Column carrying the sequence number.  Must be present in both source
        and target.

    Returns
    -------
    str
        MERGE INTO SQL with sequence-number guard.
    """
    join_cond = _join_condition(config.merge_keys)
    partition_cols = ", ".join(config.merge_keys)
    return f"""MERGE INTO {config.target_table} AS t
USING (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY {partition_cols}
        ORDER BY {seq_col} DESC
      ) AS _rn
    FROM {config.source_table}
  )
  WHERE _rn = 1
) AS s
ON {join_cond}
WHEN MATCHED AND s.{seq_col} > t.{seq_col} THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def apply_late_arriving_merge_sql(spark: SparkSession, config: UpsertConfig) -> None:
    """
    Execute the late-arriving MERGE against the live Delta target table.

    Parameters
    ----------
    spark : SparkSession
    config : UpsertConfig
        event_ts_col must be set.

    Returns
    -------
    None
    """
    spark.sql(build_late_arriving_merge_sql(config))
