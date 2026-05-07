"""
upserts/late_arriving_pyspark.py — Late-Arriving Data Handling (PySpark).

Module   : upserts.late_arriving_pyspark
Concept  : Upsert patterns that correctly handle out-of-order and late records
When     : Source delivers records with timestamps that may be older than what
           is already in the target — event-driven pipelines, IoT feeds,
           mobile app sync, retry queues.
Author   : <your name>
Version  : 1.0.0

What it is
----------
Late-arriving data occurs when a record for an event that happened at T=10:00
arrives in the pipeline at T=11:30, after records for T=10:30 and T=11:00
have already been processed.

A naive MERGE would overwrite the more-recent T=10:30 record with the stale
T=10:00 values.  Two defenses:

1. Deduplication before MERGE (pick_latest_per_key)
   When a single batch contains multiple records for the same key, keep only
   the most recent by event_ts.  This is the same pattern as idempotent dedup,
   but the driver is recency rather than idempotency.

2. Recency guard in MERGE (apply_late_arriving_merge)
   In the MERGE UPDATE clause, add a condition: only update when the source
   record is AS RECENT OR MORE RECENT than the target.
   WHEN MATCHED AND s.event_ts >= t.event_ts THEN UPDATE SET *

   This means: if a late record arrives after a more-recent record has already
   been applied, the MERGE silently skips the update for that key.

When to use
-----------
- Event-driven sources (Kafka, mobile devices, IoT sensors) where message
  ordering is not guaranteed and retries can deliver duplicates.
- Multi-region or multi-source pipelines where the same entity can be updated
  from several places simultaneously.
- Any pipeline that re-processes historical data (backfills) alongside live data.

When NOT to use
---------------
- Source is guaranteed to be in order and idempotent → basic_merge (simpler).
- You need full history of all versions (even late ones) → SCD Type 2 inserts a
  new version row regardless of order; querying by effective date handles recency.

Trade-offs
----------
Late records are silently ignored.  If you need to audit which late records
were dropped, log the pre-MERGE source minus the post-MERGE deltas.

Delta / Databricks considerations
----------------------------------
- The recency guard condition is applied per-row in the MERGE executor.
  Delta evaluates the condition on matched rows before writing.
- Combining with deduplication: always deduplicate first, then apply the
  recency guard.  Dedup ensures one source row per key; the guard ensures
  that row is only applied if it's not stale.
- For streaming late-arriving data, use Structured Streaming watermarks
  (see streaming/stateful_pyspark.py) instead of batch guards.
- Requires DBR 8.0+ / Delta Lake 1.0+.

Public API
----------
pick_latest_per_key(df, merge_keys, ts_col) -> DataFrame
    Pure transform: one row per key, most recent by ts_col.

apply_late_arriving_merge(spark, df_source, config) -> None
    Dedup + MERGE with recency guard.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from upserts.config import UpsertConfig


def pick_latest_per_key(
    df: DataFrame,
    merge_keys: List[str],
    ts_col: str,
) -> DataFrame:
    """
    Return one row per merge_key combination: the row with the latest ts_col.

    Equivalent to:
        SELECT * EXCEPT(_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY keys ORDER BY ts DESC) AS _rn
            FROM df
        ) WHERE _rn = 1

    Parameters
    ----------
    df : DataFrame
    merge_keys : list[str]
    ts_col : str
        Timestamp or sequence column.  Higher = more recent.

    Returns
    -------
    DataFrame
        One row per merge_key combination, the most recent by ts_col.
    """
    w = Window.partitionBy(*merge_keys).orderBy(F.col(ts_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def apply_late_arriving_merge(
    spark: SparkSession,
    df_source: DataFrame,
    config: UpsertConfig,
) -> None:
    """
    Apply late-arriving data MERGE: dedup source + recency-guarded UPDATE.

    Steps:
    1. Deduplicate df_source on merge_keys by event_ts_col (latest wins).
    2. MERGE with:
       - WHEN MATCHED AND s.event_ts >= t.event_ts THEN UPDATE SET *
         (skip the update if source is older than what's in the target)
       - WHEN NOT MATCHED THEN INSERT *
         (always insert new entities regardless of timestamp)

    Parameters
    ----------
    spark : SparkSession
    df_source : DataFrame
        May contain duplicate merge keys and out-of-order records.
    config : UpsertConfig
        event_ts_col must be set to the timestamp column name.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If config.event_ts_col is not set.

    Examples
    --------
        config = UpsertConfig(
            source_table="catalog.bronze.events",
            target_table="catalog.gold.dim_device",
            merge_keys=["device_id"],
            event_ts_col="event_ts",
        )

        apply_late_arriving_merge(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    if not config.event_ts_col:
        raise ValueError(
            "UpsertConfig.event_ts_col must be set for late-arriving merge"
        )

    df_deduped = pick_latest_per_key(df_source, config.merge_keys, config.event_ts_col)

    join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.merge_keys)
    recency_cond = f"s.{config.event_ts_col} >= t.{config.event_ts_col}"

    (
        DeltaTable.forName(spark, config.target_table)
        .alias("t")
        .merge(df_deduped.alias("s"), join_expr)
        .whenMatchedUpdateAll(condition=recency_cond)
        .whenNotMatchedInsertAll()
        .execute()
    )
