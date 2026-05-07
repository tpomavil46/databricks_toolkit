"""
delta_features/cdf_pyspark.py — Change Data Feed (PySpark).

Module   : delta_features.cdf_pyspark
Concept  : Read only the rows that changed between two Delta versions
When     : Propagating changes from one Delta table to a downstream consumer
           without re-scanning the full table every run.

What it is
----------
Change Data Feed (CDF) is a Delta feature that records all row-level changes
(inserts, updates, deletes) in a separate change log alongside the main table data.
When CDF is enabled, each write creates change records tagged with:
  _change_type:       'insert', 'update_preimage', 'update_postimage', 'delete'
  _commit_version:    Delta table version of the write.
  _commit_timestamp:  Wall-clock time of the commit.

Reading the change log with readChangeFeed = true gives you only the rows that
changed between two versions — far more efficient than reading the full table
and detecting changes with a hash column.

Change types
------------
insert          New row added (INSERT, INSERT INTO, COPY INTO, MERGE NOT MATCHED).
update_preimage Row values BEFORE an update (the old state).
update_postimage Row values AFTER an update (the new state).
delete          Row that was deleted.

Note: MERGE produces all three change types in one batch:
  - Matched rows that changed: update_preimage + update_postimage
  - New rows: insert
  - Deleted rows (if MERGE has DELETE clause): delete

When to use
-----------
- Bronze → Silver propagation: read CDF from Bronze to apply targeted upserts
  to Silver rather than full table scans.
- Audit trails: capture every row state change with timestamps and versions.
- Downstream CDC: feed changes to Kafka, another Delta table, or an external system.
- Incremental aggregation refresh: detect only the rows that changed and
  recompute affected aggregation buckets.

When NOT to use
---------------
- Source table has no CDF enabled: enable it first, then the feed starts from
  the next committed version (no history before enablement).
- You need the full current state: read the table normally (not CDF).
- Ultra-low latency: CDF is batch / micro-batch; for per-record latency use
  a proper message queue with Kafka or Event Hubs.

Delta / Databricks considerations
----------------------------------
- Enable CDF: ALTER TABLE <t> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
  Or at creation: CREATE TABLE ... TBLPROPERTIES (delta.enableChangeDataFeed = true)
- CDF adds storage overhead (change records are stored in _change_data/ subdirectory).
  Estimated: 10–20% overhead relative to the write volume.
- startingVersion 0 starts from the beginning of the table's history.
- startingTimestamp is resolved to the version committed AT OR AFTER the timestamp.
  (Opposite of time travel, which resolves to AT OR BEFORE.)
- Streaming CDF automatically advances the starting version on each micro-batch.
- Requires DBR 8.4+ / Delta Lake 2.0+.

Public API
----------
build_cdf_stream_options(start_version, start_timestamp) -> dict
build_cdf_batch_options(start_version, end_version, start_timestamp) -> dict
read_cdf_stream(spark, table, start_version, start_timestamp) -> DataFrame
read_cdf_batch(spark, table, start_version, end_version) -> DataFrame
filter_by_change_type(df, change_types) -> DataFrame
split_cdf_by_change_type(df) -> dict[str, DataFrame]
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CDF_CHANGE_TYPE_COL = "_change_type"
CDF_COMMIT_VERSION_COL = "_commit_version"
CDF_COMMIT_TIMESTAMP_COL = "_commit_timestamp"

INSERT = "insert"
UPDATE_PREIMAGE = "update_preimage"
UPDATE_POSTIMAGE = "update_postimage"
DELETE = "delete"

ALL_CHANGE_TYPES = {INSERT, UPDATE_PREIMAGE, UPDATE_POSTIMAGE, DELETE}


def build_cdf_stream_options(
    start_version: Optional[int] = None,
    start_timestamp: Optional[str] = None,
) -> dict:
    """
    Return readStream options dict for Change Data Feed streaming read.

    Exactly one of start_version or start_timestamp should be provided.
    If neither is set, the stream reads from the latest version (no backfill).

    Parameters
    ----------
    start_version : int | None
        Delta version to start reading from (inclusive).
        0 = start from the beginning of the table history.
    start_timestamp : str | None
        ISO 8601 timestamp.  Stream starts from the first version committed
        AT OR AFTER this timestamp.

    Returns
    -------
    dict
        Options dict for spark.readStream.format("delta").options(**opts).

    Examples
    --------
        opts = build_cdf_stream_options(start_version=42)
        df = spark.readStream.format("delta").options(**opts).table("catalog.bronze.events")
    """
    opts: dict = {"readChangeFeed": "true"}
    if start_version is not None:
        opts["startingVersion"] = str(start_version)
    if start_timestamp is not None:
        opts["startingTimestamp"] = start_timestamp
    return opts


def build_cdf_batch_options(
    start_version: int,
    end_version: Optional[int] = None,
    start_timestamp: Optional[str] = None,
) -> dict:
    """
    Return spark.read options dict for a batch (non-streaming) CDF read.

    Parameters
    ----------
    start_version : int
        First version to include (inclusive).
    end_version : int | None
        Last version to include (inclusive).  None = read through the latest.
    start_timestamp : str | None
        Alternative to start_version: start at the version AT OR AFTER this timestamp.

    Returns
    -------
    dict
    """
    opts: dict = {"readChangeFeed": "true"}
    if start_timestamp is not None:
        opts["startingTimestamp"] = start_timestamp
    else:
        opts["startingVersion"] = str(start_version)
    if end_version is not None:
        opts["endingVersion"] = str(end_version)
    return opts


def read_cdf_stream(
    spark: SparkSession,
    table: str,
    start_version: Optional[int] = None,
    start_timestamp: Optional[str] = None,
) -> DataFrame:
    """
    Open a Delta table's Change Data Feed as a streaming DataFrame.

    Parameters
    ----------
    spark : SparkSession
    table : str
        Fully-qualified Delta table name.  CDF must be enabled on this table.
    start_version : int | None
        Delta version to start from (inclusive).
    start_timestamp : str | None
        Timestamp to start from.

    Returns
    -------
    DataFrame
        Streaming DataFrame with _change_type, _commit_version, _commit_timestamp
        columns appended to the table's normal columns.
    """
    opts = build_cdf_stream_options(start_version, start_timestamp)
    reader = spark.readStream.format("delta")
    for k, v in opts.items():
        reader = reader.option(k, v)
    return reader.table(table)


def read_cdf_batch(
    spark: SparkSession,
    table: str,
    start_version: int,
    end_version: Optional[int] = None,
) -> DataFrame:
    """
    Read a Delta table's Change Data Feed as a batch DataFrame.

    Use for scheduled backfill or one-shot propagation between two versions.

    Parameters
    ----------
    spark : SparkSession
    table : str
    start_version : int
    end_version : int | None
        Inclusive end version.  None = read through the latest version.

    Returns
    -------
    DataFrame
    """
    opts = build_cdf_batch_options(start_version, end_version)
    reader = spark.read.format("delta")
    for k, v in opts.items():
        reader = reader.option(k, v)
    return reader.table(table)


def filter_by_change_type(
    df: DataFrame,
    change_types: List[str],
) -> DataFrame:
    """
    Filter a CDF DataFrame to rows with the specified change types.

    Parameters
    ----------
    df : DataFrame
        CDF DataFrame with _change_type column.
    change_types : list[str]
        Subset of: 'insert', 'update_preimage', 'update_postimage', 'delete'.

    Returns
    -------
    DataFrame

    Examples
    --------
        # Only current state (post-update and new inserts)
        df_current = filter_by_change_type(df_cdf, ["insert", "update_postimage"])
    """
    return df.filter(F.col(CDF_CHANGE_TYPE_COL).isin(change_types))


def split_cdf_by_change_type(df: DataFrame) -> Dict[str, DataFrame]:
    """
    Split a CDF DataFrame into one DataFrame per change type.

    Parameters
    ----------
    df : DataFrame
        CDF DataFrame.

    Returns
    -------
    dict[str, DataFrame]
        Keys: 'insert', 'update_preimage', 'update_postimage', 'delete'.
        Values: filtered DataFrames (may be empty if no rows of that type).
    """
    return {ct: filter_by_change_type(df, [ct]) for ct in ALL_CHANGE_TYPES}


def drop_cdf_metadata_cols(df: DataFrame) -> DataFrame:
    """
    Remove the _change_type, _commit_version, _commit_timestamp columns.

    Call this before writing CDF data to a target that uses the same schema
    as the source table (those columns don't belong in the target).

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    DataFrame
    """
    cols_to_drop = [CDF_CHANGE_TYPE_COL, CDF_COMMIT_VERSION_COL, CDF_COMMIT_TIMESTAMP_COL]
    existing = [c for c in cols_to_drop if c in df.columns]
    return df.drop(*existing)
