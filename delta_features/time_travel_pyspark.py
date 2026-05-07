"""
delta_features/time_travel_pyspark.py — Delta Time Travel (PySpark).

Module   : delta_features.time_travel_pyspark
Concept  : Read historical versions of a Delta table via the PySpark read API
When     : Auditing, debugging pipeline regressions, comparing current vs past
           state, or recovering data overwritten by a bad write.

What it is
----------
Every write to a Delta table creates a new transaction log entry and increments
the table version.  Time travel lets you read any past version by:
  - Version number: spark.read.option("versionAsOf", 5).table(...)
  - Timestamp: spark.read.option("timestampAsOf", "2024-01-15").table(...)

The historical data is available as long as VACUUM has not removed the
underlying Parquet files.  Default retention: 30 days (VACUUM RETAIN 720 HOURS).

When to use
-----------
- Debug: compare current data with data at a known-good version.
- Audit: prove what data existed at a specific point in time.
- Recovery: read the old version, write it back to "undo" a bad operation.
- Testing: seed test data from a stable historical snapshot.

When NOT to use
---------------
- Real-time CDC: use Change Data Feed instead (reads only the delta between
  two versions, not the full snapshot).
- Cross-table historical joins: time travel is per-table; if you need
  multi-table historical consistency, use Delta snapshots at the same version
  only if tables are part of the same pipeline run.

Delta / Databricks considerations
----------------------------------
- versionAsOf: integer version (0 = table creation).
- timestampAsOf: ISO 8601 string ('2024-01-15 00:00:00') or date string.
  Resolves to the latest version AT OR BEFORE the timestamp.
- DESCRIBE HISTORY <table>: shows all versions with timestamp, operation,
  user, and job info.
- RESTORE TABLE: rolls the table back to a historical state (modifies current
  version — irreversible without another restore).
- Requires DBR 7.0+ / Delta Lake 0.7+.

Public API
----------
build_version_read_options(version) -> dict
build_timestamp_read_options(timestamp) -> dict
read_at_version(spark, table, version) -> DataFrame
read_at_timestamp(spark, table, timestamp) -> DataFrame
compare_versions(spark, table, version_a, version_b, key_cols) -> DataFrame
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession


def build_version_read_options(version: int) -> dict:
    """
    Return .option() kwargs for reading a Delta table at a specific version.

    Parameters
    ----------
    version : int
        Delta table version number (0 = table creation).

    Returns
    -------
    dict
        {'versionAsOf': '<version>'}

    Examples
    --------
        opts = build_version_read_options(42)
        df = spark.read.format("delta").options(**opts).table("catalog.gold.dim_customer")
    """
    return {"versionAsOf": str(version)}


def build_timestamp_read_options(timestamp: str) -> dict:
    """
    Return .option() kwargs for reading a Delta table at a specific timestamp.

    Parameters
    ----------
    timestamp : str
        ISO 8601 timestamp or date string.  Resolves to the latest version
        committed AT OR BEFORE this timestamp.
        Examples: '2024-01-15', '2024-01-15 12:00:00', '2024-01-15T12:00:00Z'.

    Returns
    -------
    dict
        {'timestampAsOf': '<timestamp>'}
    """
    return {"timestampAsOf": timestamp}


def read_at_version(
    spark: SparkSession,
    table: str,
    version: int,
) -> DataFrame:
    """
    Read a Delta table at a specific version number.

    Parameters
    ----------
    spark : SparkSession
    table : str
        Fully-qualified Delta table name.
    version : int
        Version number to read.

    Returns
    -------
    DataFrame
        Snapshot of the table at the specified version.

    Notes
    -----
    Reading a version that has been vacuumed raises VersionNotFoundException.
    Check DESCRIBE HISTORY to see which versions are still available.
    """
    return (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .table(table)
    )


def read_at_timestamp(
    spark: SparkSession,
    table: str,
    timestamp: str,
) -> DataFrame:
    """
    Read a Delta table as it existed at a specific timestamp.

    Parameters
    ----------
    spark : SparkSession
    table : str
        Fully-qualified Delta table name.
    timestamp : str
        ISO 8601 timestamp string.

    Returns
    -------
    DataFrame
        Snapshot of the table at the latest version before the given timestamp.
    """
    return (
        spark.read.format("delta")
        .option("timestampAsOf", timestamp)
        .table(table)
    )


def compare_versions(
    spark: SparkSession,
    table: str,
    version_a: int,
    version_b: int,
    key_cols: List[str],
) -> DataFrame:
    """
    Return rows that exist in version_b but differ from version_a on any column.

    Useful for auditing what changed between two pipeline runs.

    Parameters
    ----------
    spark : SparkSession
    table : str
    version_a : int
        Baseline version (e.g., yesterday's run).
    version_b : int
        Comparison version (e.g., today's run).
    key_cols : list[str]
        Join columns to align rows across versions.

    Returns
    -------
    DataFrame
        Rows from version_b whose content differs from version_a.
        Columns from version_a are prefixed 'prev_', version_b are unprefixed.
    """
    from pyspark.sql import functions as F

    df_a = read_at_version(spark, table, version_a)
    df_b = read_at_version(spark, table, version_b)

    payload_cols = [c for c in df_b.columns if c not in key_cols]

    df_a_renamed = df_a.select(
        *key_cols,
        *[F.col(c).alias(f"prev_{c}") for c in payload_cols],
    )

    df_joined = df_b.join(df_a_renamed, on=key_cols, how="left")

    change_condition = None
    for c in payload_cols:
        cond = ~F.col(c).eqNullSafe(F.col(f"prev_{c}"))
        change_condition = cond if change_condition is None else change_condition | cond

    return df_joined.filter(change_condition) if change_condition is not None else df_joined
