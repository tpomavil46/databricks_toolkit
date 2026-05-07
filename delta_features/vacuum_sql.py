"""
delta_features/vacuum_sql.py — VACUUM (Delta File Retention).

Module   : delta_features.vacuum_sql
Concept  : Remove stale Parquet files that are no longer referenced by Delta
When     : Regularly, as part of pipeline maintenance — typically daily or weekly.
           VACUUM is the only way to reclaim cloud storage on Delta tables.

What it is
----------
Delta's MVCC keeps old Parquet files on disk so that:
1. Time travel works (read any historical version).
2. Concurrent reads are not interrupted when a MERGE or DELETE runs.

Without VACUUM, old files accumulate indefinitely — your cloud storage bill
grows even though the logical table size stays the same.

VACUUM removes all files that:
- Are not referenced by the current Delta snapshot, AND
- Are older than the retention threshold (default: 7 days / 168 hours).

VACUUM is a destructive operation: files it deletes cannot be recovered
without restoring from an external backup.

Retention trade-offs
--------------------
Longer retention (720h = 30 days default):
  + More time-travel history available.
  + Safer for long-running streaming jobs (their offsets may reference old files).
  − Higher storage cost.

Shorter retention (e.g., 168h = 7 days):
  + Less storage cost.
  − Less time-travel history.
  − May break long-running streaming jobs if their checkpoint references old files.

Minimum safe retention
-----------------------
Do NOT set retention below 7 days (168 hours) unless you:
1. Disable the Delta retention check: SET spark.databricks.delta.retentionDurationCheck.enabled = false
2. Verified that no concurrent readers or streaming jobs reference older snapshots.
3. Accept that time travel is unavailable for the removed versions.

Running VACUUM DRY RUN first shows which files would be deleted without
actually deleting them — always recommended before running with short retention.

Interaction with streaming checkpoints
---------------------------------------
If a streaming job using a Delta source fails and is down for longer than the
VACUUM retention window, its checkpoint may reference files that VACUUM has
already deleted.  The stream will fail to restart.
Solution: set retention >= 2× the maximum expected stream downtime.

Public API
----------
build_vacuum_sql(table, retain_hours, dry_run) -> str
build_disable_retention_check_sql() -> str
validate_retention_hours(hours) -> None
apply_vacuum(spark, table, retain_hours, dry_run) -> None
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

MINIMUM_RETENTION_HOURS = 168
DEFAULT_RETENTION_HOURS = 720


def validate_retention_hours(hours: Optional[float]) -> None:
    """
    Raise ValueError if retention is below the safe minimum.

    Delta enforces a minimum of 168 hours (7 days) by default.  To override,
    you must first disable the check (see build_disable_retention_check_sql).

    Parameters
    ----------
    hours : float | None
        Proposed retention in hours.  None means "use Delta's default" — safe.

    Raises
    ------
    ValueError
        If hours < MINIMUM_RETENTION_HOURS.
    """
    if hours is not None and hours < MINIMUM_RETENTION_HOURS:
        raise ValueError(
            f"Retention {hours}h is below the Delta minimum of {MINIMUM_RETENTION_HOURS}h "
            f"(7 days). This would remove files needed for time travel and may break "
            f"streaming checkpoints. To override, first run: "
            f"SET spark.databricks.delta.retentionDurationCheck.enabled = false"
        )


def build_vacuum_sql(
    table: str,
    retain_hours: Optional[float] = None,
    dry_run: bool = False,
) -> str:
    """
    Generate VACUUM SQL to remove stale Delta files.

    Parameters
    ----------
    table : str
        Fully-qualified Delta table name.
    retain_hours : float | None
        Retention threshold in hours.  Files older than this are candidates for
        deletion.  None = use Delta's configured default (delta.deletedFileRetentionDuration,
        default 7 days / 168 hours).
    dry_run : bool
        When True, generates VACUUM ... DRY RUN which lists files that would be
        deleted without actually deleting them.  Always run dry first.

    Returns
    -------
    str
        VACUUM SQL statement.

    Raises
    ------
    ValueError
        If retain_hours < MINIMUM_RETENTION_HOURS (168).

    Examples
    --------
        # Safe production vacuum (uses table's configured retention)
        build_vacuum_sql("catalog.gold.dim_customer")
        # VACUUM catalog.gold.dim_customer

        # Explicit 14-day retention
        build_vacuum_sql("catalog.gold.dim_customer", retain_hours=336)
        # VACUUM catalog.gold.dim_customer RETAIN 336 HOURS

        # Dry run first
        build_vacuum_sql("catalog.gold.dim_customer", retain_hours=168, dry_run=True)
        # VACUUM catalog.gold.dim_customer RETAIN 168 HOURS DRY RUN
    """
    validate_retention_hours(retain_hours)

    sql = f"VACUUM {table}"
    if retain_hours is not None:
        sql += f" RETAIN {retain_hours} HOURS"
    if dry_run:
        sql += " DRY RUN"
    return sql


def build_disable_retention_check_sql() -> str:
    """
    Generate SET statement to disable Delta's minimum retention enforcement.

    Use only when you intentionally need a retention below 7 days — for example,
    in development environments where storage costs matter and time travel is
    not needed.

    IMPORTANT: Reset this to true after running VACUUM.

    Returns
    -------
    str
        SET spark.databricks.delta.retentionDurationCheck.enabled = false
    """
    return "SET spark.databricks.delta.retentionDurationCheck.enabled = false"


def build_enable_retention_check_sql() -> str:
    """
    Generate SET statement to re-enable Delta's minimum retention enforcement.

    Returns
    -------
    str
    """
    return "SET spark.databricks.delta.retentionDurationCheck.enabled = true"


def build_set_retention_property_sql(table: str, retain_hours: float) -> str:
    """
    Generate ALTER TABLE to set the table-level retention duration.

    This persists the retention preference in the table's Delta properties,
    so every subsequent VACUUM respects it without needing an explicit RETAIN clause.

    Parameters
    ----------
    table : str
    retain_hours : float

    Returns
    -------
    str
    """
    validate_retention_hours(retain_hours)
    days = retain_hours / 24
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.deletedFileRetentionDuration' = 'interval {days} days')"
    )


def apply_vacuum(
    spark: SparkSession,
    table: str,
    retain_hours: Optional[float] = None,
    dry_run: bool = False,
) -> None:
    """
    Execute VACUUM against a live Delta table.

    Parameters
    ----------
    spark : SparkSession
    table : str
    retain_hours : float | None
    dry_run : bool
        When True, prints the files that would be deleted without deleting them.

    Returns
    -------
    None
    """
    sql = build_vacuum_sql(table, retain_hours, dry_run)
    result = spark.sql(sql)
    if dry_run:
        result.show(truncate=False)
