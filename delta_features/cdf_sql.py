"""
delta_features/cdf_sql.py — Change Data Feed (Spark SQL).

Module   : delta_features.cdf_sql
Concept  : Read Delta CDF and build CDC pipelines using SQL
When     : Same as cdf_pyspark.  Prefer SQL for DLT pipelines, notebook
           workflows, or when the read statement must be auditable verbatim.

Public API
----------
build_enable_cdf_sql(table) -> str
build_table_changes_sql(table, start_version, end_version, change_types) -> str
build_table_changes_timestamp_sql(table, start_ts, end_ts, change_types) -> str
build_cdf_upsert_sql(source_cte, target, merge_keys, change_type_col) -> str
apply_enable_cdf(spark, table) -> None
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import SparkSession


def build_enable_cdf_sql(table: str) -> str:
    """
    Generate ALTER TABLE SQL to enable Change Data Feed on an existing table.

    CDF must be enabled before changes are recorded.  Changes committed before
    CDF was enabled are not available in the feed — only changes after enablement.

    Parameters
    ----------
    table : str
        Fully-qualified Delta table name.

    Returns
    -------
    str

    Notes
    -----
    Alternatively, enable at table creation:
        CREATE TABLE ... TBLPROPERTIES (delta.enableChangeDataFeed = true)

    Or cluster-wide for all new tables (not recommended for production):
        SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true
    """
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"(delta.enableChangeDataFeed = true)"
    )


def build_disable_cdf_sql(table: str) -> str:
    """
    Generate ALTER TABLE SQL to disable Change Data Feed.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"(delta.enableChangeDataFeed = false)"
    )


def build_table_changes_sql(
    table: str,
    start_version: int,
    end_version: Optional[int] = None,
    change_types: Optional[List[str]] = None,
) -> str:
    """
    Generate SQL using the table_changes() function to read CDF by version range.

    table_changes() is a Databricks SQL function that returns the change records
    for a table between two Delta versions.

    Parameters
    ----------
    table : str
        Fully-qualified Delta table name (quoted inside the function call).
    start_version : int
        First version to read (inclusive).
    end_version : int | None
        Last version to read (inclusive).  None = read to the latest version.
    change_types : list[str] | None
        Filter output to specific change types.  None = all change types.
        Example: ['insert', 'update_postimage'] for current-state changes only.

    Returns
    -------
    str
        SELECT SQL using table_changes().

    Examples
    --------
        sql = build_table_changes_sql("catalog.bronze.events", start_version=10, end_version=20)
        # SELECT * FROM table_changes('catalog.bronze.events', 10, 20)

        sql = build_table_changes_sql(
            "catalog.bronze.events", 10,
            change_types=["insert", "update_postimage"]
        )
        # SELECT * FROM table_changes('catalog.bronze.events', 10)
        # WHERE _change_type IN ('insert', 'update_postimage')
    """
    if end_version is not None:
        source = f"table_changes('{table}', {start_version}, {end_version})"
    else:
        source = f"table_changes('{table}', {start_version})"

    sql = f"SELECT * FROM {source}"

    if change_types:
        quoted = ", ".join(f"'{ct}'" for ct in change_types)
        sql += f"\nWHERE _change_type IN ({quoted})"

    return sql


def build_table_changes_timestamp_sql(
    table: str,
    start_ts: str,
    end_ts: Optional[str] = None,
    change_types: Optional[List[str]] = None,
) -> str:
    """
    Generate SQL using table_changes() with timestamp-based range.

    Parameters
    ----------
    table : str
    start_ts : str
        ISO 8601 timestamp string.  Changes AT OR AFTER this time are included.
    end_ts : str | None
        ISO 8601 timestamp string.  None = read to the latest committed version.
    change_types : list[str] | None

    Returns
    -------
    str
    """
    if end_ts is not None:
        source = f"table_changes('{table}', '{start_ts}', '{end_ts}')"
    else:
        source = f"table_changes('{table}', '{start_ts}')"

    sql = f"SELECT * FROM {source}"

    if change_types:
        quoted = ", ".join(f"'{ct}'" for ct in change_types)
        sql += f"\nWHERE _change_type IN ({quoted})"

    return sql


def build_cdf_postimage_merge_sql(
    target: str,
    cdf_view: str,
    merge_keys: List[str],
    has_deletes: bool = False,
    delete_change_type: str = "delete",
) -> str:
    """
    Generate a MERGE INTO that applies CDF post-images to a downstream table.

    Reads the update_postimage and insert records from the CDF view and
    applies them as an upsert.  Optionally handles delete records.

    Parameters
    ----------
    target : str
        Downstream Delta table to apply changes to.
    cdf_view : str
        Temp view or CTE containing the CDF output (post-images only, or all
        change types if has_deletes is True).
    merge_keys : list[str]
        Join columns.
    has_deletes : bool
        When True, adds WHEN MATCHED AND _change_type = 'delete' THEN DELETE.
    delete_change_type : str
        Change type string for delete records.  Default: 'delete'.

    Returns
    -------
    str
        MERGE INTO SQL that propagates CDF changes downstream.
    """
    join_cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)

    if has_deletes:
        return f"""MERGE INTO {target} AS t
USING {cdf_view} AS s
ON {join_cond}
WHEN MATCHED AND s._change_type = '{delete_change_type}' THEN
  DELETE
WHEN MATCHED AND s._change_type != '{delete_change_type}' THEN
  UPDATE SET *
WHEN NOT MATCHED AND s._change_type != '{delete_change_type}' THEN
  INSERT *"""

    return f"""MERGE INTO {target} AS t
USING {cdf_view} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def apply_enable_cdf(spark: SparkSession, table: str) -> None:
    """
    Enable Change Data Feed on a live Delta table.

    Parameters
    ----------
    spark : SparkSession
    table : str
    """
    spark.sql(build_enable_cdf_sql(table))
