"""
medallion/pipeline_sql.py — Medallion DDL and DML SQL Templates.

Module   : medallion.pipeline_sql
Concept  : SQL for creating and populating each medallion layer
When     : Run once at pipeline bootstrap (CREATE TABLE), then per batch (MERGE).

SQL patterns by layer
---------------------
Bronze  → CREATE TABLE ... USING DELTA with metadata columns schema.
          INSERT INTO (append only — never MERGE).
Silver  → CREATE TABLE with business schema (caller-supplied column definitions).
          MERGE INTO from a source view.
Gold    → CREATE TABLE partitioned by a reporting column.
          INSERT OVERWRITE PARTITION for idempotent partition replacement.

Why SQL in addition to PySpark?
--------------------------------
SQL patterns are auditable in Databricks query history.  When a scheduled job
runs them, the full SQL is recorded, making it easy to replay a past run
manually or diagnose what a pipeline actually did.

Public API
----------
build_bronze_create_sql(config, extra_cols) -> str
build_silver_create_sql(config, col_defs) -> str
build_gold_create_sql(config, col_defs) -> str
build_silver_merge_sql(config, source_view) -> str
build_bronze_insert_sql(source_view, config) -> str
build_gold_insert_overwrite_sql(source_view, config, partition_value) -> str
"""

from __future__ import annotations

from typing import Dict, List, Optional

from medallion.config import MedallionConfig


def build_bronze_create_sql(
    config: MedallionConfig,
    extra_cols: Optional[List[str]] = None,
) -> str:
    """
    Generate CREATE TABLE IF NOT EXISTS SQL for the Bronze table.

    The Bronze schema intentionally uses STRING for all payload columns because
    the source schema may be unknown at pipeline creation time.  Type-cast in
    Silver.  Only the three mandatory metadata columns have explicit types.

    Parameters
    ----------
    config : MedallionConfig
    extra_cols : list[str] | None
        Additional column definitions in "name TYPE" format.
        Example: ["customer_id STRING", "order_date DATE"]

    Returns
    -------
    str
    """
    payload_cols = "\n".join(
        f"  {c}," for c in (extra_cols or [])
    )
    partition_clause = (
        f"\nPARTITIONED BY ({config.partition_col})"
        if config.partition_col else ""
    )
    return f"""CREATE TABLE IF NOT EXISTS {config.bronze_table} (
{payload_cols}
  _ingest_ts    TIMESTAMP NOT NULL,
  _batch_id     BIGINT NOT NULL,
  _source_file  STRING
) USING DELTA{partition_clause}
TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.autoOptimize.optimizeWrite = true
)"""


def build_silver_create_sql(
    config: MedallionConfig,
    col_defs: List[str],
) -> str:
    """
    Generate CREATE TABLE IF NOT EXISTS SQL for the Silver table.

    Parameters
    ----------
    config : MedallionConfig
    col_defs : list[str]
        Column definitions in "name TYPE [NOT NULL]" format.
        Example: ["customer_id STRING NOT NULL", "email STRING", "created_at TIMESTAMP"]

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If col_defs is empty.
    """
    if not col_defs:
        raise ValueError("col_defs must not be empty — Silver needs a declared schema")
    cols = "\n".join(f"  {c}," for c in col_defs).rstrip(",") + "\n" if col_defs else ""
    return f"""CREATE TABLE IF NOT EXISTS {config.silver_table} (
{cols}) USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)"""


def build_gold_create_sql(
    config: MedallionConfig,
    col_defs: List[str],
) -> str:
    """
    Generate CREATE TABLE IF NOT EXISTS SQL for the Gold table.

    Parameters
    ----------
    config : MedallionConfig
    col_defs : list[str]
        Column definitions.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If col_defs is empty.
    """
    if not col_defs:
        raise ValueError("col_defs must not be empty — Gold needs a declared schema")
    cols = "\n".join(f"  {c}," for c in col_defs).rstrip(",") + "\n" if col_defs else ""
    partition_clause = (
        f"\nPARTITIONED BY ({config.partition_col})"
        if config.partition_col else ""
    )
    return f"""CREATE TABLE IF NOT EXISTS {config.gold_table} (
{cols}) USING DELTA{partition_clause}
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)"""


def build_silver_merge_sql(
    config: MedallionConfig,
    source_view: str,
) -> str:
    """
    Generate MERGE INTO SQL for Silver upsert from a registered temp view.

    Matches on config.id_col.  When matched: update all columns.
    When not matched: insert all columns.

    Parameters
    ----------
    config : MedallionConfig
        id_col must be set.
    source_view : str
        Name of the registered temp view or table containing incoming data.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If config.id_col is empty.
    """
    if not config.id_col:
        raise ValueError(
            "MedallionConfig.id_col must be set to generate Silver MERGE SQL"
        )
    return f"""MERGE INTO {config.silver_table} AS t
USING {source_view} AS s
ON t.{config.id_col} = s.{config.id_col}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *"""


def build_bronze_insert_sql(
    source_view: str,
    config: MedallionConfig,
) -> str:
    """
    Generate INSERT INTO SQL appending a source view into Bronze.

    Adds the three metadata columns at insert time using Spark SQL functions.
    Use when the source is already registered as a temp view.

    Parameters
    ----------
    source_view : str
        Registered temp view containing raw source data.
    config : MedallionConfig

    Returns
    -------
    str
    """
    return f"""INSERT INTO {config.bronze_table}
SELECT
  *,
  current_timestamp() AS _ingest_ts,
  0                   AS _batch_id,
  input_file_name()   AS _source_file
FROM {source_view}"""


def build_gold_insert_overwrite_sql(
    source_view: str,
    config: MedallionConfig,
    partition_value: str,
) -> str:
    """
    Generate INSERT OVERWRITE SQL replacing one partition in the Gold table.

    This is the SQL equivalent of replaceWhere.  Idempotent: re-running with
    the same partition_value replaces that partition with identical data.

    Parameters
    ----------
    source_view : str
    config : MedallionConfig
        partition_col must be set.
    partition_value : str
        Literal value for the partition filter (e.g., '2024-06-01').

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If config.partition_col is empty.
    """
    if not config.partition_col:
        raise ValueError(
            "MedallionConfig.partition_col must be set for INSERT OVERWRITE SQL"
        )
    return f"""INSERT OVERWRITE {config.gold_table}
PARTITION ({config.partition_col} = '{partition_value}')
SELECT * FROM {source_view}
WHERE {config.partition_col} = '{partition_value}'"""
