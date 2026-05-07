"""
upserts/schema_evolution_sql.py — Schema Evolution During MERGE (Spark SQL).

Module   : upserts.schema_evolution_sql
Concept  : Schema evolution helpers, SQL implementation
When     : Same as schema_evolution_pyspark.  SQL variant for DDL generation
           and audit-friendly column management.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField

from upserts.config import UpsertConfig


def build_add_column_sql(target_table: str, field: StructField) -> str:
    """
    Generate ALTER TABLE ADD COLUMN SQL for a single StructField.

    Parameters
    ----------
    target_table : str
        Fully-qualified Delta table name.
    field : StructField
        Column to add.  dataType.simpleString() is used for the SQL type.

    Returns
    -------
    str
        ALTER TABLE ... ADD COLUMN ... SQL statement.

    Notes
    -----
    The generated statement makes the column nullable regardless of the
    StructField.nullable setting — Delta requires new columns to be nullable
    since pre-existing rows will have NULL values for the new column.
    """
    return f"ALTER TABLE {target_table} ADD COLUMN {field.name} {field.dataType.simpleString()}"


def build_add_columns_sql(target_table: str, fields: List[StructField]) -> List[str]:
    """
    Generate a list of ALTER TABLE ADD COLUMN statements for multiple fields.

    Parameters
    ----------
    target_table : str
    fields : list[StructField]

    Returns
    -------
    list[str]
        One ALTER TABLE statement per new field.
    """
    return [build_add_column_sql(target_table, f) for f in fields]


def build_schema_evolution_merge_sql(config: UpsertConfig) -> str:
    """
    Generate MERGE INTO SQL for use after schema evolution DDL has been applied.

    This is a standard MERGE INTO — the schema evolution is handled by the DDL
    statements produced by build_add_columns_sql(), not by this MERGE itself.

    Parameters
    ----------
    config : UpsertConfig

    Returns
    -------
    str
        MERGE INTO SQL.
    """
    join_cond = " AND ".join(
        f"t.{k} = s.{k}" for k in config.merge_keys
    )
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def build_schema_evolution_set_sql(enabled: bool = True) -> str:
    """
    Generate the Spark SQL SET statement for autoMerge schema evolution.

    Parameters
    ----------
    enabled : bool
        True to enable autoMerge, False to disable.

    Returns
    -------
    str
        SET spark.databricks.delta.schema.autoMerge.enabled = true/false
    """
    value = "true" if enabled else "false"
    return f"SET spark.databricks.delta.schema.autoMerge.enabled = {value}"


def apply_schema_evolution_merge_sql(
    spark: SparkSession,
    config: UpsertConfig,
    new_fields: List[StructField] = None,
) -> None:
    """
    Execute schema evolution DDL (if needed) then MERGE.

    If new_fields is provided and non-empty, issues ALTER TABLE ADD COLUMN
    for each field before running the MERGE.  If new_fields is None, uses
    the autoMerge session config approach instead.

    Parameters
    ----------
    spark : SparkSession
    config : UpsertConfig
        source_table must be a registered table or temp view.
    new_fields : list[StructField] | None
        When provided: explicit DDL approach — alter the table first.
        When None: autoMerge approach — set session config and let Delta handle it.

    Returns
    -------
    None

    Examples
    --------
    Explicit DDL approach:
        from upserts.schema_evolution_pyspark import detect_schema_drift
        new_fields = detect_schema_drift(df_source, spark.table(config.target_table))
        apply_schema_evolution_merge_sql(spark, config, new_fields)

    autoMerge approach:
        apply_schema_evolution_merge_sql(spark, config, new_fields=None)
    """
    if new_fields:
        ddl_stmts = build_add_columns_sql(config.target_table, new_fields)
        for stmt in ddl_stmts:
            spark.sql(stmt)
        spark.sql(build_schema_evolution_merge_sql(config))
    else:
        spark.sql(build_schema_evolution_set_sql(enabled=True))
        try:
            spark.sql(build_schema_evolution_merge_sql(config))
        finally:
            spark.sql(build_schema_evolution_set_sql(enabled=False))
