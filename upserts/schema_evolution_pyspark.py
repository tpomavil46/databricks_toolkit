"""
upserts/schema_evolution_pyspark.py — Schema Evolution During MERGE (PySpark).

Module   : upserts.schema_evolution_pyspark
Concept  : Automatically propagate new source columns into a Delta target table
When     : Source schema evolves over time — new columns appear in the source
           that do not yet exist in the target, and you want them to land
           automatically without manual ALTER TABLE statements.
Author   : <your name>
Version  : 1.0.0

What it is
----------
By default, Delta MERGE INTO raises an AnalysisException if the source
DataFrame has columns that are absent from the target table.  Schema evolution
is the practice of automatically extending the target schema to accommodate
new source columns.

Three approaches, from least to most controlled:

1. autoMerge session setting (this module's default)
   spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
   Delta automatically adds new columns during MERGE.  Simple but applies
   globally to the session — may silently add columns you didn't intend.

2. Explicit ADD COLUMN before MERGE (detect_schema_drift + add_missing_columns)
   Inspect the source schema, compare with target, and issue ALTER TABLE ADD
   COLUMN DDL for each new column before running the MERGE.  More controlled
   than autoMerge: you see exactly what changes and can apply validation first.

3. Schema-projection MERGE (select only known columns from source)
   Never evolve the target — drop new source columns before MERGE.
   Used when the target schema is locked and source schema changes are noise.
   Implementation: df_source.select(target_columns_only).

When to use
-----------
Approach 1 (autoMerge): rapid development, trusted single source, no locked schema.
Approach 2 (explicit DDL): production pipelines where schema drift should be
    logged, reviewed, and applied intentionally.
Approach 3 (projection): contract-first schemas where the target is the source
    of truth and source changes must be explicitly whitelisted.

Delta / Databricks considerations
----------------------------------
- autoMerge enabled: 'spark.databricks.delta.schema.autoMerge.enabled' = 'true'
  Requires DBR 10.5+ / Delta Lake 2.0+.
- New columns added via schema evolution are NULL for all pre-existing rows.
- Column type changes (e.g., INT → BIGINT) are NOT handled by autoMerge —
  those require explicit ALTER TABLE CHANGE COLUMN or table rebuild.
- After schema evolution, run OPTIMIZE on the target to compact files that
  now span the old and new schema.

Public API
----------
detect_schema_drift(df_source, df_target) -> list[StructField]
    Pure transform: returns new fields in source not present in target.
    Testable without Delta.

add_missing_columns(spark, new_fields, target_table) -> None
    Executes ALTER TABLE ADD COLUMN for each drift field.

apply_schema_evolution_merge(spark, df_source, config) -> None
    autoMerge approach: set session config, run MERGE, reset config.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField

from upserts.config import UpsertConfig


def detect_schema_drift(
    df_source: DataFrame,
    df_target: DataFrame,
) -> List[StructField]:
    """
    Return StructField objects for columns present in source but absent from target.

    This is the pure-transform entry point for controlled schema evolution.
    The caller inspects the result, decides which fields to accept, then calls
    add_missing_columns() with the approved list.

    Parameters
    ----------
    df_source : DataFrame
        Incoming batch with potentially new columns.
    df_target : DataFrame
        Current target table schema (read with spark.table(target_table)).

    Returns
    -------
    list[StructField]
        Fields in df_source.schema not present in df_target.schema by name.
        Empty list when the schemas are identical (no drift).

    Examples
    --------
        new_fields = detect_schema_drift(df_source, spark.table("catalog.gold.dim_customer"))
        if new_fields:
            print(f"Schema drift detected: {[f.name for f in new_fields]}")
            add_missing_columns(spark, new_fields, "catalog.gold.dim_customer")
    """
    target_col_names = {f.name.lower() for f in df_target.schema.fields}
    return [
        f for f in df_source.schema.fields
        if f.name.lower() not in target_col_names
    ]


def add_missing_columns(
    spark: SparkSession,
    new_fields: List[StructField],
    target_table: str,
) -> None:
    """
    Issue ALTER TABLE ADD COLUMN for each new StructField.

    Call this with the output of detect_schema_drift() after reviewing and
    approving the new columns.  New columns are NULLable by default and will
    be NULL for all pre-existing rows.

    Parameters
    ----------
    spark : SparkSession
    new_fields : list[StructField]
        Fields returned by detect_schema_drift().
    target_table : str
        Fully-qualified Delta table name.

    Returns
    -------
    None

    Notes
    -----
    ALTER TABLE ADD COLUMN is a metadata-only operation in Delta — it does not
    rewrite any data files.  It runs in milliseconds regardless of table size.
    """
    for field in new_fields:
        sql_type = field.dataType.simpleString()
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {field.name} {sql_type}")


def project_to_target_schema(
    df_source: DataFrame,
    df_target: DataFrame,
) -> DataFrame:
    """
    Drop source columns that are not in the target schema.

    Use this when you want to MERGE without schema evolution: unknown source
    columns are silently discarded rather than causing an AnalysisException or
    triggering schema drift.

    Parameters
    ----------
    df_source : DataFrame
    df_target : DataFrame

    Returns
    -------
    DataFrame
        df_source with only the columns that exist in df_target.
    """
    target_cols = {f.name.lower() for f in df_target.schema.fields}
    source_cols_to_keep = [c for c in df_source.columns if c.lower() in target_cols]
    return df_source.select(*source_cols_to_keep)


def apply_schema_evolution_merge(
    spark: SparkSession,
    df_source: DataFrame,
    config: UpsertConfig,
) -> None:
    """
    Execute MERGE with autoMerge enabled so new source columns are auto-added.

    Temporarily sets the autoMerge session config, executes the MERGE, then
    resets the config to its original value.

    Parameters
    ----------
    spark : SparkSession
    df_source : DataFrame
        May contain columns not yet in the target.
    config : UpsertConfig
        allow_schema_evolution should be True on the config, but this function
        forces autoMerge regardless of that flag (it is explicitly called for
        schema evolution).

    Returns
    -------
    None

    Notes
    -----
    Requires DBR 10.5+ / Delta Lake 2.0+.
    AutoMerge adds new columns as NULL for pre-existing rows.
    Type changes still raise AnalysisException — handle those with explicit DDL.

    Examples
    --------
        config = UpsertConfig(
            source_table="catalog.silver.customers_v2",
            target_table="catalog.gold.dim_customer",
            merge_keys=["customer_id"],
            allow_schema_evolution=True,
        )

        apply_schema_evolution_merge(spark, df_source_v2, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    prev_setting = spark.conf.get(
        "spark.databricks.delta.schema.autoMerge.enabled", "false"
    )
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    try:
        join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.merge_keys)
        (
            DeltaTable.forName(spark, config.target_table)
            .alias("t")
            .merge(df_source.alias("s"), join_expr)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    finally:
        spark.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled", prev_setting
        )
