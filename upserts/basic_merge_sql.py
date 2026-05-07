"""
upserts/basic_merge_sql.py — Basic INSERT/UPDATE MERGE (Spark SQL).

Module   : upserts.basic_merge_sql
Concept  : Foundational Delta MERGE INTO, SQL implementation
When     : Same as basic_merge_pyspark.  Prefer SQL for DLT pipelines,
           notebook workflows, or when the MERGE must be auditable verbatim.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from upserts.config import UpsertConfig


def _join_condition(merge_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in merge_keys)


def build_basic_merge_sql(config: UpsertConfig) -> str:
    """
    Generate MERGE INTO SQL for basic INSERT/UPDATE upsert.

    Parameters
    ----------
    config : UpsertConfig

    Returns
    -------
    str
        MERGE INTO statement ready for spark.sql().
    """
    join_cond = _join_condition(config.merge_keys)
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def build_selective_update_sql(config: UpsertConfig, update_columns: List[str]) -> str:
    """
    Generate MERGE INTO SQL that updates only specific columns on match.

    Useful when the target has system columns (created_at, created_by) that
    should never be overwritten by source updates.

    Parameters
    ----------
    config : UpsertConfig
    update_columns : list[str]
        Columns to include in the UPDATE SET clause.

    Returns
    -------
    str
        MERGE INTO statement with explicit UPDATE SET column list.
    """
    join_cond = _join_condition(config.merge_keys)
    set_clause = ",\n    ".join(f"t.{c} = s.{c}" for c in update_columns)
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET
    {set_clause}
WHEN NOT MATCHED THEN
  INSERT *"""


def apply_basic_merge_sql(spark: SparkSession, config: UpsertConfig) -> None:
    """
    Execute basic MERGE against the live Delta target table.

    Parameters
    ----------
    spark : SparkSession
    config : UpsertConfig
        source_table must be a registered table or temp view.

    Returns
    -------
    None

    Examples
    --------
        df_source.createOrReplaceTempView("v_customers_incoming")

        config = UpsertConfig(
            source_table="v_customers_incoming",
            target_table="catalog.gold.dim_customer",
            merge_keys=["customer_id"],
        )

        apply_basic_merge_sql(spark, config)
    """
    spark.sql(build_basic_merge_sql(config))
