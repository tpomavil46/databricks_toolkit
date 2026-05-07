"""
upserts/delete_aware_sql.py — INSERT / UPDATE / DELETE MERGE (Spark SQL).

Module   : upserts.delete_aware_sql
Concept  : CDC MERGE with DELETE clause, SQL implementation
When     : Same as delete_aware_pyspark.  Prefer SQL when the MERGE should be
           readable and auditable in Databricks query history verbatim.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from upserts.config import UpsertConfig


def _join_condition(merge_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in merge_keys)


def build_delete_aware_merge_sql(config: UpsertConfig) -> str:
    """
    Generate a MERGE INTO SQL with INSERT, UPDATE, and DELETE clauses.

    The delete clause fires first (when matched AND delete signal is set).
    The update clause fires for matched rows without the delete signal.
    The insert clause fires only for new rows without the delete signal —
    we never insert a tombstone record as a brand-new target row.

    Parameters
    ----------
    config : UpsertConfig
        delete_indicator_col: the column carrying the CDC op code.
        delete_indicator_value: the value that means "delete".

    Returns
    -------
    str
        MERGE INTO SQL string ready for spark.sql().
    """
    join_cond = _join_condition(config.merge_keys)

    if config.delete_indicator_col:
        del_col = config.delete_indicator_col
        del_val = config.delete_indicator_value
        return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED AND s.{del_col} = '{del_val}' THEN
  DELETE
WHEN MATCHED AND s.{del_col} != '{del_val}' THEN
  UPDATE SET *
WHEN NOT MATCHED AND s.{del_col} != '{del_val}' THEN
  INSERT *"""

    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def build_soft_delete_sql(config: UpsertConfig, is_active_col: str = "is_active") -> str:
    """
    Generate a MERGE INTO that marks rows as inactive instead of physically deleting.

    Soft-delete alternative: set is_active = false on the matched row when the
    delete signal is present.  Preserves history and avoids Delta compaction
    overhead from many physical deletes.

    Requires the target table to have an is_active column (boolean or int).

    Parameters
    ----------
    config : UpsertConfig
        delete_indicator_col and delete_indicator_value must be set.
    is_active_col : str
        Name of the soft-delete flag column in the target.

    Returns
    -------
    str
        MERGE INTO SQL string.
    """
    join_cond = _join_condition(config.merge_keys)
    del_col = config.delete_indicator_col
    del_val = config.delete_indicator_value
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED AND s.{del_col} = '{del_val}' THEN
  UPDATE SET t.{is_active_col} = false
WHEN MATCHED AND s.{del_col} != '{del_val}' THEN
  UPDATE SET *
WHEN NOT MATCHED AND s.{del_col} != '{del_val}' THEN
  INSERT *"""


def apply_delete_aware_merge_sql(spark: SparkSession, config: UpsertConfig) -> None:
    """
    Execute the delete-aware MERGE against the live Delta target table.

    Parameters
    ----------
    spark : SparkSession
    config : UpsertConfig

    Returns
    -------
    None

    Examples
    --------
        df_source.createOrReplaceTempView("v_customers_cdc")

        config = UpsertConfig(
            source_table="v_customers_cdc",
            target_table="catalog.gold.dim_customer",
            merge_keys=["customer_id"],
            delete_indicator_col="op",
            delete_indicator_value="D",
        )

        apply_delete_aware_merge_sql(spark, config)
    """
    spark.sql(build_delete_aware_merge_sql(config))
