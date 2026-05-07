"""
scd/type1_sql.py — SCD Type 1: Overwrite (Spark SQL).

Module   : scd.type1_sql
Concept  : SCD Type 1 — Overwrite, SQL implementation
When     : Same as type1_pyspark.  Prefer SQL when embedding in DLT pipelines
           or when the MERGE should appear verbatim in query history.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from scd.config import SCDConfig


def _join_condition(business_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in business_keys)


def build_scd1_sql(config: SCDConfig) -> str:
    """
    Generate the MERGE INTO SQL for SCD Type 1.

    Uses UPDATE SET * and INSERT * — both require source and target to share
    the same column set.  If the target has extra columns (e.g. a surrogate
    key not present in the source), use explicit column lists instead and
    modify this function accordingly.

    Parameters
    ----------
    config : SCDConfig

    Returns
    -------
    str
        MERGE INTO SQL string ready to pass to spark.sql().
    """
    join_cond = _join_condition(config.business_keys)
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def build_scd1_conditional_sql(config: SCDConfig, tracked_columns: List[str]) -> str:
    """
    Generate a MERGE INTO that only updates rows where tracked columns changed.

    This avoids touching rows that have not changed, which reduces Delta
    write amplification on tables where most rows are stable each batch.

    The condition uses Spark SQL's null-safe NOT (t.col <=> s.col) pattern.

    Parameters
    ----------
    config : SCDConfig
    tracked_columns : list[str]
        Columns to compare.  When a change is detected in any of these,
        the UPDATE fires.  Unchanged rows are skipped.

    Returns
    -------
    str
        MERGE INTO SQL string.
    """
    join_cond = _join_condition(config.business_keys)
    change_clauses = " OR ".join(
        f"NOT (t.{c} <=> s.{c})" for c in tracked_columns
    )
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED AND ({change_clauses}) THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def apply_scd1_sql(spark: SparkSession, config: SCDConfig) -> None:
    """
    Execute SCD Type 1 MERGE against the live Delta target table.

    Parameters
    ----------
    spark : SparkSession
    config : SCDConfig
        source_table must be a registered table or temp view.

    Returns
    -------
    None

    Examples
    --------
        df_source.createOrReplaceTempView("v_customers_incoming")

        config = SCDConfig(
            source_table="v_customers_incoming",
            target_table="catalog.gold.dim_customer",
            business_keys=["customer_id"],
        )

        apply_scd1_sql(spark, config)
    """
    spark.sql(build_scd1_sql(config))


def apply_scd1_conditional_sql(
    spark: SparkSession, config: SCDConfig, tracked_columns: List[str]
) -> None:
    """
    Execute SCD Type 1 MERGE that only updates rows where tracked columns changed.

    Parameters
    ----------
    spark : SparkSession
    config : SCDConfig
    tracked_columns : list[str]
    """
    spark.sql(build_scd1_conditional_sql(config, tracked_columns))
