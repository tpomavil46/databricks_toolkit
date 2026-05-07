"""
scd/type3_sql.py — SCD Type 3: Previous Value Columns (Spark SQL).

Module   : scd.type3_sql
Concept  : SCD Type 3 — Previous value columns, SQL implementation
When     : Same as type3_pyspark.  SQL variant useful for DLT or ad-hoc
           notebook execution where the MERGE should be auditable in query
           history verbatim.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from scd.config import SCDConfig


def _join_condition(business_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in business_keys)


def build_scd3_sql(
    config: SCDConfig,
    tracked_columns: List[str],
    prev_value_columns: List[str],
) -> str:
    """
    Generate the MERGE INTO SQL for SCD Type 3.

    The WHEN MATCHED UPDATE clause explicitly sets:
    - prev_<col> = t.<col>  (capture old value from target)
    - <col>      = s.<col>  (apply new value from source)

    The WHEN NOT MATCHED INSERT clause sets prev_<col> = NULL for new rows
    (no previous value exists for a first-time insert).

    Parameters
    ----------
    config : SCDConfig
    tracked_columns : list[str]
        Columns to update on WHEN MATCHED.
    prev_value_columns : list[str]
        Subset of tracked_columns for which to maintain prev_ columns.

    Returns
    -------
    str
        MERGE INTO SQL string.

    Notes
    -----
    This MERGE assumes the target already has prev_<col> columns.  If the
    table does not have them, run ALTER TABLE first:
        ALTER TABLE {config.target_table} ADD COLUMN prev_tier STRING;
    """
    join_cond = _join_condition(config.business_keys)

    update_clauses = []
    for c in prev_value_columns:
        update_clauses.append(f"    t.prev_{c} = t.{c}")
    for c in tracked_columns:
        update_clauses.append(f"    t.{c} = s.{c}")

    insert_cols = list(config.business_keys) + list(tracked_columns) + [f"prev_{c}" for c in prev_value_columns]
    insert_src_vals = (
        [f"s.{k}" for k in config.business_keys]
        + [f"s.{c}" for c in tracked_columns]
        + ["NULL" for _ in prev_value_columns]
    )

    update_block = ",\n".join(update_clauses)
    insert_col_block = ", ".join(insert_cols)
    insert_val_block = ", ".join(insert_src_vals)

    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET
{update_block}
WHEN NOT MATCHED THEN
  INSERT ({insert_col_block})
  VALUES ({insert_val_block})"""


def apply_scd3_sql(
    spark: SparkSession,
    config: SCDConfig,
    tracked_columns: List[str],
    prev_value_columns: List[str],
) -> None:
    """
    Execute SCD Type 3 MERGE against the live Delta target table.

    Parameters
    ----------
    spark : SparkSession
    config : SCDConfig
    tracked_columns : list[str]
        All columns to update when a match is found.
    prev_value_columns : list[str]
        Columns for which to copy old value to prev_<col>.

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

        apply_scd3_sql(
            spark, config,
            tracked_columns=["email", "tier"],
            prev_value_columns=["tier"],
        )
    """
    sql = build_scd3_sql(config, tracked_columns, prev_value_columns)
    spark.sql(sql)
