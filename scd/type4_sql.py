"""
scd/type4_sql.py — SCD Type 4: Separate History Table (Spark SQL).

Module   : scd.type4_sql
Concept  : SCD Type 4 — Current table (Type 1) + history table, SQL implementation
When     : Same as type4_pyspark.  Prefer SQL when you want both statements
           visible in query history or when embedding in DLT pipelines.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from scd.config import SCDConfig


def _join_condition(business_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in business_keys)


def build_scd4_current_merge_sql(config: SCDConfig) -> str:
    """
    Generate the MERGE INTO SQL for the current table (Type 1 semantics).

    Parameters
    ----------
    config : SCDConfig

    Returns
    -------
    str
        MERGE INTO SQL for the current/active table.
    """
    join_cond = _join_condition(config.business_keys)
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def build_scd4_history_insert_sql(
    config: SCDConfig,
    tracked_columns: List[str],
    source_columns: List[str],
) -> str:
    """
    Generate INSERT SQL to append new and changed rows to the history table.

    Selects from source all rows that are either new (no matching business key
    in current table) or changed (business key exists, tracked cols differ).

    Adds:
    - changed_at  = current_timestamp()
    - change_type = CASE WHEN target key is NULL THEN 'INSERT' ELSE 'UPDATE'
    - scd_key     = SHA-256 of business_keys + changed_at

    Parameters
    ----------
    config : SCDConfig
        history_table must be set.
    tracked_columns : list[str]
    source_columns : list[str]

    Returns
    -------
    str
        INSERT INTO SQL for the history table.
    """
    if not config.history_table:
        raise ValueError("SCDConfig.history_table must be set for Type 4")

    join_cond = _join_condition(config.business_keys, left="t", right="s")
    change_clauses = " OR ".join(
        f"NOT (t.{c} <=> s.{c})" for c in tracked_columns
    )
    src_col_list = ", ".join(f"s.{c}" for c in source_columns)

    sk_expr = ""
    if config.surrogate_key_col:
        key_concat = ", '|', ".join(
            f"COALESCE(CAST(s.{c} AS STRING), '__null__')"
            for c in config.business_keys
        )
        sk_expr = (
            f"    sha2(concat_ws('|', {key_concat}, CAST(current_timestamp() AS STRING)), 256)"
            f" AS {config.surrogate_key_col},\n"
        )

    return f"""INSERT INTO {config.history_table}
SELECT
{sk_expr}    {src_col_list},
    current_timestamp() AS changed_at,
    CASE WHEN t.{config.business_keys[0]} IS NULL THEN 'INSERT' ELSE 'UPDATE' END AS change_type
FROM {config.source_table} AS s
LEFT JOIN {config.target_table} AS t
  ON {join_cond}
WHERE t.{config.business_keys[0]} IS NULL
   OR ({change_clauses})"""


def apply_scd4_sql(
    spark: SparkSession,
    config: SCDConfig,
    tracked_columns: List[str],
    source_columns: List[str],
) -> None:
    """
    Execute SCD Type 4 in two SQL steps: merge current + insert history.

    Parameters
    ----------
    spark : SparkSession
    config : SCDConfig
        source_table must be a registered table or temp view.
        history_table must be set.
    tracked_columns : list[str]
    source_columns : list[str]

    Returns
    -------
    None

    Examples
    --------
        df_source.createOrReplaceTempView("v_customers_incoming")

        config = SCDConfig(
            source_table="v_customers_incoming",
            target_table="catalog.gold.dim_customer_current",
            history_table="catalog.gold.dim_customer_history",
            business_keys=["customer_id"],
        )

        apply_scd4_sql(
            spark, config,
            tracked_columns=["email", "tier"],
            source_columns=["customer_id", "name", "email", "tier"],
        )
    """
    spark.sql(build_scd4_current_merge_sql(config))
    spark.sql(build_scd4_history_insert_sql(config, tracked_columns, source_columns))
