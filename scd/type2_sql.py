"""
scd/type2_sql.py — SCD Type 2: Full History with Effective Dates (Spark SQL).

Module   : scd.type2_sql
Concept  : SCD Type 2 — Full history, SQL implementation
When     : Same as type2_pyspark.  Prefer this when the pipeline is SQL-first
           or when you want MERGE statements visible in query history verbatim.
Author   : <your name>
Version  : 1.0.0

Two-step SQL pattern
--------------------
Step 1  MERGE INTO: expire current rows where business key matches AND
        tracked columns changed.  Sets effective_end = current_timestamp()
        and is_current = false.

Step 2  INSERT INTO: insert new row versions for new entities AND for the
        changed entities (their new version with the updated values).
        This is a separate INSERT rather than a MERGE WHEN MATCHED INSERT
        because SQL MERGE cannot emit two rows for a single matched source row.

Public API
----------
build_scd2_expire_sql(config, tracked_columns) -> str
    Step 1 MERGE SQL: expire old current rows.

build_scd2_insert_sql(config, tracked_columns, source_columns) -> str
    Step 2 INSERT SQL: insert new versions.

apply_scd2_sql(spark, config, tracked_columns, source_columns) -> None
    Execute both steps against live Delta tables.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from scd.config import SCDConfig


def _join_condition(business_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in business_keys)


def _change_condition_sql(tracked_columns: List[str], left: str = "t", right: str = "s") -> str:
    clauses = [f"NOT ({left}.{c} <=> {right}.{c})" for c in tracked_columns]
    return "(" + "\n       OR ".join(clauses) + ")"


def build_scd2_expire_sql(config: SCDConfig, tracked_columns: List[str]) -> str:
    """
    Generate SQL (Step 1): expire current rows for changed entities.

    Matches source rows against current target rows on business_keys WHERE
    at least one tracked column differs.  Updates effective_end and
    is_current on those matched target rows.

    Parameters
    ----------
    config : SCDConfig
    tracked_columns : list[str]
        Columns to compare for change detection.

    Returns
    -------
    str
        MERGE INTO SQL string for Step 1.
    """
    join_cond = _join_condition(config.business_keys)
    change_cond = _change_condition_sql(tracked_columns)
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
  AND t.{config.is_current_col} = true
WHEN MATCHED AND {change_cond} THEN
  UPDATE SET
    t.{config.effective_end_col} = current_timestamp(),
    t.{config.is_current_col}    = false"""


def build_scd2_insert_sql(
    config: SCDConfig,
    tracked_columns: List[str],
    source_columns: List[str],
) -> str:
    """
    Generate SQL (Step 2): insert new versions for new and changed entities.

    Selects from source all rows that either:
    a) have no matching business key in the current target (new entities), OR
    b) have a matching key AND at least one tracked column changed.

    Adds effective_start, effective_end (NULL), is_current (true), and
    a SHA-256 surrogate key.

    Parameters
    ----------
    config : SCDConfig
    tracked_columns : list[str]
        Used to identify changed rows.
    source_columns : list[str]
        All columns from the source, used to build the SELECT list.

    Returns
    -------
    str
        INSERT INTO SQL string for Step 2.
    """
    join_cond = _join_condition(config.business_keys, left="t", right="s")
    change_cond = _change_condition_sql(tracked_columns, left="t", right="s")
    src_col_list = ", ".join(f"s.{c}" for c in source_columns)

    sk_expr = ""
    if config.surrogate_key_col:
        key_concat = ", '|', ".join(
            f"COALESCE(CAST(s.{c} AS STRING), '__null__')"
            for c in config.business_keys
        )
        sk_expr = f"    sha2(concat_ws('|', {key_concat}, CAST(current_timestamp() AS STRING)), 256) AS {config.surrogate_key_col},\n"

    return f"""INSERT INTO {config.target_table}
SELECT
{sk_expr}    {src_col_list},
    current_timestamp() AS {config.effective_start_col},
    CAST(NULL AS TIMESTAMP) AS {config.effective_end_col},
    true AS {config.is_current_col}
FROM {config.source_table} AS s
LEFT JOIN (
    SELECT {', '.join(f't.{k}' for k in config.business_keys)},
           {', '.join(f't.{c}' for c in tracked_columns)}
    FROM {config.target_table} AS t
    WHERE t.{config.is_current_col} = true
) AS t ON {join_cond}
WHERE t.{config.business_keys[0]} IS NULL
   OR {change_cond}"""


def apply_scd2_sql(
    spark: SparkSession,
    config: SCDConfig,
    tracked_columns: List[str],
    source_columns: List[str],
) -> None:
    """
    Execute SCD Type 2 in two SQL steps against live Delta tables.

    config.source_table must be registered as a table or temp view.

    Parameters
    ----------
    spark : SparkSession
    config : SCDConfig
    tracked_columns : list[str]
        Columns to monitor for changes.
    source_columns : list[str]
        All column names in the source (used to build INSERT SELECT list).
        Obtain with: [f.name for f in df_source.schema.fields]

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

        apply_scd2_sql(
            spark, config,
            tracked_columns=["email", "tier"],
            source_columns=["customer_id", "name", "email", "tier"],
        )
    """
    expire_sql = build_scd2_expire_sql(config, tracked_columns)
    insert_sql = build_scd2_insert_sql(config, tracked_columns, source_columns)
    spark.sql(expire_sql)
    spark.sql(insert_sql)
