"""
scd/type6_sql.py — SCD Type 6: Hybrid (Types 1 + 2 + 3) (Spark SQL).

Module   : scd.type6_sql
Concept  : SCD Type 6 — Hybrid, SQL implementation
When     : Same as type6_pyspark.  SQL variant useful when the full pipeline
           is SQL-first or when you want individual statements auditable in
           Databricks query history.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from scd.config import SCDConfig
from scd.type2_sql import build_scd2_expire_sql


def _join_condition(business_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in business_keys)


def _change_condition_sql(tracked_columns: List[str], left: str = "t", right: str = "s") -> str:
    clauses = [f"NOT ({left}.{c} <=> {right}.{c})" for c in tracked_columns]
    return "(" + "\n       OR ".join(clauses) + ")"


def build_scd6_insert_sql(
    config: SCDConfig,
    tracked_columns: List[str],
    prev_value_columns: List[str],
    source_columns: List[str],
) -> str:
    """
    Generate the INSERT SQL for Type 6 new versions.

    Inserts rows for both new entities and new versions of changed entities.
    Includes:
    - Source column values (new state)
    - prev_<col> values drawn from the current target row (NULL for new entities)
    - effective_start = current_timestamp()
    - effective_end   = NULL
    - is_current      = true
    - scd_key         = SHA-256 hash

    The prev_<col> values are retrieved via a LEFT JOIN to the target's
    current rows (is_current = true).

    Parameters
    ----------
    config : SCDConfig
    tracked_columns : list[str]
    prev_value_columns : list[str]
        Subset of tracked_columns for which to include prev_ columns.
    source_columns : list[str]

    Returns
    -------
    str
        INSERT INTO SQL string.
    """
    join_cond = _join_condition(config.business_keys, left="t", right="s")
    change_cond = _change_condition_sql(tracked_columns, left="t", right="s")
    src_col_list = ", ".join(f"s.{c}" for c in source_columns)
    prev_col_list = ", ".join(f"t.{c} AS prev_{c}" for c in prev_value_columns)

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

    prev_select = f"    {prev_col_list}," if prev_col_list else ""

    return f"""INSERT INTO {config.target_table}
SELECT
{sk_expr}    {src_col_list},
{prev_select}
    current_timestamp() AS {config.effective_start_col},
    CAST(NULL AS TIMESTAMP) AS {config.effective_end_col},
    true AS {config.is_current_col}
FROM {config.source_table} AS s
LEFT JOIN (
    SELECT *
    FROM {config.target_table}
    WHERE {config.is_current_col} = true
) AS t ON {join_cond}
WHERE t.{config.business_keys[0]} IS NULL
   OR {change_cond}"""


def build_scd6_current_view_sql(
    config: SCDConfig,
    tracked_columns: List[str],
    view_name: str,
) -> str:
    """
    Generate a CREATE OR REPLACE VIEW that adds current_<col> columns.

    This view exposes Type 1 semantics (always-latest values in current_<col>)
    on top of the Type 2 history table without denormalising at write time.

    Parameters
    ----------
    config : SCDConfig
    tracked_columns : list[str]
        Columns for which to compute current_<col> via window function.
    view_name : str
        Fully-qualified name of the view to create.

    Returns
    -------
    str
        CREATE OR REPLACE VIEW SQL.
    """
    bk = config.business_keys[0]
    current_cols = ",\n    ".join(
        f"LAST_VALUE({c}) OVER ("
        f"PARTITION BY {bk} ORDER BY {config.effective_start_col} "
        f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
        f") AS current_{c}"
        for c in tracked_columns
    )
    return f"""CREATE OR REPLACE VIEW {view_name} AS
SELECT
    *,
    {current_cols}
FROM {config.target_table}"""


def apply_scd6_sql(
    spark: SparkSession,
    config: SCDConfig,
    tracked_columns: List[str],
    prev_value_columns: List[str],
    source_columns: List[str],
) -> None:
    """
    Execute SCD Type 6 in two SQL steps: expire old rows, insert new versions.

    Parameters
    ----------
    spark : SparkSession
    config : SCDConfig
        source_table must be a registered table or temp view.
    tracked_columns : list[str]
    prev_value_columns : list[str]
    source_columns : list[str]

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

        apply_scd6_sql(
            spark, config,
            tracked_columns=["email", "tier"],
            prev_value_columns=["tier"],
            source_columns=["customer_id", "name", "email", "tier"],
        )
    """
    expire_sql = build_scd2_expire_sql(config, tracked_columns)
    insert_sql = build_scd6_insert_sql(config, tracked_columns, prev_value_columns, source_columns)
    spark.sql(expire_sql)
    spark.sql(insert_sql)
