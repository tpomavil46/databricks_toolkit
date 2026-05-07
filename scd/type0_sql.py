"""
scd/type0_sql.py — SCD Type 0: Fixed / Preserve Original (Spark SQL).

Module   : scd.type0_sql
Concept  : SCD Type 0 — Fixed dimension attributes, SQL implementation
When     : Same use cases as type0_pyspark.  Choose the SQL flavour when
           your team prefers SQL-first pipelines, DLT notebooks, or when
           you want the MERGE statement logged in query history verbatim.
Author   : <your name>
Version  : 1.0.0

SQL vs PySpark trade-offs (applies to all SCD SQL modules)
----------------------------------------------------------
SQL implementation
    Pro: Statements appear verbatim in Databricks query history and Spark UI,
         making auditing straightforward.  DLT pipelines can embed these
         SQL strings directly.  DBAs/analysts can read and modify without
         PySpark knowledge.
    Con: Dynamic column lists require string interpolation, which is harder
         to type-check and test.  Error messages from malformed SQL are less
         descriptive than DataFrame API errors.

PySpark implementation
    Pro: DataFrame API errors include column names and types.  Static type
         checkers (mypy) can catch column name typos at lint time.
    Con: Generated query plan may look different from hand-written SQL;
         harder for SQL-oriented reviewers to audit.

Public API
----------
build_scd0_sql(config) -> str
    Generate the MERGE INTO SQL string.  Call this to inspect or log the
    SQL before executing.

apply_scd0_sql(spark, config) -> None
    Execute the generated MERGE INTO against the live Delta target table.

detect_rejected_sql(spark, config) -> str
    Generate a SELECT that returns rows that WOULD have changed.
    Execute with spark.sql() to get an audit DataFrame.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from scd.config import SCDConfig


def _join_condition(business_keys: List[str], left: str = "t", right: str = "s") -> str:
    """Build the ON clause for MERGE: 't.k1 = s.k1 AND t.k2 = s.k2'."""
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in business_keys)


def _change_condition_sql(tracked_columns: List[str], left: str = "t", right: str = "s") -> str:
    """
    Build a WHERE clause that is true when any tracked column changed.

    Uses Spark SQL's null-safe NOT (col1 <=> col2) pattern:
    - NULL <=> NULL  is TRUE  → NOT TRUE = FALSE (no change detected for NULL→NULL)
    - 'a'  <=> 'a'  is TRUE  → NOT TRUE = FALSE (no change)
    - 'a'  <=> 'b'  is FALSE → NOT FALSE = TRUE  (change detected)
    - NULL <=> 'a'  is FALSE → NOT FALSE = TRUE  (change detected)
    """
    clauses = [f"NOT ({left}.{c} <=> {right}.{c})" for c in tracked_columns]
    return "(" + "\n       OR ".join(clauses) + ")"


def build_scd0_sql(config: SCDConfig, tracked_columns: List[str] = None) -> str:
    """
    Generate the MERGE INTO SQL for SCD Type 0.

    The generated statement has only a WHEN NOT MATCHED INSERT ALL clause.
    Existing rows are never updated or deleted — the WHEN MATCHED path
    is intentionally absent.

    Parameters
    ----------
    config : SCDConfig
        source_table and target_table are used verbatim in the SQL.
        business_keys drive the ON condition.
    tracked_columns : list[str] | None
        Not used in the MERGE itself (Type 0 never updates), but included
        as a parameter for documentation consistency with other types.

    Returns
    -------
    str
        A MERGE INTO SQL statement ready to pass to spark.sql().
    """
    join_cond = _join_condition(config.business_keys)
    return f"""MERGE INTO {config.target_table} AS t
USING {config.source_table} AS s
ON {join_cond}
WHEN NOT MATCHED THEN
  INSERT *"""


def detect_rejected_sql(config: SCDConfig, tracked_columns: List[str]) -> str:
    """
    Generate SQL that returns source rows that would have updated an existing row.

    Execute with spark.sql(detect_rejected_sql(...)) to get an audit DataFrame.

    Parameters
    ----------
    config : SCDConfig
    tracked_columns : list[str]
        Columns to compare for change detection.

    Returns
    -------
    str
        A SELECT statement returning matching rows with changes.
    """
    join_cond = _join_condition(config.business_keys)
    change_cond = _change_condition_sql(tracked_columns)
    return f"""SELECT s.*
FROM {config.source_table} AS s
INNER JOIN {config.target_table} AS t
  ON {join_cond}
WHERE {change_cond}"""


def apply_scd0_sql(spark: SparkSession, config: SCDConfig) -> None:
    """
    Execute SCD Type 0 MERGE against the live Delta target table.

    Requires config.source_table to be registered as a table or temp view
    accessible from the active SparkSession.

    Parameters
    ----------
    spark : SparkSession
    config : SCDConfig

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

        apply_scd0_sql(spark, config)
    """
    sql = build_scd0_sql(config)
    spark.sql(sql)
