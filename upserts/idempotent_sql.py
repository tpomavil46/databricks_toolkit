"""
upserts/idempotent_sql.py — Idempotent Upsert Patterns (Spark SQL).

Module   : upserts.idempotent_sql
Concept  : Idempotent MERGE with dedup CTE and content-hash guard, SQL implementation
When     : Same as idempotent_pyspark.  SQL variant for DLT and notebook pipelines.
Author   : <your name>
Version  : 1.0.0
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from upserts.config import UpsertConfig


def _join_condition(merge_keys: List[str], left: str = "t", right: str = "s") -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in merge_keys)


def build_dedup_cte_sql(
    config: UpsertConfig,
    order_col: str,
    desc: bool = True,
) -> str:
    """
    Generate a CTE that deduplicates the source table to one row per merge key.

    The CTE uses ROW_NUMBER() OVER (PARTITION BY merge_keys ORDER BY order_col).
    This CTE is intended to be embedded in a larger MERGE statement.

    Parameters
    ----------
    config : UpsertConfig
    order_col : str
        Column to order by when selecting which duplicate to keep.
    desc : bool
        True (default): latest row wins (ORDER BY order_col DESC).
        False: first row wins.

    Returns
    -------
    str
        CTE fragment: 'WITH deduped AS (SELECT *, ROW_NUMBER()... WHERE _rn=1)'
    """
    direction = "DESC" if desc else "ASC"
    partition_cols = ", ".join(config.merge_keys)
    return f"""WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY {partition_cols}
      ORDER BY {order_col} {direction}
    ) AS _rn
  FROM {config.source_table}
)
SELECT * EXCEPT(_rn) FROM deduped WHERE _rn = 1"""


def build_idempotent_merge_sql(
    config: UpsertConfig,
    tracked_columns: List[str],
    dedup_view: str = "deduped_source",
) -> str:
    """
    Generate a MERGE INTO with content-hash change guard.

    Assumes:
    - The source has been deduplicated and registered as dedup_view.
    - The target already has config.content_hash_col column.

    Parameters
    ----------
    config : UpsertConfig
    tracked_columns : list[str]
        Columns used to compute the content hash in the source SELECT.
    dedup_view : str
        Name of the deduplicated source view to MERGE from.

    Returns
    -------
    str
        MERGE INTO SQL using hash-guarded UPDATE.
    """
    join_cond = _join_condition(config.merge_keys, left="t", right="s")
    hash_col = config.content_hash_col

    hash_concat = ", '|', ".join(
        f"COALESCE(CAST(s.{c} AS STRING), '__null__')"
        for c in tracked_columns
    )

    return f"""MERGE INTO {config.target_table} AS t
USING (
  SELECT *,
    sha2(concat_ws('|', {hash_concat}), 256) AS {hash_col}
  FROM {dedup_view}
) AS s
ON {join_cond}
WHEN MATCHED AND t.{hash_col} != s.{hash_col} THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *"""


def apply_idempotent_merge_sql(
    spark: SparkSession,
    config: UpsertConfig,
    tracked_columns: List[str],
    order_col: str = "",
) -> None:
    """
    Execute an idempotent MERGE in two steps: dedup + hash-guarded MERGE.

    Step 1: Create a deduplicated temp view from the source (if order_col set).
    Step 2: Execute the hash-guarded MERGE against the target.

    Parameters
    ----------
    spark : SparkSession
    config : UpsertConfig
        source_table must be a registered table or temp view.
    tracked_columns : list[str]
        Columns to hash for change detection.
    order_col : str
        Deduplication order column.  Empty = skip deduplication step.

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

        apply_idempotent_merge_sql(
            spark, config,
            tracked_columns=["name", "email", "tier"],
            order_col="received_at",
        )
    """
    dedup_view = "_deduped_source"

    if order_col:
        dedup_sql = build_dedup_cte_sql(config, order_col)
        spark.sql(dedup_sql).createOrReplaceTempView(dedup_view)
    else:
        spark.sql(f"SELECT * FROM {config.source_table}").createOrReplaceTempView(dedup_view)

    merge_sql = build_idempotent_merge_sql(config, tracked_columns, dedup_view)
    spark.sql(merge_sql)
