"""
scd/type1_pyspark.py — SCD Type 1: Overwrite (PySpark).

Module   : scd.type1_pyspark
Concept  : SCD Type 1 — Always keep the latest value, no history retained
When     : Correcting data errors; attributes where only current state matters
           (status, last_login_date, current_address).  Most common SCD type.
Author   : <your name>
Version  : 1.0.0

What it is
----------
SCD Type 1 overwrites the existing row when the source sends an updated value.
No history of previous values is kept.  New entities are inserted.

When to use
-----------
- The historical value has no analytical value: correcting a data entry error,
  updating a phone number after a customer calls in.
- Storage efficiency matters and the full change history is not needed.
- The downstream query always wants the current state (e.g. "what tier is
  this customer right now?").
- As the "current" table in a Type 4 SCD (paired with a history table).

When NOT to use
---------------
- You need to answer "what was the customer's tier in Q1?" → Type 2.
- You need to keep one previous value → Type 3.
- The attribute is legally immutable → Type 0.

Trade-offs
----------
Pro: Simple.  One row per entity — easy to query, no fan-out in JOINs,
     no is_current filter required.
Con: Overwrites original values permanently.  No time-travel query possible
     (unless you use Delta time travel, but that's at the table level, not
     per-entity).

Delta / Databricks considerations
----------------------------------
- Uses MERGE INTO with UPDATE SET * on match and INSERT * on no-match.
- 'UPDATE SET *' requires source and target to have the same column set.
  If schemas diverge (schema evolution), use explicit column mapping or
  enable schema evolution on the target table.
- Delta automatically compacts small files on OPTIMIZE; run OPTIMIZE after
  large MERGE batches to avoid small-file accumulation.
- Requires DBR 8.0+ / Delta Lake 1.0+.

Public API
----------
classify_changes(df_source, df_target, config)
    -> (df_new, df_changed, df_unchanged)
    Pure transform, testable without Delta.

apply_scd1(spark, df_source, config) -> None
    Execute Delta MERGE INTO.  Requires live Delta target.
"""

from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scd.config import SCDConfig
from scd.surrogate_key import add_surrogate_key


def classify_changes(
    df_source: DataFrame,
    df_target: DataFrame,
    config: SCDConfig,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Classify source rows into new, changed, and unchanged relative to target.

    This is the core logic shared by Type 1 and used as a building block
    for Types 3, 4, and 6.  It is a pure DataFrame transformation with no
    side effects — safe to call in unit tests without a Delta table.

    Parameters
    ----------
    df_source : DataFrame
        Incoming batch.
    df_target : DataFrame
        Current state of the target dimension (all rows, not just current).
        For Type 1 there is only one row per entity.
    config : SCDConfig
        Drives business_keys and tracked_columns.

    Returns
    -------
    (df_new, df_changed, df_unchanged)
        df_new       : rows where business key does not exist in target.
        df_changed   : rows where business key exists AND tracked cols differ.
        df_unchanged : rows where business key exists AND tracked cols match.

    Notes
    -----
    Null-safe comparison (eqNullSafe) ensures:
    - NULL → 'value'  →  change detected
    - 'value' → NULL  →  change detected
    - NULL → NULL     →  no change (treated as equal)
    """
    tracked = config.resolve_tracked(df_source.columns)
    tracked_in_target = [c for c in tracked if c in df_target.columns]

    df_new = df_source.join(
        df_target.select(*config.business_keys).distinct(),
        on=config.business_keys,
        how="left_anti",
    )

    df_existing_src = df_source.join(
        df_target.select(*config.business_keys).distinct(),
        on=config.business_keys,
        how="left_semi",
    )

    if not tracked_in_target:
        return df_new, df_source.limit(0), df_existing_src

    target_renamed = df_target.select(
        *config.business_keys,
        *[F.col(c).alias(f"_tgt_{c}") for c in tracked_in_target],
    )

    joined = df_existing_src.join(target_renamed, on=config.business_keys, how="inner")

    change_cond = F.lit(False)
    for c in tracked_in_target:
        change_cond = change_cond | (~F.col(c).eqNullSafe(F.col(f"_tgt_{c}")))

    df_changed = joined.filter(change_cond).select(df_source.columns)
    df_unchanged = joined.filter(~change_cond).select(df_source.columns)

    return df_new, df_changed, df_unchanged


def apply_scd1(
    spark: SparkSession,
    df_source: DataFrame,
    config: SCDConfig,
) -> None:
    """
    Apply SCD Type 1: overwrite changed values, insert new rows.

    Executes a Delta MERGE INTO with:
    - WHEN MATCHED THEN UPDATE SET *   (overwrite changed + unchanged rows)
    - WHEN NOT MATCHED THEN INSERT *   (insert new rows)

    Parameters
    ----------
    spark : SparkSession
        Active session connected to a Delta-capable environment.
    df_source : DataFrame
        Incoming batch from the source.
    config : SCDConfig
        target_table must be an existing Delta table.
        surrogate_key_col: if non-empty, a SHA-256 key derived from
        business_keys is added before writing.

    Returns
    -------
    None

    Examples
    --------
        from scd.config import SCDConfig
        from scd.type1_pyspark import apply_scd1

        config = SCDConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer",
            business_keys=["customer_id"],
            tracked_columns=["email", "tier"],
        )

        apply_scd1(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    df_to_write = df_source
    if config.surrogate_key_col:
        df_to_write = add_surrogate_key(df_source, config.business_keys, config.surrogate_key_col)

    join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.business_keys)

    (
        DeltaTable.forName(spark, config.target_table)
        .alias("t")
        .merge(df_to_write.alias("s"), join_expr)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
