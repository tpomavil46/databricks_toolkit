"""
scd/type3_pyspark.py — SCD Type 3: Previous Value Columns (PySpark).

Module   : scd.type3_pyspark
Concept  : SCD Type 3 — Store one previous value alongside the current value
When     : You need to answer "what was the previous state?" for a small set
           of attributes, but a full history (Type 2) is not justified.
Author   : <your name>
Version  : 1.0.0

What it is
----------
SCD Type 3 adds a 'prev_<col>' column for each tracked attribute alongside
the current column.  When a change is detected:
- prev_<col> ← current <col>  (old value is copied to prev_)
- <col>       ← new value

Only ONE previous value is retained.  If the attribute changes again, the
previous-previous value is lost.

When to use
-----------
- You need "what was the last known value?" for a few attributes.
- Business users want to see "previous tier" next to "current tier" in
  a report without writing a complex time-range JOIN.
- The dimension is relatively stable and the attribute rarely changes more
  than once per entity.

When NOT to use
---------------
- The attribute changes frequently → previous value is overwritten each batch,
  history is incomplete.
- You need full history → Type 2.
- You need more than one historical value → Type 2 or Type 4.

Trade-offs
----------
Pro: Simple schema (still one row per entity).  No is_current filter.
     Easy to query: SELECT current_tier, prev_tier FROM dim_customer.
Con: Only one previous value.  Third change loses the oldest previous value.
     Schema changes (adding new tracked columns) require ALTER TABLE.

Delta / Databricks considerations
----------------------------------
- Implemented as a MERGE INTO that explicitly sets prev_<col> = t.<col>
  (old target value) and <col> = s.<col> (new source value) on MATCH.
- Schema must pre-exist with prev_<col> columns.  Adding columns to an
  existing Delta table requires ALTER TABLE or mergeSchema=True.
- Requires DBR 8.0+ / Delta Lake 1.0+.

Public API
----------
build_update_with_prev(df_source, df_target_current, config) -> DataFrame
    Pure transform: builds a DataFrame with both current and prev_ columns
    for changed rows.  Testable without Delta.

apply_scd3(spark, df_source, config) -> None
    Execute Delta MERGE INTO.  Requires live Delta target.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scd.config import SCDConfig
from scd.surrogate_key import add_surrogate_key
from scd.type1_pyspark import classify_changes


def build_update_with_prev(
    df_source: DataFrame,
    df_target: DataFrame,
    config: SCDConfig,
) -> DataFrame:
    """
    Build the update DataFrame for Type 3: new values + captured prev_ values.

    Joins changed source rows with the current target values to extract the
    "old" values, then builds a row with both new values and prev_ columns.

    Parameters
    ----------
    df_source : DataFrame
        Incoming batch.
    df_target : DataFrame
        Current state of the target (one row per entity).
    config : SCDConfig
        prev_value_columns controls which columns get a prev_ counterpart.

    Returns
    -------
    DataFrame
        Rows that need to be written (new or changed), with prev_ columns
        populated for changed rows and NULL for new rows.
        Schema: business_keys + tracked_cols + prev_<col> for each prev col.
    """
    prev_cols = config.resolve_prev_columns(df_source.columns)
    prev_in_target = [c for c in prev_cols if c in df_target.columns]

    _, df_changed, _ = classify_changes(df_source, df_target, config)

    target_prev = df_target.select(
        *config.business_keys,
        *[F.col(c).alias(f"prev_{c}") for c in prev_in_target],
    )

    df_new_rows = df_source.join(
        df_target.select(*config.business_keys).distinct(),
        on=config.business_keys,
        how="left_anti",
    )
    for c in prev_in_target:
        df_new_rows = df_new_rows.withColumn(f"prev_{c}", F.lit(None).cast("string"))

    df_changed_with_prev = df_changed.join(target_prev, on=config.business_keys, how="left")

    return df_new_rows.unionByName(df_changed_with_prev, allowMissingColumns=True)


def apply_scd3(
    spark: SparkSession,
    df_source: DataFrame,
    config: SCDConfig,
) -> None:
    """
    Apply SCD Type 3: overwrite current values and shift old values to prev_ columns.

    Executes a Delta MERGE with explicit SET clauses that:
    - Copy old target values to prev_<col>
    - Write new source values to <col>

    Parameters
    ----------
    spark : SparkSession
    df_source : DataFrame
        Incoming batch.
    config : SCDConfig
        target_table must have prev_<col> columns pre-created.
        prev_value_columns controls which columns get prev_ tracking.

    Returns
    -------
    None

    Examples
    --------
        config = SCDConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer",
            business_keys=["customer_id"],
            tracked_columns=["email", "tier"],
            prev_value_columns=["tier"],  # only keep prev for tier
        )

        apply_scd3(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    prev_cols = config.resolve_prev_columns(df_source.columns)

    update_set = {}
    for c in prev_cols:
        update_set[f"prev_{c}"] = F.col(f"t.{c}")

    tracked = config.resolve_tracked(df_source.columns)
    for c in tracked:
        update_set[c] = F.col(f"s.{c}")

    df_to_write = df_source
    if config.surrogate_key_col:
        df_to_write = add_surrogate_key(df_source, config.business_keys, config.surrogate_key_col)

    join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.business_keys)

    insert_vals = {c: F.col(f"s.{c}") for c in df_to_write.columns}
    for c in prev_cols:
        insert_vals[f"prev_{c}"] = F.lit(None)

    (
        DeltaTable.forName(spark, config.target_table)
        .alias("t")
        .merge(df_to_write.alias("s"), join_expr)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )
