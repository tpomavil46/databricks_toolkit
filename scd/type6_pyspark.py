"""
scd/type6_pyspark.py — SCD Type 6: Hybrid (Types 1 + 2 + 3) (PySpark).

Module   : scd.type6_pyspark
Concept  : SCD Type 6 — Hybrid combining Type 1 current values, Type 2
           full history rows, and Type 3 previous-value columns
When     : You need everything: full history (Type 2) AND the current_<col>
           shortcut on historical rows (Type 1 denormalisation) AND a
           prev_<col> on the current row (Type 3) — in a single table.
Author   : <your name>
Version  : 1.0.0

What it is
----------
SCD Type 6 (also called "Super Type 2") combines:

Type 2  All rows carry effective_start, effective_end, is_current.  A new
        version row is inserted on every change.

Type 1  A "current_<col>" column is added alongside each tracked column.
        Every row in the table — including historical ones — has the LATEST
        value of each tracked attribute in current_<col>.  This allows
        "what is this customer's current tier?" without filtering to
        is_current = True.

Type 3  The active row (is_current = True) also carries prev_<col> columns
        holding the previous values before the most recent change.

When to use
-----------
- You have sophisticated analytics that need all three perspectives: current
  state, historical state at a point in time, and what changed most recently.
- You're building a data warehouse where dimension designers want to avoid
  complex JOINs while still supporting full history.
- Rarely justified in practice — evaluate Type 2 alone first.

When NOT to use
---------------
- The added schema complexity (current_ + prev_ columns alongside base
  columns) is not worth the query convenience → use Type 2.
- Storage budget is constrained — Type 6 is the most storage-intensive SCD.

Trade-offs
----------
Pro: Maximum flexibility.  Analysts can query without worrying about
     effective dates (use current_<col>) or with dates (use <col> + date
     range filter).
Con: Most complex schema.  Every attribute change requires:
     1. Expiring the old current row (set effective_end, is_current = false,
        current_<col> updated to latest).
     2. Inserting a new current row.
     3. Updating all historical rows for this entity to reflect the new
        current value in current_<col>.  This step is expensive at scale.

Delta / Databricks considerations
----------------------------------
- Step 3 (propagating current_<col> to all history rows) requires either:
  a) A separate MERGE INTO that updates all is_current = false rows for
     changed entities — this is a potentially large write.
  b) Computing current_<col> at query time using a window function:
     LAST_VALUE(col) OVER (PARTITION BY business_key ORDER BY effective_start
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING).
     This avoids the write but adds query complexity.
  This implementation uses approach (b) to avoid unbounded write amplification.
  Override _build_current_col_expr() to switch to approach (a).

- Liquid Clustering on business_key is strongly recommended for the target
  table given the mixed access patterns (point-in-time + current-state).
  Requires DBR 13.3+.

Public API
----------
classify_changes(df_source, df_current, config)
    -> (df_new, df_changed)
    Identical to Type 2 classify (reused from type2_pyspark).

build_scd6_new_versions(df_new, df_changed, df_target_full, config)
    -> DataFrame
    Build rows to insert: Type 2 structure + prev_ columns.

apply_scd6(spark, df_source, config) -> None
    Full three-step write.  Requires live Delta target.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scd.config import SCDConfig
from scd.surrogate_key import add_surrogate_key
from scd.type2_pyspark import classify_changes


def build_scd6_new_versions(
    df_new: DataFrame,
    df_changed: DataFrame,
    df_target_full: DataFrame,
    config: SCDConfig,
) -> DataFrame:
    """
    Build new version rows for Type 6: Type 2 structure + prev_ columns.

    For changed entities, prev_<col> is populated from the current target row.
    For new entities, prev_<col> is NULL.
    current_<col> is always the new source value (Type 1 denormalisation).

    Parameters
    ----------
    df_new : DataFrame
        Brand-new entities (from classify_changes).
    df_changed : DataFrame
        Changed entities (from classify_changes).
    df_target_full : DataFrame
        Full target table — needed to extract prev_ values from current rows.
    config : SCDConfig

    Returns
    -------
    DataFrame
        Rows ready to INSERT into the target.
    """
    prev_cols = config.resolve_prev_columns(df_new.columns)

    df_target_current = df_target_full.filter(
        F.col(config.is_current_col) == True  # noqa: E712
    )

    target_prev = df_target_current.select(
        *config.business_keys,
        *[F.col(c).alias(f"prev_{c}") for c in prev_cols if c in df_target_current.columns],
    )

    df_new_with_prev = df_new
    for c in prev_cols:
        df_new_with_prev = df_new_with_prev.withColumn(f"prev_{c}", F.lit(None).cast("string"))

    df_changed_with_prev = df_changed.join(target_prev, on=config.business_keys, how="left")

    all_rows = df_new_with_prev.unionByName(df_changed_with_prev, allowMissingColumns=True)

    all_rows = (
        all_rows
        .withColumn(config.effective_start_col, F.current_timestamp())
        .withColumn(config.effective_end_col, F.lit(None).cast("timestamp"))
        .withColumn(config.is_current_col, F.lit(True))
    )

    if config.surrogate_key_col:
        sk_cols = config.business_keys + [config.effective_start_col]
        all_rows = add_surrogate_key(all_rows, sk_cols, config.surrogate_key_col)

    return all_rows


def apply_scd6(
    spark: SparkSession,
    df_source: DataFrame,
    config: SCDConfig,
) -> None:
    """
    Apply SCD Type 6: full history (Type 2) + current_ columns + prev_ columns.

    Steps:
    1. Classify source rows (new / changed).
    2. Expire current rows for changed entities (same as Type 2 Step 1).
    3. Insert new version rows with prev_ columns and SCD2 metadata.

    The current_<col> pattern is computed at query time via a window function
    rather than denormalising at write time (see module docstring for rationale).

    Parameters
    ----------
    spark : SparkSession
    df_source : DataFrame
    config : SCDConfig
        target_table must have columns: business_keys, tracked_columns,
        prev_<col> for each prev column, effective_start_col, effective_end_col,
        is_current_col, and optionally surrogate_key_col.

    Returns
    -------
    None

    Examples
    --------
        from scd.config import SCDConfig
        from scd.type6_pyspark import apply_scd6

        config = SCDConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer",
            business_keys=["customer_id"],
            tracked_columns=["email", "tier"],
            prev_value_columns=["tier"],
            surrogate_key_col="scd_key",
        )

        apply_scd6(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    df_target_full = spark.table(config.target_table)
    df_current = df_target_full.filter(F.col(config.is_current_col) == True)  # noqa: E712

    df_new, df_changed = classify_changes(df_source, df_current, config)

    if df_changed.isEmpty() and df_new.isEmpty():
        return

    if not df_changed.isEmpty():
        join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.business_keys)
        expire_set = {
            config.effective_end_col: F.current_timestamp(),
            config.is_current_col: F.lit(False),
        }
        (
            DeltaTable.forName(spark, config.target_table)
            .alias("t")
            .merge(
                df_changed.alias("s"),
                f"{join_expr} AND t.{config.is_current_col} = true",
            )
            .whenMatchedUpdate(set=expire_set)
            .execute()
        )

    new_versions = build_scd6_new_versions(df_new, df_changed, df_target_full, config)
    new_versions.write.format("delta").mode("append").saveAsTable(config.target_table)


def add_current_value_columns(
    df: DataFrame,
    tracked_columns: list,
    business_key: str,
    effective_start_col: str = "effective_start",
) -> DataFrame:
    """
    Add current_<col> to all rows using a window function (query-time approach).

    This is an alternative to writing current_<col> at MERGE time.  Call this
    inside a view or a Gold layer query to expose Type 1 semantics on a Type 2
    table without the write overhead.

    Parameters
    ----------
    df : DataFrame
        Full Type 2/6 dimension table (all history rows).
    tracked_columns : list[str]
        Columns for which to compute current_<col>.
    business_key : str
        Partition column for the window (entity identifier).
    effective_start_col : str
        Used as the ORDER BY column to identify the latest row.

    Returns
    -------
    DataFrame
        Input DataFrame with current_<col> columns appended.
    """
    w = (
        Window.partitionBy(business_key)
        .orderBy(F.col(effective_start_col).desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    for c in tracked_columns:
        df = df.withColumn(f"current_{c}", F.first(F.col(c)).over(w))
    return df
