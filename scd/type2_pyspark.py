"""
scd/type2_pyspark.py — SCD Type 2: Full History with Effective Dates (PySpark).

Module   : scd.type2_pyspark
Concept  : SCD Type 2 — Track complete history with effective date ranges
When     : You need to answer time-variant questions: "What was this customer's
           tier in Q3 last year?", "Which salesperson owned this account when
           this order was placed?"
Author   : <your name>
Version  : 1.0.0

What it is
----------
SCD Type 2 keeps full history by never updating existing rows.  When a change
is detected, the current active row is "closed" (effective_end is set,
is_current = False) and a new row is inserted with:
- The new attribute values
- effective_start = now
- effective_end = NULL (open-ended)
- is_current = True
- A new surrogate key (derived from business_key + effective_start)

The result is that a single business entity (customer_id = 'C001') has
multiple rows in the dimension, each representing a valid time window.

When to use
-----------
- Reporting requires point-in-time accuracy against dimension attributes.
- Fact table rows must JOIN to the dimension version that was active when
  the fact occurred (e.g. a sale joined to the customer's tier at sale time).
- Regulatory / audit requirements mandate that original values are preserved.

When NOT to use
---------------
- Only the current state matters → Type 1 (cheaper, simpler queries).
- You only need one previous value → Type 3.
- Your dimension is very high-cardinality and changes frequently (e.g.
  inventory price per product per day) → consider partitioned snapshots instead.

Trade-offs
----------
Pro: Complete history. Point-in-time joins work correctly by filtering
     effective_start <= fact_date AND (effective_end > fact_date OR is_current).
Con: Multiple rows per entity. Queries must always filter on is_current or
     use a date range condition — forgetting this causes fan-out.
     Higher storage and write cost per batch than Type 1.

Delta / Databricks considerations
----------------------------------
- Two-step write pattern:
    Step 1: MERGE — expire rows where business key exists and values changed.
    Step 2: INSERT (append) — insert new versions for both changed and new entities.
  Two separate operations are required because Delta MERGE INTO cannot both
  UPDATE an existing row and INSERT a new row for the same matched key in a
  single MERGE (that would require MERGE with multiple rows output per match,
  which SQL MERGE does not support).

- Delta time travel provides a safety net: if a batch was applied incorrectly,
  you can restore to the previous version with RESTORE TABLE.

- Z-ORDER on business_key + is_current dramatically speeds up the MERGE probe
  for large Type 2 tables.  Run OPTIMIZE WITH ZORDER(...) periodically.

- Requires DBR 8.0+ / Delta Lake 1.0+ for MERGE INTO.

- Liquid Clustering (DBR 13.3+) on business_key columns is a strong alternative
  to Z-ORDER for tables that are frequently OPTIMIZEd: no ZORDER spec needed
  and the clustering is maintained incrementally.

Public API
----------
classify_changes(df_source, df_current, config)
    -> (df_new, df_changed)
    Pure transform.  df_current must contain only is_current=True rows.

build_new_versions(df_new, df_changed, config) -> DataFrame
    Attach effective_start, effective_end, is_current, scd_key to both
    new and changed rows.  Pure transform.

apply_scd2(spark, df_source, config) -> None
    Full two-step Delta write.  Requires live Delta target.
"""

from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scd.config import SCDConfig
from scd.surrogate_key import add_surrogate_key


def classify_changes(
    df_source: DataFrame,
    df_current: DataFrame,
    config: SCDConfig,
) -> Tuple[DataFrame, DataFrame]:
    """
    Classify source rows into new entities and changed entities.

    Parameters
    ----------
    df_source : DataFrame
        Incoming batch from the source system.
    df_current : DataFrame
        Only the currently-active rows from the target (is_current = True).
        Filtering to current rows before calling this function avoids joining
        against the full history, which can be very large.
    config : SCDConfig
        business_keys and tracked_columns drive classification.

    Returns
    -------
    (df_new, df_changed)
        df_new     : source rows whose business key is not in df_current.
        df_changed : source rows whose business key IS in df_current AND at
                     least one tracked column differs.

    Notes
    -----
    Unchanged rows (key exists, values identical) are not returned —
    Type 2 skips them entirely.  They do not produce a new row version.
    """
    tracked = config.resolve_tracked(df_source.columns)
    tracked_in_current = [c for c in tracked if c in df_current.columns]

    df_new = df_source.join(
        df_current.select(*config.business_keys).distinct(),
        on=config.business_keys,
        how="left_anti",
    )

    df_existing_src = df_source.join(
        df_current.select(*config.business_keys).distinct(),
        on=config.business_keys,
        how="left_semi",
    )

    if not tracked_in_current:
        return df_new, df_source.limit(0)

    current_renamed = df_current.select(
        *config.business_keys,
        *[F.col(c).alias(f"_cur_{c}") for c in tracked_in_current],
    )

    joined = df_existing_src.join(current_renamed, on=config.business_keys, how="inner")

    change_cond = F.lit(False)
    for c in tracked_in_current:
        change_cond = change_cond | (~F.col(c).eqNullSafe(F.col(f"_cur_{c}")))

    df_changed = joined.filter(change_cond).select(df_source.columns)

    return df_new, df_changed


def build_new_versions(
    df_new: DataFrame,
    df_changed: DataFrame,
    config: SCDConfig,
) -> DataFrame:
    """
    Build the rows to insert into the Type 2 target table.

    Attaches SCD metadata columns to both brand-new entities and new versions
    of changed entities:
    - effective_start = current_timestamp()
    - effective_end   = NULL (open-ended — this is the current version)
    - is_current      = True
    - scd_key         = sha2(business_keys + effective_start)

    Parameters
    ----------
    df_new     : New-entity rows from classify_changes().
    df_changed : Changed-entity rows from classify_changes().
    config     : SCDConfig.

    Returns
    -------
    DataFrame
        Union of df_new and df_changed with SCD metadata columns appended.
        Schema: source columns + effective_start + effective_end +
                is_current + scd_key (if surrogate_key_col set).
    """
    all_rows = df_new.unionByName(df_changed, allowMissingColumns=True)

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


def apply_scd2(
    spark: SparkSession,
    df_source: DataFrame,
    config: SCDConfig,
) -> None:
    """
    Apply SCD Type 2: full history with effective dates.

    Two-step process:
    1. MERGE — expire current rows for changed entities (set effective_end,
       is_current = False).  New entities are skipped in this step.
    2. Append — insert new row versions for both new and changed entities.

    Parameters
    ----------
    spark : SparkSession
        Active session connected to a Delta-capable environment.
    df_source : DataFrame
        Incoming batch from the source.
    config : SCDConfig
        target_table must be an existing Delta table with columns:
        business_keys, tracked_columns, effective_start_col, effective_end_col,
        is_current_col, and optionally surrogate_key_col.

    Returns
    -------
    None

    Examples
    --------
        from scd.config import SCDConfig
        from scd.type2_pyspark import apply_scd2

        config = SCDConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer",
            business_keys=["customer_id"],
            tracked_columns=["email", "tier"],
            surrogate_key_col="scd_key",
        )

        apply_scd2(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    df_current = spark.table(config.target_table).filter(
        F.col(config.is_current_col) == True  # noqa: E712
    )

    df_new, df_changed = classify_changes(df_source, df_current, config)

    if df_changed.isEmpty():
        if not df_new.isEmpty():
            new_versions = build_new_versions(df_new, df_source.limit(0), config)
            new_versions.write.format("delta").mode("append").saveAsTable(config.target_table)
        return

    # Step 1: expire current rows for changed entities
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

    # Step 2: insert new versions
    new_versions = build_new_versions(df_new, df_changed, config)
    new_versions.write.format("delta").mode("append").saveAsTable(config.target_table)
