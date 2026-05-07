"""
scd/type4_pyspark.py — SCD Type 4: Separate History Table (PySpark).

Module   : scd.type4_pyspark
Concept  : SCD Type 4 — Current table (Type 1) + separate history table
When     : You need fast point-in-time lookups (current table is always
           Type 1 — single row per entity, no date filtering needed) AND
           you need full history preserved (in a separate history table).
Author   : <your name>
Version  : 1.0.0

What it is
----------
SCD Type 4 uses two tables:

1. Current table  (config.target_table)
   Always contains one row per entity with the latest attribute values.
   Updated via Type 1 MERGE (overwrite on match, insert on new key).

2. History table  (config.history_table)
   Append-only.  Every INSERT or UPDATE to the current table produces a
   corresponding row in the history table capturing:
   - The new attribute values
   - changed_at = current_timestamp()
   - change_type = 'INSERT' or 'UPDATE'
   - A surrogate key (scd_key)

When to use
-----------
- Query performance is critical for current-state lookups (no is_current
  filter required on the current table).
- History table is queried separately and less frequently.
- You want clean separation of concerns: operational queries hit the current
  table; analytical / audit queries hit the history table.
- Operational system replication patterns (CDC → current + audit log).

When NOT to use
---------------
- Fact-table time-range JOINs require Type 2 (single table with effective
  dates); Type 4 history rows don't have effective_end.
- Managing two tables adds operational complexity.

Trade-offs
----------
Pro: Current table is small and simple.  No is_current filter anywhere.
     History table is pure append — no MERGE write amplification there.
Con: Two tables to OPTIMIZE, VACUUM, and govern.  Queries that need to
     "travel back in time" must query history, which requires knowing which
     history row was active at a given point (there is no effective_end).

Delta / Databricks considerations
----------------------------------
- Current table: Delta MERGE INTO (Type 1 pattern).
- History table: Delta append write.  Enable OPTIMIZE on the history table
  with ZORDER(changed_at) for time-range queries.
- Both tables benefit from Change Data Feed (CDF) if downstream consumers
  need to react to changes.
- Requires DBR 8.0+ / Delta Lake 1.0+.

Public API
----------
build_history_rows(df_new, df_changed, config) -> DataFrame
    Pure transform: build the rows to append to the history table.
    Testable without Delta.

apply_scd4(spark, df_source, config) -> None
    Execute MERGE on current table + append to history table.
    Requires live Delta tables.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scd.config import SCDConfig
from scd.surrogate_key import add_surrogate_key
from scd.type1_pyspark import classify_changes


def build_history_rows(
    df_new: DataFrame,
    df_changed: DataFrame,
    config: SCDConfig,
) -> DataFrame:
    """
    Build rows to append to the history table.

    Adds:
    - scd_key     : surrogate key (hash of business_keys + changed_at)
    - changed_at  : timestamp when the change was recorded
    - change_type : 'INSERT' for new entities, 'UPDATE' for changed entities

    Parameters
    ----------
    df_new     : DataFrame
        Brand-new entities (no matching business key in current table).
    df_changed : DataFrame
        Changed entities (business key existed, values differ).
    config : SCDConfig

    Returns
    -------
    DataFrame
        Union of new and changed rows with history metadata columns appended.
    """
    ts_col = "changed_at"
    type_col = "change_type"

    df_new_tagged = (
        df_new
        .withColumn(ts_col, F.current_timestamp())
        .withColumn(type_col, F.lit("INSERT"))
    )
    df_changed_tagged = (
        df_changed
        .withColumn(ts_col, F.current_timestamp())
        .withColumn(type_col, F.lit("UPDATE"))
    )

    all_rows = df_new_tagged.unionByName(df_changed_tagged, allowMissingColumns=True)

    if config.surrogate_key_col:
        sk_cols = config.business_keys + [ts_col]
        all_rows = add_surrogate_key(all_rows, sk_cols, config.surrogate_key_col)

    return all_rows


def apply_scd4(
    spark: SparkSession,
    df_source: DataFrame,
    config: SCDConfig,
) -> None:
    """
    Apply SCD Type 4: update current table (Type 1) + append to history table.

    Step 1: Classify source rows (new / changed / unchanged).
    Step 2: MERGE INTO current table — overwrite on match, insert on new key.
    Step 3: Append new and changed rows to the history table.

    Parameters
    ----------
    spark : SparkSession
        Active session connected to a Delta-capable environment.
    df_source : DataFrame
        Incoming batch from the source.
    config : SCDConfig
        target_table  — current Delta table (one row per entity).
        history_table — append-only Delta history table.
        history_table must be non-empty.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If config.history_table is not set.

    Examples
    --------
        from scd.config import SCDConfig
        from scd.type4_pyspark import apply_scd4

        config = SCDConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer_current",
            history_table="catalog.gold.dim_customer_history",
            business_keys=["customer_id"],
            tracked_columns=["email", "tier"],
            surrogate_key_col="scd_key",
        )

        apply_scd4(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    if not config.history_table:
        raise ValueError("SCDConfig.history_table must be set for Type 4")

    df_target = spark.table(config.target_table)
    df_new, df_changed, _ = classify_changes(df_source, df_target, config)

    # Step 2: Merge into current table (Type 1 pattern)
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

    # Step 3: Append to history table
    if not df_new.isEmpty() or not df_changed.isEmpty():
        history_rows = build_history_rows(df_new, df_changed, config)
        history_rows.write.format("delta").mode("append").saveAsTable(config.history_table)
