"""
upserts/basic_merge_pyspark.py — Basic INSERT/UPDATE MERGE (PySpark).

Module   : upserts.basic_merge_pyspark
Concept  : Foundational Delta MERGE INTO — the building block for all upsert patterns
When     : Any pipeline that needs to keep a target table in sync with a source:
           CDC feeds, dimension refreshes, lookup table updates, API-to-Delta sync.
Author   : <your name>
Version  : 1.0.0

What it is
----------
A Delta MERGE INTO with two clauses:
- WHEN MATCHED THEN UPDATE SET *   — overwrite all columns when key matches
- WHEN NOT MATCHED THEN INSERT *   — insert rows whose key is absent from target

This is the same operation as SCD Type 1, but framed as a standalone upsert
pattern rather than a dimension management pattern.  The distinction matters
for documentation: SCD connotes dimension tables; basic MERGE is used for
operational tables, lookup tables, and transactional data targets.

When to use
-----------
- Keeping a Delta table in sync with a changing source of truth.
- Incremental CDC loads where every source row is an authoritative "current
  state" record (no delete signalling needed).
- Idempotent batch re-runs: running the same source data twice leaves the
  target unchanged (MERGE is set-based, not accumulating).
- As the foundation to extend with delete, hash-guard, or recency-guard clauses.

When NOT to use
---------------
- Source rows signal deletes → delete_aware_merge.
- You need full history of changes → SCD Type 2.
- Source may contain duplicate keys → idempotent_merge (deduplicate first).
- Source may contain out-of-order records → late_arriving_merge.

MERGE vs overwrite vs append
-----------------------------
MERGE          Changes/inserts only; reads target to find matches.  Best when
               the batch covers only a fraction of the target (selective update).
               Higher per-row cost than overwrite due to match lookup.

overwrite      Replaces the entire table or a partition.  Best for full-refresh
               loads where the source is authoritative and re-reading the target
               is cheaper than the overhead of MERGE.  No match cost.

append         Adds rows unconditionally.  Best for event tables where each
               source delivery is new data with no overlap.  Cheapest write
               but requires the caller to guarantee no duplicates.

Delta / Databricks considerations
----------------------------------
- MERGE INTO requires at least one matched-or-not-matched clause.
- UPDATE SET *  requires source and target to share the same column set.
  Use explicit column lists when schemas diverge (see schema_evolution).
- MERGE is not equivalent to re-running overwrite if the target has rows
  absent from the source — those rows are left untouched by MERGE.
- On large tables, use ZORDER/Liquid Clustering on merge_keys to minimise
  the number of files Delta reads during the match phase.
- Requires DBR 8.0+ / Delta Lake 1.0+.

Public API
----------
apply_basic_merge(spark, df_source, config) -> None
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from upserts.config import UpsertConfig


def apply_basic_merge(
    spark: SparkSession,
    df_source: DataFrame,
    config: UpsertConfig,
) -> None:
    """
    Execute a basic INSERT/UPDATE MERGE INTO against a Delta target table.

    Parameters
    ----------
    spark : SparkSession
    df_source : DataFrame
        Incoming batch.  Schema must be compatible with the target.
    config : UpsertConfig
        target_table must be an existing Delta table.
        merge_keys drive the MERGE ON condition.

    Returns
    -------
    None

    Examples
    --------
        from upserts.config import UpsertConfig
        from upserts.basic_merge_pyspark import apply_basic_merge

        config = UpsertConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer",
            merge_keys=["customer_id"],
        )

        apply_basic_merge(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.merge_keys)

    (
        DeltaTable.forName(spark, config.target_table)
        .alias("t")
        .merge(df_source.alias("s"), join_expr)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
