"""
scd/type0_pyspark.py — SCD Type 0: Fixed / Preserve Original (PySpark).

Module   : scd.type0_pyspark
Concept  : SCD Type 0 — Fixed dimension attributes
When     : Attributes that are immutable by definition — original sign-up date,
           first-assigned account number, birth date, original credit score.
           The dimension grows (new entities are inserted) but existing rows
           are never modified regardless of what the source sends.
Author   : <your name>
Version  : 1.0.0

What it is
----------
SCD Type 0 is the simplest SCD strategy: do nothing when the source sends an
updated value for an existing row.  Only new business keys trigger an INSERT.
Changes to known keys are silently discarded — or, if you pass an audit
callback, logged to a quarantine / rejected-changes table.

When to use
-----------
- The attribute captures the state at first encounter (e.g. original_channel,
  signup_cohort, first_order_date).
- Regulatory contexts where overwriting original values is prohibited.
- Stub dimensions that are populated once from a reference file and never
  refreshed from the operational system.

When NOT to use
---------------
- You need the latest value → Type 1.
- You need full version history → Type 2.
- You need one previous value alongside current → Type 3.

Trade-offs
----------
Pro: Lowest write amplification — no MERGE update path fires.
     Simple reasoning: "if it's in the table, it's the original value."
Con: Source changes are silently dropped.  Callers must consume the returned
     rejected_df to audit what was discarded.

Delta / Databricks considerations
----------------------------------
MERGE INTO with only WHEN NOT MATCHED fires only the INSERT path.  Delta
skips the update path entirely at the executor level — no write amplification
for matched rows.  On large tables this is noticeably cheaper than a full
overwrite.

Requires DBR 8.0+ / Delta Lake 1.0+ for MERGE INTO.

Public API
----------
detect_rejected_changes(df_source, df_target, config) -> DataFrame
    Pure transform: returns source rows that WOULD have changed an existing
    row.  Testable without Delta.  Use the result to write to an audit table.

apply_scd0(spark, df_source, config) -> DataFrame
    Executes the Type 0 MERGE.  Requires a live Delta target table.
    Returns the rejected changes DataFrame for the caller to handle.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scd.config import SCDConfig
from scd.surrogate_key import add_surrogate_key


def detect_rejected_changes(
    df_source: DataFrame,
    df_target: DataFrame,
    config: SCDConfig,
) -> DataFrame:
    """
    Identify source rows that would update an existing target row.

    These rows match on business_keys but differ on at least one
    tracked_column.  Type 0 never applies them; this function surfaces them
    so the caller can log or route them to a quarantine table.

    Change detection uses null-safe equality (eqNullSafe) so that:
    - NULL → 'value'  is detected as a change
    - 'value' → NULL  is detected as a change
    - NULL  → NULL    is NOT detected as a change

    Parameters
    ----------
    df_source : DataFrame
        Incoming batch from the source system.
    df_target : DataFrame
        Current state of the target dimension table.
    config : SCDConfig
        SCD configuration.

    Returns
    -------
    DataFrame
        Source rows that exist in target (matched on business_keys) AND
        differ on at least one tracked column.  Schema = df_source schema.
        Empty DataFrame (same schema) when no changes exist.
    """
    tracked = config.resolve_tracked(df_source.columns)
    tracked_in_target = [c for c in tracked if c in df_target.columns]

    if not tracked_in_target:
        return df_source.limit(0)

    target_renamed = df_target.select(
        *config.business_keys,
        *[F.col(c).alias(f"_tgt_{c}") for c in tracked_in_target],
    )

    joined = df_source.join(target_renamed, on=config.business_keys, how="inner")

    change_cond = F.lit(False)
    for c in tracked_in_target:
        change_cond = change_cond | (~F.col(c).eqNullSafe(F.col(f"_tgt_{c}")))

    return joined.filter(change_cond).select(df_source.columns)


def apply_scd0(
    spark: SparkSession,
    df_source: DataFrame,
    config: SCDConfig,
) -> DataFrame:
    """
    Apply SCD Type 0: insert new rows only, silently drop updates.

    Executes a Delta MERGE INTO with only a WHEN NOT MATCHED INSERT clause.
    Existing rows are never touched.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session connected to a Delta-capable environment.
    df_source : DataFrame
        Incoming batch from the source system.
    config : SCDConfig
        source_table and target_table must both be registered Delta tables.
        surrogate_key_col: if non-empty, a SHA-256 key is added from
        business_keys before writing.

    Returns
    -------
    DataFrame
        Rejected changes — source rows that matched an existing target row.
        Write this to an audit table if you need a full audit trail of
        what Type 0 discarded.

    Notes
    -----
    delta.tables.DeltaTable is imported at call time so that module-level
    import does not fail in local unit-test environments where delta-spark
    is not installed.

    Examples
    --------
        from scd.config import SCDConfig
        from scd.type0_pyspark import apply_scd0

        config = SCDConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer",
            business_keys=["customer_id"],
            tracked_columns=["email", "tier"],
        )

        rejected = apply_scd0(spark, df_source, config)
        if rejected.count() > 0:
            rejected.write.format("delta").mode("append").saveAsTable(
                "catalog.audit.scd0_rejected_changes"
            )
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    df_target = spark.table(config.target_table)
    rejected = detect_rejected_changes(df_source, df_target, config)

    df_to_write = df_source
    if config.surrogate_key_col:
        df_to_write = add_surrogate_key(df_source, config.business_keys, config.surrogate_key_col)

    join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.business_keys)

    (
        DeltaTable.forName(spark, config.target_table)
        .alias("t")
        .merge(df_to_write.alias("s"), join_expr)
        .whenNotMatchedInsertAll()
        .execute()
    )

    return rejected
