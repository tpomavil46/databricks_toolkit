"""
upserts/idempotent_pyspark.py — Idempotent Upsert Patterns (PySpark).

Module   : upserts.idempotent_pyspark
Concept  : Make MERGE pipelines safe to re-run without side effects
When     : Any MERGE pipeline where: (a) the source may deliver duplicate keys
           in a single batch, or (b) re-running the same batch must produce
           exactly the same target state as running it once.
Author   : <your name>
Version  : 1.0.0

What idempotency means here
---------------------------
A MERGE is idempotent when:
1. Running it once and running it N times produce the same target state.
2. The source may have duplicate keys — we handle this before hitting MERGE
   (Delta MERGE raises an error or produces non-deterministic results when
   the source has multiple rows matching the same target row).

Two complementary patterns are implemented:

Pattern 1 — Source deduplication (deduplicate_source)
    Before MERGE, reduce the source to one row per merge_key set using
    ROW_NUMBER() or aggregation.  Prevents the "non-deterministic update"
    error that Delta raises when a single target row matches multiple source rows.

Pattern 2 — Content-hash change guard (add_content_hash + idempotent MERGE)
    Add a SHA-256 hash of all tracked columns to both source and target.
    In the MERGE, only update when the hash differs.  Unchanged rows pass
    through the match phase without generating a Delta write operation.
    Pro: Eliminates write amplification for stable rows in large batches.
    Pro: Makes the MERGE idempotent for repeated source deliveries.
    Con: Requires a hash column in the target schema.

When NOT to use
---------------
- Source is guaranteed to have unique keys per batch → dedup is unnecessary.
- Table is append-only (no MERGE) → these patterns don't apply.
- High-cardinality tables where hashing every row costs more than the write
  amplification you're trying to avoid — profile before adding hash columns.

Delta / Databricks considerations
----------------------------------
- Delta raises AnalysisException: "MERGE target table requires ON condition
  to be unique" when the source has duplicate merge keys.  This is NOT a
  bug — Delta cannot safely apply two conflicting updates to the same row.
  Always deduplicate before MERGE.
- Content hash guard: the hash column must be maintained by the MERGE.
  The UPDATE SET clause must include: target._content_hash = source._content_hash.
- Requires DBR 8.0+ / Delta Lake 1.0+.

Public API
----------
deduplicate_source(df, merge_keys, order_col, desc) -> DataFrame
    Pure transform: one row per key, ordered by order_col.

add_content_hash(df, columns, hash_col) -> DataFrame
    Pure transform: add SHA-256 hash column.

apply_idempotent_merge(spark, df_source, config) -> None
    MERGE with both dedup and hash guard.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from upserts.config import UpsertConfig


def deduplicate_source(
    df: DataFrame,
    merge_keys: List[str],
    order_col: str,
    desc: bool = True,
) -> DataFrame:
    """
    Reduce source to one row per merge_key combination, keeping the row
    with the highest (or lowest) value of order_col.

    Call this before every MERGE when the source may contain duplicate keys.
    Delta's MERGE is non-deterministic — or raises an error — when multiple
    source rows match the same target row.

    Parameters
    ----------
    df : DataFrame
        Source data that may contain duplicate merge keys.
    merge_keys : list[str]
        Columns that define uniqueness in the target.
    order_col : str
        Column used to pick which duplicate to keep.  Typically a timestamp
        (event_ts, received_at) or a sequence number (seq_num).
    desc : bool
        True (default): keep the row with the HIGHEST order_col value
        (latest record wins, standard for time-ordered CDC).
        False: keep the row with the LOWEST order_col value (first-write-wins).

    Returns
    -------
    DataFrame
        De-duplicated source: exactly one row per merge_key combination.

    Notes
    -----
    Uses ROW_NUMBER() window function.  Requires a shuffle.  For very large
    sources, consider coalescing on the merge key before calling this function.

    Examples
    --------
        # Latest record per customer wins
        df_deduped = deduplicate_source(df, ["customer_id"], "event_ts", desc=True)
    """
    order_expr = F.col(order_col).desc() if desc else F.col(order_col).asc()
    w = Window.partitionBy(*merge_keys).orderBy(order_expr)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_content_hash(
    df: DataFrame,
    columns: List[str],
    hash_col: str = "_content_hash",
) -> DataFrame:
    """
    Add a SHA-256 content hash column derived from the specified columns.

    The hash is deterministic: same column values always produce the same hash.
    Null values are replaced with '__null__' before hashing so that two rows
    with the same null pattern hash identically.

    Use this on both the source and target before MERGE.  In the MERGE,
    only update when s._content_hash != t._content_hash (content changed).

    Parameters
    ----------
    df : DataFrame
    columns : list[str]
        Columns whose values form the hash input.  Typically all non-key,
        non-system columns (the "business payload").
    hash_col : str
        Name of the output hash column.  Default: '_content_hash'.

    Returns
    -------
    DataFrame
        Input DataFrame with hash_col appended.

    Examples
    --------
        tracked_cols = ["name", "email", "tier"]
        df_hashed = add_content_hash(df, tracked_cols)
        # Now use: WHEN MATCHED AND t._content_hash != s._content_hash THEN UPDATE
    """
    coalesced = [F.coalesce(F.col(c).cast("string"), F.lit("__null__")) for c in columns]
    return df.withColumn(hash_col, F.sha2(F.concat_ws("|", *coalesced), 256))


def apply_idempotent_merge(
    spark: SparkSession,
    df_source: DataFrame,
    config: UpsertConfig,
    order_col: str = "",
) -> None:
    """
    Execute an idempotent MERGE: deduplicate source, then MERGE with hash guard.

    Steps:
    1. Deduplicate df_source on merge_keys (if order_col provided).
    2. Add content hash to deduplicated source.
    3. MERGE with UPDATE only when hash differs.

    Requires the target table to already have a content_hash_col column.
    Add it with: ALTER TABLE <target> ADD COLUMN _content_hash STRING.

    Parameters
    ----------
    spark : SparkSession
    df_source : DataFrame
    config : UpsertConfig
        content_hash_col: name of the hash column (default: '_content_hash').
    order_col : str
        Column for deduplication ordering.  If empty, deduplication is skipped
        (use only when source is guaranteed to have unique merge keys).

    Returns
    -------
    None

    Examples
    --------
        config = UpsertConfig(
            source_table="catalog.silver.customers_incoming",
            target_table="catalog.gold.dim_customer",
            merge_keys=["customer_id"],
        )

        apply_idempotent_merge(spark, df_source, config, order_col="received_at")
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    df = df_source
    if order_col:
        df = deduplicate_source(df, config.merge_keys, order_col)

    tracked_cols = config.resolve_update_columns(df.columns)
    df = add_content_hash(df, tracked_cols, config.content_hash_col)

    join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.merge_keys)
    hash_changed = f"t.{config.content_hash_col} != s.{config.content_hash_col}"

    (
        DeltaTable.forName(spark, config.target_table)
        .alias("t")
        .merge(df.alias("s"), join_expr)
        .whenMatchedUpdateAll(condition=hash_changed)
        .whenNotMatchedInsertAll()
        .execute()
    )
