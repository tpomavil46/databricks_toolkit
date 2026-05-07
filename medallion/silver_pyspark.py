"""
medallion/silver_pyspark.py — Silver Layer (PySpark).

Module   : medallion.silver_pyspark
Concept  : Cleansed, conformed, deduplicated records — one authoritative row per key
When     : After Bronze write.  Read from Bronze, apply business rules, write Silver.

What it is
----------
Silver is the "trusted" layer.  Rules that Bronze relaxes are enforced here:
  - No duplicate business keys (deduplication by id_col + ts_col).
  - No bronze metadata overhead (_ingest_ts, _batch_id, _source_file dropped).
  - Business rules applied: type casting, null handling, value normalization.
  - Conforms to a declared schema (Silver should have a defined StructType).

Silver writes use MERGE (upsert), not append, so re-processing Bronze is safe.
Re-running the Silver job from the same Bronze data produces the same Silver table
— this is the idempotency guarantee.

Silver vs Bronze deduplication
-------------------------------
Bronze keeps all received records (audit trail).  Silver deduplicates:
    - If the same business key appears in multiple Bronze rows (re-delivery or
      late-arriving data), Silver picks the row with the latest ts_col value.
    - Ties on ts_col are broken by _ingest_ts (latest ingest wins).

Public API
----------
drop_bronze_metadata(df) -> DataFrame
deduplicate_silver(df, id_col, ts_col) -> DataFrame
apply_silver_transform(df, config, transform_fn) -> DataFrame
make_silver_upsert_batch_fn(config, transform_fn) -> Callable
"""

from __future__ import annotations

from typing import Callable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from medallion.config import BRONZE_METADATA_COLS, MedallionConfig


def drop_bronze_metadata(df: DataFrame) -> DataFrame:
    """
    Remove Bronze metadata columns from a DataFrame.

    Safe to call even if some metadata columns are absent — only drops
    columns that actually exist in df.

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    DataFrame
        Input with _ingest_ts, _batch_id, _source_file dropped.
    """
    to_drop = [c for c in BRONZE_METADATA_COLS if c in df.columns]
    return df.drop(*to_drop)


def deduplicate_silver(
    df: DataFrame,
    id_col: str,
    ts_col: str,
) -> DataFrame:
    """
    Keep the latest row per id_col, ordered by ts_col descending.

    When multiple rows share the same id_col value (re-delivery, late arrival),
    the one with the greatest ts_col value survives.  Ties are broken by
    _ingest_ts if present, otherwise by row order.

    Parameters
    ----------
    df : DataFrame
    id_col : str
        Business key column.  Use a list via Window.partitionBy for composites.
    ts_col : str
        Timestamp or sequence column that determines recency.

    Returns
    -------
    DataFrame
        One row per id_col value (the latest).

    Notes
    -----
    This uses a window function (_rn), not a groupBy, so all columns are
    preserved — no need to list columns explicitly.
    """
    order_cols = [F.col(ts_col).desc()]
    if "_ingest_ts" in df.columns:
        order_cols.append(F.col("_ingest_ts").desc())

    w = Window.partitionBy(id_col).orderBy(*order_cols)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def apply_silver_transform(
    df: DataFrame,
    config: MedallionConfig,
    transform_fn: Optional[Callable[[DataFrame], DataFrame]] = None,
) -> DataFrame:
    """
    Apply the full Silver transformation pipeline to a Bronze DataFrame.

    Steps (in order):
      1. Drop bronze metadata columns.
      2. Apply caller-supplied transform_fn (casting, normalisation, business rules).
      3. Deduplicate by id_col + ts_col (if both are configured).

    Parameters
    ----------
    df : DataFrame
        Bronze DataFrame (may include _ingest_ts, _batch_id, _source_file).
    config : MedallionConfig
    transform_fn : callable | None
        (DataFrame) -> DataFrame.  Apply type casts, null coalescing, column
        renames, and business-rule filters here.  None = identity (no transform).

    Returns
    -------
    DataFrame
        Silver-ready DataFrame.
    """
    result = drop_bronze_metadata(df)
    if transform_fn is not None:
        result = transform_fn(result)
    if config.id_col and config.ts_col:
        result = deduplicate_silver(result, config.id_col, config.ts_col)
    return result


def make_silver_upsert_batch_fn(
    config: MedallionConfig,
    transform_fn: Optional[Callable[[DataFrame], DataFrame]] = None,
) -> Callable[[DataFrame, int], None]:
    """
    Return a foreachBatch function that upserts each micro-batch into Silver.

    Reads from the streaming Bronze source (via readStream on Bronze table),
    applies Silver transformations, and MERGEs into Silver using config.id_col
    as the match condition.

    Requires config.id_col to be set — Silver MERGE is keyed on the business key.

    Parameters
    ----------
    config : MedallionConfig
    transform_fn : callable | None

    Returns
    -------
    Callable[[DataFrame, int], None]
    """
    if not config.id_col:
        raise ValueError(
            "MedallionConfig.id_col must be set for Silver upsert foreachBatch"
        )

    def _fn(df_batch: DataFrame, batch_id: int) -> None:
        from delta.tables import DeltaTable  # noqa: PLC0415

        silver_df = apply_silver_transform(df_batch, config, transform_fn)
        spark = df_batch.sparkSession

        if DeltaTable.isDeltaTable(spark, config.silver_table.replace(".", "/")):
            silver = DeltaTable.forName(spark, config.silver_table)
            (
                silver.alias("t")
                .merge(
                    silver_df.alias("s"),
                    f"t.{config.id_col} = s.{config.id_col}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            silver_df.write.format("delta").mode("overwrite").saveAsTable(config.silver_table)

    return _fn
