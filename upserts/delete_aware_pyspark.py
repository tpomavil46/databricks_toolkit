"""
upserts/delete_aware_pyspark.py — INSERT / UPDATE / DELETE MERGE (PySpark).

Module   : upserts.delete_aware_pyspark
Concept  : Full CDC MERGE: handle inserts, updates, and deletes in one pass
When     : Source is a CDC stream or snapshot that includes a delete-signal
           column (op code, is_deleted flag, tombstone record).
Author   : <your name>
Version  : 1.0.0

What it is
----------
Extends the basic MERGE with a WHEN MATCHED AND <delete_condition> THEN DELETE
clause.  The source delivers a "delete indicator" — a column value meaning
"this row should be removed from the target."

Common CDC patterns supported:
  op-code column      'op' = 'D' → delete, 'op' in ('I','U') → upsert
  boolean flag        'is_deleted' = true → delete
  NULL sentinel       Any tracked column is NULL → delete (tombstone)

When to use
-----------
- Source is a Debezium, Fivetran, or Airbyte CDC feed with op-code column.
- Source is a snapshot with soft-delete markers.
- Full-sync patterns where the source represents the complete intended state
  and absences should trigger deletes in the target.

When NOT to use
---------------
- Source has no delete signalling → basic_merge (simpler, faster).
- You need to preserve deleted records for audit → SCD Type 2 (expire the row
  instead of deleting it) or SCD Type 4 (history table).

Delta / Databricks considerations
----------------------------------
- WHEN MATCHED THEN DELETE is evaluated before WHEN MATCHED THEN UPDATE.
  Both conditions are applied to matched rows; only one clause fires per row.
- Delta hard-deletes the row from the table.  If you need to recover it, use
  Delta time travel (within VACUUM retention window) or enable Change Data Feed
  (CDF) on the table to capture delete events.
- Enabling CDF (TBLPROPERTIES delta.enableChangeDataFeed = true) lets
  downstream consumers read deleted rows from the table's change log.
- Requires DBR 8.0+ / Delta Lake 1.0+.
- Soft-delete alternative: set an 'is_active' flag to false instead of
  physically deleting.  Preserves history and avoids small-file fragmentation
  from many DELETE operations.

Public API
----------
split_upserts_and_deletes(df_source, config) -> (DataFrame, DataFrame)
    Pure transform: returns (upsert_rows, delete_rows).  Testable without Delta.

apply_delete_aware_merge(spark, df_source, config) -> None
"""

from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from upserts.config import UpsertConfig


def split_upserts_and_deletes(
    df_source: DataFrame,
    config: UpsertConfig,
) -> Tuple[DataFrame, DataFrame]:
    """
    Split source into upsert rows and delete-signalled rows.

    Parameters
    ----------
    df_source : DataFrame
        Incoming CDC batch containing both data changes and delete signals.
    config : UpsertConfig
        delete_indicator_col: column carrying the delete signal.
        delete_indicator_value: the value that means "delete this row".

    Returns
    -------
    (df_upserts, df_deletes)
        df_upserts : rows where delete_indicator_col != delete_indicator_value
                     (or where delete_indicator_col is absent).
        df_deletes : rows where delete_indicator_col == delete_indicator_value.

    Notes
    -----
    When delete_indicator_col is not set, all rows are returned as upserts.
    """
    if not config.delete_indicator_col or config.delete_indicator_col not in df_source.columns:
        return df_source, df_source.limit(0)

    delete_cond = F.col(config.delete_indicator_col).cast("string") == F.lit(
        config.delete_indicator_value
    )
    df_deletes = df_source.filter(delete_cond)
    df_upserts = df_source.filter(~delete_cond)
    return df_upserts, df_deletes


def apply_delete_aware_merge(
    spark: SparkSession,
    df_source: DataFrame,
    config: UpsertConfig,
) -> None:
    """
    Execute a CDC-style MERGE that handles inserts, updates, and deletes.

    Clause order in the generated MERGE:
    1. WHEN MATCHED AND <delete_condition> THEN DELETE
    2. WHEN MATCHED AND NOT <delete_condition> THEN UPDATE SET *
    3. WHEN NOT MATCHED AND NOT <delete_condition> THEN INSERT *

    Clause 3 is conditional: we never insert a delete-signalled row as a new
    target row — that would be a contradiction.

    Parameters
    ----------
    spark : SparkSession
    df_source : DataFrame
        CDC batch with delete_indicator_col.
    config : UpsertConfig
        delete_indicator_col must be set and present in df_source.

    Returns
    -------
    None

    Examples
    --------
        config = UpsertConfig(
            source_table="catalog.bronze.customers_cdc",
            target_table="catalog.gold.dim_customer",
            merge_keys=["customer_id"],
            delete_indicator_col="op",
            delete_indicator_value="D",
        )

        apply_delete_aware_merge(spark, df_source, config)
    """
    from delta.tables import DeltaTable  # noqa: PLC0415

    join_expr = " AND ".join(f"t.{k} = s.{k}" for k in config.merge_keys)

    merge = (
        DeltaTable.forName(spark, config.target_table)
        .alias("t")
        .merge(df_source.alias("s"), join_expr)
    )

    if config.delete_indicator_col:
        del_cond = f"s.{config.delete_indicator_col} = '{config.delete_indicator_value}'"
        merge = (
            merge
            .whenMatchedDelete(condition=del_cond)
            .whenMatchedUpdateAll(condition=f"NOT ({del_cond})")
            .whenNotMatchedInsertAll(condition=f"NOT ({del_cond})")
        )
    else:
        merge = merge.whenMatchedUpdateAll().whenNotMatchedInsertAll()

    merge.execute()
