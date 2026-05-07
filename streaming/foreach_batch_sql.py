"""
streaming/foreach_batch_sql.py — foreachBatch Factories (Spark SQL).

Module   : streaming.foreach_batch_sql
Concept  : SQL-based foreachBatch — register batch as temp view, run spark.sql()
When     : Same as foreach_batch_pyspark.  Prefer the SQL variant when the MERGE
           statement must be auditable in Databricks query history verbatim, or
           when building pipelines where ops teams inspect SQL logs.

How SQL foreachBatch works
--------------------------
1. Each micro-batch DataFrame is registered as a temporary view.
2. The MERGE SQL references that view as its source.
3. spark.sql(merge_sql) executes the MERGE against the live Delta target.

The SQL string is built once at factory-creation time (no Spark needed).
The view registration and SQL execution happen inside the closure at run time.

Advantage over PySpark API
--------------------------
The MERGE SQL is a plain string that can be logged, diffed, or injected into
a notebook cell for debugging.  The PySpark DeltaTable API generates the same
SQL under the hood, but it's not directly inspectable.

Idempotency note
-----------------
Same rules as foreach_batch_pyspark:  the batch function must be idempotent.
For the SQL variant, idempotency is achieved by:
- Registering the view with createOrReplaceTempView (safe to re-register).
- Using a hash-guarded MERGE that only updates rows whose content changed.
- Deduplicating via the CTE approach before the MERGE.

Public API
----------
build_foreach_batch_merge_sql(upsert_config, temp_view_name) -> str
    Generate the MERGE SQL that references temp_view_name as source.

make_sql_batch_fn(upsert_config, temp_view_name) -> Callable
    Factory: register batch as temp view then execute MERGE SQL.

make_sql_dedup_batch_fn(upsert_config, ts_col, temp_view_name) -> Callable
    Factory: CTE dedup + MERGE SQL inside foreachBatch.
"""

from __future__ import annotations

from typing import Callable

from upserts.config import UpsertConfig


def build_foreach_batch_merge_sql(
    upsert_config: UpsertConfig,
    temp_view_name: str = "_stream_batch",
) -> str:
    """
    Generate the MERGE INTO SQL that uses temp_view_name as the streaming source.

    Parameters
    ----------
    upsert_config : UpsertConfig
        target_table and merge_keys are used.  source_table is ignored —
        replaced by temp_view_name.
    temp_view_name : str
        Name of the temporary view registered by the batch function.
        Default: '_stream_batch'.

    Returns
    -------
    str
        MERGE INTO SQL ready for spark.sql().

    Examples
    --------
        sql = build_foreach_batch_merge_sql(config)
        print(sql)
        # MERGE INTO catalog.gold.dim_customer AS t
        # USING _stream_batch AS s
        # ON t.customer_id = s.customer_id
        # ...
    """
    from upserts.config import UpsertConfig as _UpsertConfig
    from upserts.basic_merge_sql import build_basic_merge_sql

    batch_config = _UpsertConfig(
        source_table=temp_view_name,
        target_table=upsert_config.target_table,
        merge_keys=upsert_config.merge_keys,
        update_columns=upsert_config.update_columns,
        delete_indicator_col=upsert_config.delete_indicator_col,
        delete_indicator_value=upsert_config.delete_indicator_value,
    )
    return build_basic_merge_sql(batch_config)


def make_sql_batch_fn(
    upsert_config: UpsertConfig,
    temp_view_name: str = "_stream_batch",
) -> Callable:
    """
    Return a foreachBatch function that runs MERGE via spark.sql().

    The MERGE SQL is built once at factory-creation time.  On each batch:
    1. Register df_batch as temp_view_name.
    2. Execute the pre-built MERGE SQL.

    Parameters
    ----------
    upsert_config : UpsertConfig
    temp_view_name : str
        Temp view name embedded in the MERGE SQL and used at registration.

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None

    Examples
    --------
        batch_fn = make_sql_batch_fn(config)
        df.writeStream.foreachBatch(batch_fn).trigger(availableNow=True).start()
    """
    merge_sql = build_foreach_batch_merge_sql(upsert_config, temp_view_name)

    def _fn(df_batch, batch_id):
        df_batch.createOrReplaceTempView(temp_view_name)
        df_batch.sparkSession.sql(merge_sql)

    return _fn


def build_foreach_batch_dedup_sql(
    upsert_config: UpsertConfig,
    ts_col: str,
    temp_view_name: str = "_stream_batch",
    dedup_view_name: str = "_stream_batch_deduped",
) -> str:
    """
    Generate the CTE deduplication SQL that produces a deduplicated view.

    Run this SQL first, register the result as dedup_view_name, then run
    the MERGE SQL against dedup_view_name.

    Parameters
    ----------
    upsert_config : UpsertConfig
    ts_col : str
        Timestamp/sequence column.  Most recent wins.
    temp_view_name : str
        The raw batch temp view (source of the dedup CTE).
    dedup_view_name : str
        Unused here — the caller registers the CTE result under this name.

    Returns
    -------
    str
        CTE dedup SQL — SELECT statement, not a MERGE.
    """
    from upserts.idempotent_sql import build_dedup_cte_sql
    from upserts.config import UpsertConfig as _UpsertConfig

    dedup_config = _UpsertConfig(
        source_table=temp_view_name,
        target_table=upsert_config.target_table,
        merge_keys=upsert_config.merge_keys,
    )
    return build_dedup_cte_sql(dedup_config, order_col=ts_col, desc=True)


def make_sql_dedup_batch_fn(
    upsert_config: UpsertConfig,
    ts_col: str,
    temp_view_name: str = "_stream_batch",
    dedup_view_name: str = "_stream_batch_deduped",
) -> Callable:
    """
    Return a foreachBatch function that deduplicates via SQL CTE then MERGEs.

    Steps per batch:
    1. Register raw batch as temp_view_name.
    2. Run CTE dedup SQL → register result as dedup_view_name.
    3. Run MERGE SQL against dedup_view_name.

    Parameters
    ----------
    upsert_config : UpsertConfig
    ts_col : str
        Timestamp/sequence column for dedup ordering.
    temp_view_name : str
    dedup_view_name : str

    Returns
    -------
    Callable
        (df_batch: DataFrame, batch_id: int) -> None
    """
    dedup_sql = build_foreach_batch_dedup_sql(
        upsert_config, ts_col, temp_view_name, dedup_view_name
    )

    from upserts.config import UpsertConfig as _UpsertConfig
    from upserts.basic_merge_sql import build_basic_merge_sql

    deduped_config = _UpsertConfig(
        source_table=dedup_view_name,
        target_table=upsert_config.target_table,
        merge_keys=upsert_config.merge_keys,
        update_columns=upsert_config.update_columns,
    )
    merge_sql = build_basic_merge_sql(deduped_config)

    def _fn(df_batch, batch_id):
        spark = df_batch.sparkSession
        df_batch.createOrReplaceTempView(temp_view_name)
        spark.sql(dedup_sql).createOrReplaceTempView(dedup_view_name)
        spark.sql(merge_sql)

    return _fn
