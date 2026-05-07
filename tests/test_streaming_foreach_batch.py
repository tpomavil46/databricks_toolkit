"""
tests/test_streaming_foreach_batch.py — Unit tests for foreachBatch factories.

Tests cover:
- build_foreach_batch_merge_sql: SQL structure, temp view as source.
- build_foreach_batch_dedup_sql: CTE dedup SQL referencing temp view.
- make_sql_batch_fn / make_sql_dedup_batch_fn: return callables.
- make_upsert_batch_fn / make_dedup_upsert_batch_fn: return callables.
- Checkpoint utilities: get_checkpoint_path, checkpoint_options.

No Spark required — factories and SQL generation are pure functions.
"""

import pytest
from upserts.config import UpsertConfig
from streaming.config import StreamingConfig
from streaming.checkpoint import checkpoint_options, get_checkpoint_path
from streaming.foreach_batch_sql import (
    build_foreach_batch_dedup_sql,
    build_foreach_batch_merge_sql,
    make_sql_batch_fn,
    make_sql_dedup_batch_fn,
)
from streaming.foreach_batch_pyspark import (
    make_delete_aware_batch_fn,
    make_dedup_upsert_batch_fn,
    make_idempotent_batch_fn,
    make_scd2_batch_fn,
    make_upsert_batch_fn,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def upsert_config():
    return UpsertConfig(
        source_table="_stream_batch",
        target_table="catalog.gold.dim_customer",
        merge_keys=["customer_id"],
    )


@pytest.fixture
def streaming_config():
    return StreamingConfig(
        source_path="catalog.bronze.events",
        target_table="catalog.silver.events",
        checkpoint_base="dbfs:/checkpoints",
        pipeline_name="events_silver",
    )


# ---------------------------------------------------------------------------
# build_foreach_batch_merge_sql
# ---------------------------------------------------------------------------

class TestBuildForeachBatchMergeSql:

    def test_starts_with_merge_into(self, upsert_config):
        sql = build_foreach_batch_merge_sql(upsert_config)
        assert sql.upper().startswith("MERGE INTO")

    def test_target_table_in_sql(self, upsert_config):
        sql = build_foreach_batch_merge_sql(upsert_config)
        assert "catalog.gold.dim_customer" in sql

    def test_default_temp_view_name_is_source(self, upsert_config):
        sql = build_foreach_batch_merge_sql(upsert_config)
        assert "_stream_batch" in sql

    def test_custom_temp_view_name(self, upsert_config):
        sql = build_foreach_batch_merge_sql(upsert_config, temp_view_name="my_batch")
        assert "my_batch" in sql

    def test_merge_key_in_on_clause(self, upsert_config):
        sql = build_foreach_batch_merge_sql(upsert_config)
        assert "customer_id" in sql

    def test_temp_view_name_is_source_in_sql(self, upsert_config):
        sql = build_foreach_batch_merge_sql(upsert_config, temp_view_name="my_view")
        assert "my_view" in sql

    def test_update_and_insert_clauses(self, upsert_config):
        sql = build_foreach_batch_merge_sql(upsert_config)
        assert "UPDATE" in sql.upper()
        assert "INSERT" in sql.upper()

    def test_composite_key_in_on_clause(self):
        cfg = UpsertConfig(
            source_table="_batch",
            target_table="catalog.gold.fact_order",
            merge_keys=["order_id", "line_id"],
        )
        sql = build_foreach_batch_merge_sql(cfg)
        assert "order_id" in sql
        assert "line_id" in sql


# ---------------------------------------------------------------------------
# build_foreach_batch_dedup_sql
# ---------------------------------------------------------------------------

class TestBuildForeachBatchDedupSql:

    def test_contains_row_number(self, upsert_config):
        sql = build_foreach_batch_dedup_sql(upsert_config, ts_col="event_ts")
        assert "ROW_NUMBER()" in sql.upper()

    def test_default_temp_view_is_source(self, upsert_config):
        sql = build_foreach_batch_dedup_sql(upsert_config, ts_col="event_ts")
        assert "_stream_batch" in sql

    def test_ts_col_in_order_by(self, upsert_config):
        sql = build_foreach_batch_dedup_sql(upsert_config, ts_col="event_ts")
        assert "event_ts" in sql

    def test_merge_key_in_partition_by(self, upsert_config):
        sql = build_foreach_batch_dedup_sql(upsert_config, ts_col="event_ts")
        assert "customer_id" in sql

    def test_desc_ordering(self, upsert_config):
        sql = build_foreach_batch_dedup_sql(upsert_config, ts_col="event_ts")
        assert "DESC" in sql.upper()


# ---------------------------------------------------------------------------
# make_sql_batch_fn
# ---------------------------------------------------------------------------

class TestMakeSqlBatchFn:

    def test_returns_callable(self, upsert_config):
        fn = make_sql_batch_fn(upsert_config)
        assert callable(fn)

    def test_callable_accepts_two_args(self, upsert_config):
        import inspect
        fn = make_sql_batch_fn(upsert_config)
        sig = inspect.signature(fn)
        assert len(sig.parameters) == 2

    def test_different_configs_different_sql_in_closure(self):
        cfg1 = UpsertConfig(source_table="_batch", target_table="t1", merge_keys=["id"])
        cfg2 = UpsertConfig(source_table="_batch", target_table="t2", merge_keys=["id"])
        fn1 = make_sql_batch_fn(cfg1)
        fn2 = make_sql_batch_fn(cfg2)
        assert fn1 is not fn2


# ---------------------------------------------------------------------------
# make_sql_dedup_batch_fn
# ---------------------------------------------------------------------------

class TestMakeSqlDedupBatchFn:

    def test_returns_callable(self, upsert_config):
        fn = make_sql_dedup_batch_fn(upsert_config, ts_col="event_ts")
        assert callable(fn)

    def test_callable_accepts_two_args(self, upsert_config):
        import inspect
        fn = make_sql_dedup_batch_fn(upsert_config, ts_col="event_ts")
        sig = inspect.signature(fn)
        assert len(sig.parameters) == 2


# ---------------------------------------------------------------------------
# PySpark factory callables
# ---------------------------------------------------------------------------

class TestPySparkFactories:

    def test_make_upsert_batch_fn_returns_callable(self, upsert_config):
        fn = make_upsert_batch_fn(upsert_config)
        assert callable(fn)

    def test_make_dedup_upsert_batch_fn_returns_callable(self, upsert_config):
        fn = make_dedup_upsert_batch_fn(upsert_config, ts_col="event_ts")
        assert callable(fn)

    def test_make_idempotent_batch_fn_returns_callable(self, upsert_config):
        fn = make_idempotent_batch_fn(upsert_config)
        assert callable(fn)

    def test_make_delete_aware_batch_fn_returns_callable(self):
        cfg = UpsertConfig(
            source_table="_batch", target_table="t", merge_keys=["id"],
            delete_indicator_col="op", delete_indicator_value="D",
        )
        fn = make_delete_aware_batch_fn(cfg)
        assert callable(fn)

    def test_make_scd2_batch_fn_returns_callable(self):
        from scd.config import SCDConfig
        scd_cfg = SCDConfig(
            source_table="s", target_table="t",
            business_keys=["id"], tracked_columns=["val"],
        )
        fn = make_scd2_batch_fn(scd_cfg)
        assert callable(fn)

    def test_each_factory_returns_different_closure(self, upsert_config):
        fn1 = make_upsert_batch_fn(upsert_config)
        fn2 = make_dedup_upsert_batch_fn(upsert_config, ts_col="ts")
        assert fn1 is not fn2


# ---------------------------------------------------------------------------
# Checkpoint utilities
# ---------------------------------------------------------------------------

class TestCheckpointUtilities:

    def test_get_checkpoint_path_matches_config_property(self, streaming_config):
        assert get_checkpoint_path(streaming_config) == streaming_config.checkpoint_path

    def test_checkpoint_options_returns_dict_with_location(self, streaming_config):
        opts = checkpoint_options(streaming_config)
        assert "checkpointLocation" in opts
        assert opts["checkpointLocation"] == streaming_config.checkpoint_path

    def test_checkpoint_path_structure(self, streaming_config):
        path = get_checkpoint_path(streaming_config)
        assert "dbfs:/checkpoints" in path
        assert "events_silver" in path
