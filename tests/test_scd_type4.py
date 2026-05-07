"""
tests/test_scd_type4.py — Unit tests for SCD Type 4 (Separate History Table).

Tests cover:
- build_history_rows: builds the append rows for the history table,
  including changed_at timestamp, change_type (INSERT/UPDATE), scd_key.
- SQL generation: both current-merge and history-insert SQL are well-formed.

apply_scd4() (Delta MERGE + append) is integration-only.
"""

import pytest
from pyspark.sql import functions as F
from tests.fixtures.scd_data import (
    source_batch,
    source_all_new,
    source_empty,
    target_scd1,
)
from scd.config import SCDConfig
from scd.type1_pyspark import classify_changes
from scd.type4_pyspark import build_history_rows
from scd.type4_sql import build_scd4_current_merge_sql, build_scd4_history_insert_sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return SCDConfig(
        source_table="source_view",
        target_table="catalog.gold.dim_customer_current",
        history_table="catalog.gold.dim_customer_history",
        business_keys=["customer_id"],
        tracked_columns=["email", "tier"],
        surrogate_key_col="scd_key",
    )


# ---------------------------------------------------------------------------
# build_history_rows
# ---------------------------------------------------------------------------

class TestBuildHistoryRows:

    def _classify(self, spark, config, src, tgt):
        df_new, df_changed, _ = classify_changes(src, tgt, config)
        return df_new, df_changed

    def test_history_has_changed_at_column(self, spark, config):
        src = source_batch(spark)
        tgt = target_scd1(spark)
        df_new, df_changed = self._classify(spark, config, src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        assert "changed_at" in history.columns

    def test_history_has_change_type_column(self, spark, config):
        src = source_batch(spark)
        tgt = target_scd1(spark)
        df_new, df_changed = self._classify(spark, config, src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        assert "change_type" in history.columns

    def test_new_rows_tagged_as_insert(self, spark, config):
        src = source_all_new(spark)
        tgt = target_scd1(spark)
        df_new, df_changed = self._classify(spark, config, src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        types = {r.change_type for r in history.collect()}
        assert types == {"INSERT"}

    def test_changed_rows_tagged_as_update(self, spark, config):
        tgt = target_scd1(spark)
        src = target_scd1(spark)

        from pyspark.sql.types import StringType, StructField, StructType
        changed_data = [("C001", "Alice", "alice@new.com", "Gold")]
        schema = tgt.schema
        changed_src = spark.createDataFrame(changed_data, schema)

        df_new, df_changed = self._classify(spark, config, changed_src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        types = {r.change_type for r in history.collect()}
        assert "UPDATE" in types

    def test_scd_key_not_null(self, spark, config):
        src = source_batch(spark)
        tgt = target_scd1(spark)
        df_new, df_changed = self._classify(spark, config, src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        null_keys = history.filter(F.col("scd_key").isNull())
        assert null_keys.count() == 0

    def test_changed_at_not_null(self, spark, config):
        src = source_batch(spark)
        tgt = target_scd1(spark)
        df_new, df_changed = self._classify(spark, config, src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        null_ts = history.filter(F.col("changed_at").isNull())
        assert null_ts.count() == 0

    def test_row_count_new_plus_changed(self, spark, config):
        src = source_batch(spark)
        tgt = target_scd1(spark)
        df_new, df_changed = self._classify(spark, config, src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        assert history.count() == df_new.count() + df_changed.count()

    def test_empty_source_empty_history(self, spark, config):
        src = source_empty(spark)
        tgt = target_scd1(spark)
        df_new, df_changed = self._classify(spark, config, src, tgt)
        history = build_history_rows(df_new, df_changed, config)
        assert history.count() == 0

    def test_raises_if_history_table_not_set(self):
        """apply_scd4 should raise ValueError when history_table is empty."""
        from scd.type4_pyspark import apply_scd4
        import unittest.mock as mock

        bad_config = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["id"],
            history_table="",
        )
        mock_spark = mock.MagicMock()
        with pytest.raises(ValueError, match="history_table"):
            apply_scd4(mock_spark, mock.MagicMock(), bad_config)


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

class TestBuildScd4Sql:

    def test_current_merge_contains_tables(self, config):
        sql = build_scd4_current_merge_sql(config)
        assert config.target_table in sql
        assert config.source_table in sql

    def test_current_merge_has_update_and_insert(self, config):
        sql = build_scd4_current_merge_sql(config)
        assert "UPDATE SET" in sql.upper()
        assert "INSERT" in sql.upper()

    def test_history_insert_contains_history_table(self, config):
        sql = build_scd4_history_insert_sql(
            config, ["email", "tier"], ["customer_id", "name", "email", "tier"]
        )
        assert config.history_table in sql

    def test_history_insert_has_change_type_case(self, config):
        sql = build_scd4_history_insert_sql(
            config, ["email", "tier"], ["customer_id", "name", "email", "tier"]
        )
        assert "INSERT" in sql
        assert "UPDATE" in sql
        assert "CASE WHEN" in sql.upper()

    def test_history_insert_has_sha2(self, config):
        sql = build_scd4_history_insert_sql(
            config, ["email", "tier"], ["customer_id", "name", "email", "tier"]
        )
        assert "sha2" in sql.lower()

    def test_history_insert_has_change_condition(self, config):
        sql = build_scd4_history_insert_sql(
            config, ["email", "tier"], ["customer_id", "name", "email", "tier"]
        )
        assert "NOT (t.email <=> s.email)" in sql
        assert "NOT (t.tier <=> s.tier)" in sql

    def test_history_insert_raises_if_no_history_table(self):
        cfg = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["id"],
            history_table="",
        )
        with pytest.raises(ValueError, match="history_table"):
            build_scd4_history_insert_sql(cfg, ["col"], ["id", "col"])
