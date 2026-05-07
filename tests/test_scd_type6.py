"""
tests/test_scd_type6.py — Unit tests for SCD Type 6 (Hybrid 1+2+3).

Tests cover:
- build_scd6_new_versions: new version rows with Type 2 metadata AND prev_
  columns from the current target row.
- add_current_value_columns: window function that adds current_<col> to a
  Type 2 table (query-time Type 1 denormalisation).
- SQL generation: expire and insert SQL for Type 6 are well-formed.

apply_scd6() (Delta MERGE + append) is integration-only.
"""

import pytest
from pyspark.sql import functions as F
from tests.fixtures.scd_data import (
    source_batch,
    source_all_new,
    source_empty,
    target_scd2_current_only,
    target_scd2_with_history,
)
from scd.config import SCDConfig
from scd.type2_pyspark import classify_changes
from scd.type6_pyspark import add_current_value_columns, build_scd6_new_versions
from scd.type6_sql import build_scd6_insert_sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return SCDConfig(
        source_table="source_view",
        target_table="catalog.gold.dim_customer",
        business_keys=["customer_id"],
        tracked_columns=["email", "tier"],
        prev_value_columns=["tier"],
        surrogate_key_col="scd_key",
    )


# ---------------------------------------------------------------------------
# build_scd6_new_versions
# ---------------------------------------------------------------------------

class TestBuildScd6NewVersions:

    def _run(self, spark, config, src=None, history=None):
        src = src or source_batch(spark)
        history = history or target_scd2_with_history(spark)
        cur = history.filter(F.col("is_current") == True)  # noqa: E712
        df_new, df_changed = classify_changes(src, cur, config)
        return df_new, df_changed, build_scd6_new_versions(df_new, df_changed, history, config)

    def test_scd2_metadata_present(self, spark, config):
        _, _, result = self._run(spark, config)
        assert config.effective_start_col in result.columns
        assert config.effective_end_col in result.columns
        assert config.is_current_col in result.columns

    def test_is_current_true_for_all_new_versions(self, spark, config):
        _, _, result = self._run(spark, config)
        non_current = result.filter(F.col(config.is_current_col) == False)  # noqa: E712
        assert non_current.count() == 0

    def test_prev_columns_present(self, spark, config):
        _, _, result = self._run(spark, config)
        assert "prev_tier" in result.columns

    def test_changed_entity_prev_tier_from_target(self, spark, config):
        """
        C001 in target has tier='Silver' (current row).
        Source sends tier='Gold'.
        Expected: new version has prev_tier='Silver'.
        """
        _, _, result = self._run(spark, config)
        c001 = result.filter(F.col("customer_id") == "C001").collect()
        assert len(c001) == 1
        assert c001[0].prev_tier == "Silver"

    def test_new_entity_prev_is_null(self, spark, config):
        """C003 is new — it has no previous state, so prev_tier is NULL."""
        _, _, result = self._run(spark, config)
        c003 = result.filter(F.col("customer_id") == "C003").collect()
        assert len(c003) == 1
        assert c003[0].prev_tier is None

    def test_surrogate_key_not_null(self, spark, config):
        _, _, result = self._run(spark, config)
        null_keys = result.filter(F.col(config.surrogate_key_col).isNull())
        assert null_keys.count() == 0

    def test_effective_end_null(self, spark, config):
        _, _, result = self._run(spark, config)
        non_null_end = result.filter(F.col(config.effective_end_col).isNotNull())
        assert non_null_end.count() == 0

    def test_empty_source_empty_result(self, spark, config):
        src = source_empty(spark)
        history = target_scd2_with_history(spark)
        cur = history.filter(F.col("is_current") == True)  # noqa: E712
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_scd6_new_versions(df_new, df_changed, history, config)
        assert result.count() == 0

    def test_all_new_entities_prev_null(self, spark, config):
        src = source_all_new(spark)
        history = target_scd2_with_history(spark)
        cur = history.filter(F.col("is_current") == True)  # noqa: E712
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_scd6_new_versions(df_new, df_changed, history, config)
        null_prev = result.filter(F.col("prev_tier").isNull())
        assert null_prev.count() == result.count()


# ---------------------------------------------------------------------------
# add_current_value_columns (window-function Type 1 denormalisation)
# ---------------------------------------------------------------------------

class TestAddCurrentValueColumns:

    def test_current_columns_added(self, spark):
        history = target_scd2_with_history(spark)
        result = add_current_value_columns(
            history,
            tracked_columns=["tier"],
            business_key="customer_id",
            effective_start_col="effective_start",
        )
        assert "current_tier" in result.columns

    def test_historical_rows_get_latest_current_value(self, spark):
        """
        C001 has two rows: v1 (tier=Bronze, historical) and v2 (tier=Silver, current).
        Both rows should have current_tier = 'Silver' (latest).
        """
        history = target_scd2_with_history(spark)
        result = add_current_value_columns(
            history,
            tracked_columns=["tier"],
            business_key="customer_id",
            effective_start_col="effective_start",
        )
        c001_rows = result.filter(F.col("customer_id") == "C001").collect()
        assert all(r.current_tier == "Silver" for r in c001_rows)

    def test_row_count_unchanged(self, spark):
        history = target_scd2_with_history(spark)
        result = add_current_value_columns(
            history, ["tier"], "customer_id", "effective_start"
        )
        assert result.count() == history.count()


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

class TestBuildScd6InsertSql:

    def test_contains_tables(self, config):
        sql = build_scd6_insert_sql(
            config, ["email", "tier"], ["tier"], ["customer_id", "name", "email", "tier"]
        )
        assert config.target_table in sql
        assert config.source_table in sql

    def test_contains_prev_column_reference(self, config):
        sql = build_scd6_insert_sql(
            config, ["email", "tier"], ["tier"], ["customer_id", "name", "email", "tier"]
        )
        assert "prev_tier" in sql

    def test_contains_is_current_true(self, config):
        sql = build_scd6_insert_sql(
            config, ["email", "tier"], ["tier"], ["customer_id", "name", "email", "tier"]
        )
        assert "true" in sql.lower()

    def test_contains_sha2_for_surrogate(self, config):
        sql = build_scd6_insert_sql(
            config, ["email", "tier"], ["tier"], ["customer_id", "name", "email", "tier"]
        )
        assert "sha2" in sql.lower()

    def test_is_current_filter_on_join(self, config):
        sql = build_scd6_insert_sql(
            config, ["email", "tier"], ["tier"], ["customer_id", "name", "email", "tier"]
        )
        assert "is_current" in sql
