"""
tests/test_scd_type1.py — Unit tests for SCD Type 1 (Overwrite).

Tests cover:
- classify_changes: classifies source rows into new / changed / unchanged.
  This is the core pure-transform function shared with Types 3, 4, 6.
- SQL generation: build_scd1_sql and build_scd1_conditional_sql return
  strings with the correct structure and table names.

apply_scd1() (Delta MERGE) is integration-only; see tests/integration/.
"""

import pytest
from tests.fixtures.scd_data import (
    source_all_new,
    source_batch,
    source_empty,
    target_scd1,
)
from scd.config import SCDConfig
from scd.type1_pyspark import classify_changes
from scd.type1_sql import build_scd1_conditional_sql, build_scd1_sql


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
    )


# ---------------------------------------------------------------------------
# classify_changes
# ---------------------------------------------------------------------------

class TestClassifyChanges:

    def test_correct_classification(self, spark, config):
        """C001 changed, C002 unchanged, C003 new, C004 changed (NULL tier)."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        df_new, df_changed, df_unchanged = classify_changes(src, tgt, config)

        new_ids = {r.customer_id for r in df_new.collect()}
        changed_ids = {r.customer_id for r in df_changed.collect()}
        unchanged_ids = {r.customer_id for r in df_unchanged.collect()}

        assert new_ids == {"C003"}
        assert "C001" in changed_ids
        assert "C004" in changed_ids
        assert unchanged_ids == {"C002"}

    def test_empty_source_all_empty(self, spark, config):
        src = source_empty(spark)
        tgt = target_scd1(spark)
        df_new, df_changed, df_unchanged = classify_changes(src, tgt, config)
        assert df_new.count() == 0
        assert df_changed.count() == 0
        assert df_unchanged.count() == 0

    def test_all_new_goes_to_new(self, spark, config):
        src = source_all_new(spark)
        tgt = target_scd1(spark)
        df_new, df_changed, df_unchanged = classify_changes(src, tgt, config)
        assert df_new.count() == src.count()
        assert df_changed.count() == 0
        assert df_unchanged.count() == 0

    def test_no_changes_all_unchanged(self, spark, config):
        """Source = target exactly → everything lands in unchanged."""
        tgt = target_scd1(spark)
        df_new, df_changed, df_unchanged = classify_changes(tgt, tgt, config)
        assert df_new.count() == 0
        assert df_changed.count() == 0
        assert df_unchanged.count() == tgt.count()

    def test_schema_preserved_in_all_buckets(self, spark, config):
        src = source_batch(spark)
        tgt = target_scd1(spark)
        df_new, df_changed, df_unchanged = classify_changes(src, tgt, config)
        for df in [df_new, df_changed, df_unchanged]:
            assert set(df.columns) == set(src.columns)

    def test_null_to_non_null_is_a_change(self, spark, config):
        """Source sends NULL tier for C004; target has 'Gold' → C004 is in changed."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        _, df_changed, _ = classify_changes(src, tgt, config)
        changed_ids = {r.customer_id for r in df_changed.collect()}
        assert "C004" in changed_ids

    def test_composite_business_key(self, spark):
        """Composite keys are handled correctly."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([
            StructField("order_id", StringType()),
            StructField("line_id", StringType()),
            StructField("qty", StringType()),
        ])
        src = spark.createDataFrame([("O1", "L1", "5"), ("O1", "L2", "3"), ("O2", "L1", "1")], schema)
        tgt = spark.createDataFrame([("O1", "L1", "4"), ("O1", "L2", "3")], schema)

        cfg = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["order_id", "line_id"],
            tracked_columns=["qty"],
        )
        df_new, df_changed, df_unchanged = classify_changes(src, tgt, cfg)

        new_ids = {(r.order_id, r.line_id) for r in df_new.collect()}
        changed_ids = {(r.order_id, r.line_id) for r in df_changed.collect()}
        unchanged_ids = {(r.order_id, r.line_id) for r in df_unchanged.collect()}

        assert new_ids == {("O2", "L1")}
        assert changed_ids == {("O1", "L1")}
        assert unchanged_ids == {("O1", "L2")}

    def test_defaults_tracked_to_all_non_key_columns(self, spark):
        """When tracked_columns is empty, all non-key columns are compared."""
        cfg = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["customer_id"],
        )
        src = source_batch(spark)
        tgt = target_scd1(spark)
        _, df_changed, _ = classify_changes(src, tgt, cfg)
        assert df_changed.count() > 0


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

class TestBuildScd1Sql:

    def test_contains_target_table(self, config):
        sql = build_scd1_sql(config)
        assert config.target_table in sql

    def test_contains_source_table(self, config):
        sql = build_scd1_sql(config)
        assert config.source_table in sql

    def test_contains_business_key_in_on_clause(self, config):
        sql = build_scd1_sql(config)
        assert "customer_id" in sql

    def test_contains_update_and_insert(self, config):
        sql = build_scd1_sql(config)
        assert "UPDATE SET" in sql.upper()
        assert "INSERT" in sql.upper()

    def test_merge_keyword_present(self, config):
        sql = build_scd1_sql(config)
        assert sql.upper().startswith("MERGE INTO")

    def test_conditional_sql_contains_change_condition(self, config):
        sql = build_scd1_conditional_sql(config, tracked_columns=["email", "tier"])
        assert "NOT (t.email <=> s.email)" in sql
        assert "NOT (t.tier <=> s.tier)" in sql

    def test_conditional_sql_when_matched_condition(self, config):
        sql = build_scd1_conditional_sql(config, tracked_columns=["email"])
        assert "WHEN MATCHED AND" in sql.upper()
