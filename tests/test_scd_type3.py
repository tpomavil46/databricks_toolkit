"""
tests/test_scd_type3.py — Unit tests for SCD Type 3 (Previous Value Columns).

Tests cover:
- build_update_with_prev: the pure transform that builds update rows with
  both the new values AND the old (previous) values captured in prev_<col>.
- SQL generation: build_scd3_sql produces correct MERGE structure.

apply_scd3() (Delta MERGE) is integration-only.
"""

import pytest
from pyspark.sql import functions as F
from tests.fixtures.scd_data import (
    source_batch,
    source_empty,
    target_scd1,
    target_scd3,
)
from scd.config import SCDConfig
from scd.type3_pyspark import build_update_with_prev
from scd.type3_sql import build_scd3_sql


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
        prev_value_columns=["email", "tier"],
    )


# ---------------------------------------------------------------------------
# build_update_with_prev
# ---------------------------------------------------------------------------

class TestBuildUpdateWithPrev:

    def test_prev_columns_added(self, spark, config):
        src = source_batch(spark)
        tgt = target_scd1(spark)
        result = build_update_with_prev(src, tgt, config)
        assert "prev_email" in result.columns
        assert "prev_tier" in result.columns

    def test_changed_row_prev_values_from_target(self, spark, config):
        """
        C001: source has email='alice@new.com', tier='Gold'.
        Target has email='alice@old.com', tier='Silver'.
        Expected: prev_email='alice@old.com', prev_tier='Silver'.
        """
        src = source_batch(spark)
        tgt = target_scd1(spark)
        result = build_update_with_prev(src, tgt, config)
        c001 = result.filter(F.col("customer_id") == "C001").collect()
        assert len(c001) == 1
        assert c001[0].prev_email == "alice@old.com"
        assert c001[0].prev_tier == "Silver"

    def test_current_values_reflect_source(self, spark, config):
        """C001 current email and tier should come from source, not target."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        result = build_update_with_prev(src, tgt, config)
        c001 = result.filter(F.col("customer_id") == "C001").collect()
        assert c001[0].email == "alice@new.com"
        assert c001[0].tier == "Gold"

    def test_new_row_prev_is_null(self, spark, config):
        """C003 is a new entity — prev_email and prev_tier should be NULL."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        result = build_update_with_prev(src, tgt, config)
        c003 = result.filter(F.col("customer_id") == "C003").collect()
        assert len(c003) == 1
        assert c003[0].prev_email is None
        assert c003[0].prev_tier is None

    def test_unchanged_row_not_in_result(self, spark, config):
        """C002 is unchanged — it should not appear in the update DataFrame."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        result = build_update_with_prev(src, tgt, config)
        # Result should contain only new (C003) and changed (C001, C004) rows
        ids = {r.customer_id for r in result.collect()}
        assert "C002" not in ids

    def test_empty_source_empty_result(self, spark, config):
        src = source_empty(spark)
        tgt = target_scd1(spark)
        result = build_update_with_prev(src, tgt, config)
        assert result.count() == 0

    def test_partial_prev_columns(self, spark):
        """When prev_value_columns only tracks 'tier', only prev_tier is added."""
        cfg = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["customer_id"],
            tracked_columns=["email", "tier"],
            prev_value_columns=["tier"],
        )
        src = source_batch(spark)
        tgt = target_scd1(spark)
        result = build_update_with_prev(src, tgt, cfg)
        assert "prev_tier" in result.columns
        assert "prev_email" not in result.columns


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

class TestBuildScd3Sql:

    def test_contains_tables(self, config):
        sql = build_scd3_sql(config, ["email", "tier"], ["email", "tier"])
        assert config.target_table in sql
        assert config.source_table in sql

    def test_contains_merge_keyword(self, config):
        sql = build_scd3_sql(config, ["email", "tier"], ["email", "tier"])
        assert sql.upper().startswith("MERGE INTO")

    def test_update_clause_sets_prev_and_current(self, config):
        sql = build_scd3_sql(config, ["email", "tier"], ["email", "tier"])
        assert "t.prev_email = t.email" in sql
        assert "t.prev_tier = t.tier" in sql
        assert "t.email = s.email" in sql
        assert "t.tier = s.tier" in sql

    def test_insert_clause_present(self, config):
        sql = build_scd3_sql(config, ["email", "tier"], ["email", "tier"])
        assert "WHEN NOT MATCHED THEN" in sql.upper()
        assert "INSERT" in sql.upper()

    def test_insert_prev_columns_as_null(self, config):
        sql = build_scd3_sql(config, ["email", "tier"], ["email", "tier"])
        assert "NULL" in sql

    def test_business_key_in_on_clause(self, config):
        sql = build_scd3_sql(config, ["email", "tier"], ["email", "tier"])
        assert "customer_id" in sql
