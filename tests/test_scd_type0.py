"""
tests/test_scd_type0.py — Unit tests for SCD Type 0 (Fixed / Preserve Original).

Tests cover:
- detect_rejected_changes: the pure-transform function that identifies what
  WOULD have changed but is discarded by Type 0.
- SCDConfig validation that is relevant to Type 0.

All tests run locally with `pytest tests/` using the local[*] SparkSession
from conftest.py.  No Databricks connection or delta-spark required.

The apply_scd0() function (which performs the actual Delta MERGE) is NOT
tested here — it requires a live Delta table.  Integration tests for that
function belong in tests/integration/.
"""

import pytest
from tests.fixtures.scd_data import (
    source_all_new,
    source_batch,
    source_empty,
    target_scd1,
)
from scd.config import SCDConfig
from scd.type0_pyspark import detect_rejected_changes


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return SCDConfig(
        source_table="source",
        target_table="target",
        business_keys=["customer_id"],
        tracked_columns=["email", "tier"],
    )


# ---------------------------------------------------------------------------
# detect_rejected_changes
# ---------------------------------------------------------------------------

class TestDetectRejectedChanges:

    def test_detects_changed_rows(self, spark, config):
        """C001 has changed email+tier; C002 is unchanged; C003 is new → only C001 rejected."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, config)
        ids = [row.customer_id for row in rejected.select("customer_id").collect()]
        assert "C001" in ids
        assert "C002" not in ids
        assert "C003" not in ids

    def test_null_to_value_is_a_change(self, spark, config):
        """Source sends NULL tier for C004; target has 'Gold' → should be rejected."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, config)
        ids = [row.customer_id for row in rejected.select("customer_id").collect()]
        assert "C004" in ids

    def test_empty_source_returns_empty(self, spark, config):
        src = source_empty(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, config)
        assert rejected.count() == 0

    def test_all_new_returns_empty(self, spark, config):
        """When no source row matches any target key, nothing is rejected."""
        src = source_all_new(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, config)
        assert rejected.count() == 0

    def test_unchanged_row_not_rejected(self, spark, config):
        """C002 values match exactly in source and target → not rejected."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, config)
        ids = [row.customer_id for row in rejected.select("customer_id").collect()]
        assert "C002" not in ids

    def test_schema_preserved(self, spark, config):
        """Rejected DataFrame columns should match the source schema."""
        src = source_batch(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, config)
        assert set(rejected.columns) == set(src.columns)

    def test_empty_tracked_columns_defaults_to_all_non_keys(self, spark):
        """When tracked_columns is empty, all non-key columns are compared."""
        cfg = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["customer_id"],
            tracked_columns=[],
        )
        src = source_batch(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, cfg)
        ids = [row.customer_id for row in rejected.select("customer_id").collect()]
        assert "C001" in ids

    def test_tracked_columns_not_in_target_returns_empty(self, spark):
        """If tracked_columns don't exist in target, nothing can be compared → empty."""
        cfg = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["customer_id"],
            tracked_columns=["nonexistent_column"],
        )
        src = source_batch(spark)
        tgt = target_scd1(spark)
        rejected = detect_rejected_changes(src, tgt, cfg)
        assert rejected.count() == 0


# ---------------------------------------------------------------------------
# SCDConfig validation
# ---------------------------------------------------------------------------

class TestSCDConfigValidation:

    def test_empty_source_table_raises(self):
        with pytest.raises(ValueError, match="source_table"):
            SCDConfig(source_table="", target_table="t", business_keys=["id"])

    def test_empty_target_table_raises(self):
        with pytest.raises(ValueError, match="target_table"):
            SCDConfig(source_table="s", target_table="", business_keys=["id"])

    def test_empty_business_keys_raises(self):
        with pytest.raises(ValueError, match="business_keys"):
            SCDConfig(source_table="s", target_table="t", business_keys=[])

    def test_from_dict_round_trip(self):
        d = {
            "source_table": "catalog.silver.src",
            "target_table": "catalog.gold.dim",
            "business_keys": ["id"],
            "tracked_columns": ["col1", "col2"],
            "env": "prod",
        }
        cfg = SCDConfig.from_dict(d)
        assert cfg.source_table == "catalog.silver.src"
        assert cfg.tracked_columns == ["col1", "col2"]
        assert cfg.env == "prod"
        assert cfg.effective_start_col == "effective_start"
