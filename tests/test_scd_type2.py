"""
tests/test_scd_type2.py — Unit tests for SCD Type 2 (Full History).

Tests cover:
- classify_changes: identifies new vs changed entities against the current
  (is_current=True) state of the target.
- build_new_versions: attaches effective_start, effective_end, is_current,
  and scd_key to the rows that need to be inserted.
- surrogate_key: hash is deterministic and non-null.
- SQL generation: expire and insert SQL strings are well-formed.

apply_scd2() (Delta MERGE + append) is integration-only.
"""

import pytest
from pyspark.sql import functions as F
from tests.fixtures.scd_data import (
    source_all_new,
    source_batch,
    source_empty,
    target_scd2_current_only,
)
from scd.config import SCDConfig
from scd.type2_pyspark import build_new_versions, classify_changes
from scd.type2_sql import build_scd2_expire_sql, build_scd2_insert_sql
from scd.surrogate_key import add_surrogate_key


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
        surrogate_key_col="scd_key",
    )


# ---------------------------------------------------------------------------
# classify_changes (against current rows only)
# ---------------------------------------------------------------------------

class TestClassifyChanges:

    def test_new_and_changed_correctly_split(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)

        new_ids = {r.customer_id for r in df_new.collect()}
        changed_ids = {r.customer_id for r in df_changed.collect()}

        assert new_ids == {"C003"}
        assert "C001" in changed_ids
        assert "C004" in changed_ids
        assert "C002" not in changed_ids

    def test_empty_source(self, spark, config):
        src = source_empty(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        assert df_new.count() == 0
        assert df_changed.count() == 0

    def test_all_new(self, spark, config):
        src = source_all_new(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        assert df_new.count() == src.count()
        assert df_changed.count() == 0

    def test_no_changes(self, spark, config):
        """When source values match current target exactly, changed is empty."""
        from tests.fixtures.scd_data import SOURCE_SCHEMA
        cur = target_scd2_current_only(spark)
        src = cur.select("customer_id", "name", "email", "tier")
        df_new, df_changed = classify_changes(src, cur, config)
        assert df_changed.count() == 0

    def test_schema_of_outputs_matches_source(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        for df in [df_new, df_changed]:
            assert set(df.columns) == set(src.columns)


# ---------------------------------------------------------------------------
# build_new_versions
# ---------------------------------------------------------------------------

class TestBuildNewVersions:

    def test_scd_metadata_columns_added(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_new_versions(df_new, df_changed, config)

        assert config.effective_start_col in result.columns
        assert config.effective_end_col in result.columns
        assert config.is_current_col in result.columns
        assert config.surrogate_key_col in result.columns

    def test_is_current_always_true(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_new_versions(df_new, df_changed, config)
        non_current = result.filter(F.col(config.is_current_col) == False)  # noqa: E712
        assert non_current.count() == 0

    def test_effective_end_is_null(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_new_versions(df_new, df_changed, config)
        non_null_end = result.filter(F.col(config.effective_end_col).isNotNull())
        assert non_null_end.count() == 0

    def test_effective_start_not_null(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_new_versions(df_new, df_changed, config)
        null_start = result.filter(F.col(config.effective_start_col).isNull())
        assert null_start.count() == 0

    def test_row_count_equals_new_plus_changed(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_new_versions(df_new, df_changed, config)
        assert result.count() == df_new.count() + df_changed.count()

    def test_surrogate_key_not_null(self, spark, config):
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, config)
        result = build_new_versions(df_new, df_changed, config)
        null_keys = result.filter(F.col(config.surrogate_key_col).isNull())
        assert null_keys.count() == 0

    def test_no_surrogate_key_when_col_empty(self, spark):
        cfg = SCDConfig(
            source_table="s",
            target_table="t",
            business_keys=["customer_id"],
            surrogate_key_col="",
        )
        src = source_batch(spark)
        cur = target_scd2_current_only(spark)
        df_new, df_changed = classify_changes(src, cur, cfg)
        result = build_new_versions(df_new, df_changed, cfg)
        assert "scd_key" not in result.columns


# ---------------------------------------------------------------------------
# surrogate_key
# ---------------------------------------------------------------------------

class TestSurrogateKey:

    def test_deterministic_same_input(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField("id", StringType()), StructField("val", StringType())])
        df = spark.createDataFrame([("A", "x"), ("B", "y")], schema)
        r1 = add_surrogate_key(df, ["id"], "sk").collect()
        r2 = add_surrogate_key(df, ["id"], "sk").collect()
        assert all(a.sk == b.sk for a, b in zip(r1, r2))

    def test_different_inputs_different_keys(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField("id", StringType())])
        df = spark.createDataFrame([("A",), ("B",)], schema)
        rows = add_surrogate_key(df, ["id"], "sk").collect()
        assert rows[0].sk != rows[1].sk

    def test_null_handled_as_sentinel(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField("id", StringType())])
        df = spark.createDataFrame([(None,)], schema)
        rows = add_surrogate_key(df, ["id"], "sk").collect()
        assert rows[0].sk is not None

    def test_key_is_64_char_hex(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField("id", StringType())])
        df = spark.createDataFrame([("X",)], schema)
        rows = add_surrogate_key(df, ["id"], "sk").collect()
        import re
        assert re.match(r"^[0-9a-f]{64}$", rows[0].sk)


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

class TestBuildScd2Sql:

    def test_expire_sql_contains_tables(self, config):
        sql = build_scd2_expire_sql(config, ["email", "tier"])
        assert config.target_table in sql
        assert config.source_table in sql

    def test_expire_sql_sets_effective_end(self, config):
        sql = build_scd2_expire_sql(config, ["email", "tier"])
        assert config.effective_end_col in sql
        assert "current_timestamp()" in sql.lower()

    def test_expire_sql_sets_is_current_false(self, config):
        sql = build_scd2_expire_sql(config, ["email", "tier"])
        assert config.is_current_col in sql
        assert "false" in sql.lower()

    def test_expire_sql_has_null_safe_change_condition(self, config):
        sql = build_scd2_expire_sql(config, ["email", "tier"])
        assert "NOT (t.email <=> s.email)" in sql
        assert "NOT (t.tier <=> s.tier)" in sql

    def test_insert_sql_contains_tables(self, config):
        sql = build_scd2_insert_sql(config, ["email", "tier"], ["customer_id", "name", "email", "tier"])
        assert config.target_table in sql
        assert config.source_table in sql

    def test_insert_sql_has_is_current_true(self, config):
        sql = build_scd2_insert_sql(config, ["email", "tier"], ["customer_id", "name", "email", "tier"])
        assert "true" in sql.lower()

    def test_insert_sql_has_sha2(self, config):
        sql = build_scd2_insert_sql(config, ["email", "tier"], ["customer_id", "name", "email", "tier"])
        assert "sha2" in sql.lower()
