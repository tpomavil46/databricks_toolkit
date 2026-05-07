"""
tests/test_upserts_delete_aware.py — Unit tests for CDC delete-aware MERGE.

Tests cover:
- split_upserts_and_deletes: pure transform tested with Spark fixture.
- build_delete_aware_merge_sql: SQL structure, DELETE clause, conditions.
- build_soft_delete_sql: soft-delete flag pattern.

apply_delete_aware_merge() is integration-only; see tests/integration/.
"""

import pytest
from tests.fixtures.upsert_data import source_cdc, source_basic, target_cdc
from upserts.config import UpsertConfig
from upserts.delete_aware_pyspark import split_upserts_and_deletes
from upserts.delete_aware_sql import build_delete_aware_merge_sql, build_soft_delete_sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cdc_config():
    return UpsertConfig(
        source_table="v_customers_cdc",
        target_table="catalog.gold.dim_customer",
        merge_keys=["customer_id"],
        delete_indicator_col="op",
        delete_indicator_value="D",
    )


@pytest.fixture
def no_delete_config():
    return UpsertConfig(
        source_table="v_customers",
        target_table="catalog.gold.dim_customer",
        merge_keys=["customer_id"],
    )


# ---------------------------------------------------------------------------
# split_upserts_and_deletes
# ---------------------------------------------------------------------------

class TestSplitUpsertAndDeletes:

    def test_delete_row_classified_correctly(self, spark, cdc_config):
        src = source_cdc(spark)
        df_upserts, df_deletes = split_upserts_and_deletes(src, cdc_config)
        delete_ids = {r.customer_id for r in df_deletes.collect()}
        assert "C004" in delete_ids

    def test_upsert_rows_exclude_deletes(self, spark, cdc_config):
        src = source_cdc(spark)
        df_upserts, df_deletes = split_upserts_and_deletes(src, cdc_config)
        upsert_ids = {r.customer_id for r in df_upserts.collect()}
        assert "C004" not in upsert_ids

    def test_upsert_rows_include_non_deletes(self, spark, cdc_config):
        src = source_cdc(spark)
        df_upserts, _ = split_upserts_and_deletes(src, cdc_config)
        upsert_ids = {r.customer_id for r in df_upserts.collect()}
        assert {"C001", "C002", "C003"}.issubset(upsert_ids)

    def test_counts_sum_to_total(self, spark, cdc_config):
        src = source_cdc(spark)
        df_upserts, df_deletes = split_upserts_and_deletes(src, cdc_config)
        assert df_upserts.count() + df_deletes.count() == src.count()

    def test_no_delete_col_all_rows_are_upserts(self, spark, no_delete_config):
        src = source_basic(spark)
        df_upserts, df_deletes = split_upserts_and_deletes(src, no_delete_config)
        assert df_upserts.count() == src.count()
        assert df_deletes.count() == 0

    def test_missing_delete_col_in_source_returns_all_as_upserts(self, spark):
        src = source_basic(spark)
        cfg = UpsertConfig(
            source_table="s", target_table="t", merge_keys=["customer_id"],
            delete_indicator_col="nonexistent_col", delete_indicator_value="D",
        )
        df_upserts, df_deletes = split_upserts_and_deletes(src, cfg)
        assert df_upserts.count() == src.count()
        assert df_deletes.count() == 0

    def test_schema_preserved_in_both_outputs(self, spark, cdc_config):
        src = source_cdc(spark)
        df_upserts, df_deletes = split_upserts_and_deletes(src, cdc_config)
        assert set(df_upserts.columns) == set(src.columns)
        assert set(df_deletes.columns) == set(src.columns)

    def test_empty_source_returns_empty_splits(self, spark, cdc_config):
        from tests.fixtures.upsert_data import CDC_SCHEMA
        src = spark.createDataFrame([], CDC_SCHEMA)
        df_upserts, df_deletes = split_upserts_and_deletes(src, cdc_config)
        assert df_upserts.count() == 0
        assert df_deletes.count() == 0


# ---------------------------------------------------------------------------
# build_delete_aware_merge_sql
# ---------------------------------------------------------------------------

class TestBuildDeleteAwareMergeSql:

    def test_contains_delete_clause(self, cdc_config):
        sql = build_delete_aware_merge_sql(cdc_config)
        assert "DELETE" in sql.upper()

    def test_delete_condition_uses_indicator_col(self, cdc_config):
        sql = build_delete_aware_merge_sql(cdc_config)
        assert "s.op" in sql
        assert "'D'" in sql

    def test_update_clause_has_not_delete_condition(self, cdc_config):
        sql = build_delete_aware_merge_sql(cdc_config)
        upper = sql.upper()
        assert "WHEN MATCHED AND" in upper

    def test_insert_clause_has_not_delete_condition(self, cdc_config):
        sql = build_delete_aware_merge_sql(cdc_config)
        assert "WHEN NOT MATCHED AND" in sql.upper()

    def test_never_insert_a_delete_row(self, cdc_config):
        sql = build_delete_aware_merge_sql(cdc_config)
        assert "WHEN NOT MATCHED AND" in sql.upper()

    def test_target_and_source_tables_present(self, cdc_config):
        sql = build_delete_aware_merge_sql(cdc_config)
        assert cdc_config.target_table in sql
        assert cdc_config.source_table in sql

    def test_no_delete_col_falls_back_to_basic_merge(self, no_delete_config):
        sql = build_delete_aware_merge_sql(no_delete_config)
        assert "DELETE" not in sql.upper()
        assert "UPDATE SET *" in sql
        assert "INSERT *" in sql

    def test_merge_key_in_on_clause(self, cdc_config):
        sql = build_delete_aware_merge_sql(cdc_config)
        assert "t.customer_id = s.customer_id" in sql


# ---------------------------------------------------------------------------
# build_soft_delete_sql
# ---------------------------------------------------------------------------

class TestBuildSoftDeleteSql:

    def test_sets_is_active_false(self, cdc_config):
        sql = build_soft_delete_sql(cdc_config)
        assert "false" in sql.lower()
        assert "is_active" in sql

    def test_custom_is_active_col(self, cdc_config):
        sql = build_soft_delete_sql(cdc_config, is_active_col="active_flag")
        assert "active_flag" in sql

    def test_no_physical_delete_keyword(self, cdc_config):
        sql = build_soft_delete_sql(cdc_config)
        lines = sql.upper().splitlines()
        assert not any(l.strip() == "DELETE" for l in lines)

    def test_still_updates_non_delete_rows(self, cdc_config):
        sql = build_soft_delete_sql(cdc_config)
        assert "UPDATE SET *" in sql

    def test_still_inserts_non_delete_rows(self, cdc_config):
        sql = build_soft_delete_sql(cdc_config)
        assert "INSERT *" in sql
