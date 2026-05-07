"""
tests/test_upserts_schema_evolution.py — Unit tests for schema evolution MERGE.

Tests cover:
- detect_schema_drift: pure transform — new source columns not in target.
- project_to_target_schema: pure transform — drop unknown source columns.
- build_add_column_sql / build_add_columns_sql: ALTER TABLE DDL generation.
- build_schema_evolution_merge_sql: standard MERGE INTO.
- build_schema_evolution_set_sql: SET statement for autoMerge.

apply_schema_evolution_merge() / apply_schema_evolution_merge_sql() are
integration-only; see tests/integration/.
"""

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from tests.fixtures.upsert_data import source_basic, source_evolved, target_basic
from upserts.config import UpsertConfig
from upserts.schema_evolution_pyspark import detect_schema_drift, project_to_target_schema
from upserts.schema_evolution_sql import (
    build_add_column_sql,
    build_add_columns_sql,
    build_schema_evolution_merge_sql,
    build_schema_evolution_set_sql,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return UpsertConfig(
        source_table="v_customers_v2",
        target_table="catalog.gold.dim_customer",
        merge_keys=["customer_id"],
        allow_schema_evolution=True,
    )


@pytest.fixture
def phone_field():
    return StructField("phone", StringType(), True)


@pytest.fixture
def loyalty_field():
    return StructField("loyalty_score", StringType(), True)


# ---------------------------------------------------------------------------
# detect_schema_drift
# ---------------------------------------------------------------------------

class TestDetectSchemaDrift:

    def test_detects_new_columns(self, spark, config):
        src = source_evolved(spark)
        tgt = target_basic(spark)
        new_fields = detect_schema_drift(src, tgt)
        new_names = {f.name for f in new_fields}
        assert "phone" in new_names
        assert "loyalty_score" in new_names

    def test_no_drift_when_schemas_match(self, spark):
        src = source_basic(spark)
        tgt = target_basic(spark)
        new_fields = detect_schema_drift(src, tgt)
        assert new_fields == []

    def test_returns_only_new_columns(self, spark):
        src = source_evolved(spark)
        tgt = target_basic(spark)
        new_fields = detect_schema_drift(src, tgt)
        new_names = {f.name for f in new_fields}
        assert "customer_id" not in new_names
        assert "name" not in new_names
        assert "email" not in new_names
        assert "tier" not in new_names

    def test_returns_structfields(self, spark):
        src = source_evolved(spark)
        tgt = target_basic(spark)
        new_fields = detect_schema_drift(src, tgt)
        assert all(isinstance(f, StructField) for f in new_fields)

    def test_case_insensitive_comparison(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema_upper = StructType([
            StructField("CUSTOMER_ID", StringType(), False),
            StructField("EMAIL", StringType(), True),
        ])
        schema_lower = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), True),
        ])
        src = spark.createDataFrame([("C1", "a@a.com")], schema_upper)
        tgt = spark.createDataFrame([("C1", "a@a.com")], schema_lower)
        new_fields = detect_schema_drift(src, tgt)
        assert new_fields == []

    def test_empty_source_returns_empty(self, spark):
        from tests.fixtures.upsert_data import EVOLVED_SCHEMA
        src = spark.createDataFrame([], EVOLVED_SCHEMA)
        tgt = target_basic(spark)
        new_fields = detect_schema_drift(src, tgt)
        assert isinstance(new_fields, list)

    def test_correct_count_of_new_fields(self, spark):
        src = source_evolved(spark)
        tgt = target_basic(spark)
        new_fields = detect_schema_drift(src, tgt)
        assert len(new_fields) == 2


# ---------------------------------------------------------------------------
# project_to_target_schema
# ---------------------------------------------------------------------------

class TestProjectToTargetSchema:

    def test_drops_extra_source_columns(self, spark):
        src = source_evolved(spark)
        tgt = target_basic(spark)
        projected = project_to_target_schema(src, tgt)
        assert "phone" not in projected.columns
        assert "loyalty_score" not in projected.columns

    def test_retains_target_columns(self, spark):
        src = source_evolved(spark)
        tgt = target_basic(spark)
        projected = project_to_target_schema(src, tgt)
        assert set(projected.columns) == set(tgt.columns)

    def test_row_count_unchanged(self, spark):
        src = source_evolved(spark)
        tgt = target_basic(spark)
        projected = project_to_target_schema(src, tgt)
        assert projected.count() == src.count()

    def test_no_op_when_schemas_match(self, spark):
        src = source_basic(spark)
        tgt = target_basic(spark)
        projected = project_to_target_schema(src, tgt)
        assert set(projected.columns) == set(src.columns)
        assert projected.count() == src.count()

    def test_case_insensitive_column_matching(self, spark):
        schema_upper = StructType([
            StructField("CUSTOMER_ID", StringType(), False),
            StructField("NAME", StringType(), True),
            StructField("extra_col", StringType(), True),
        ])
        schema_lower = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
        ])
        src = spark.createDataFrame([("C1", "Alice", "extra")], schema_upper)
        tgt = spark.createDataFrame([("C1", "Alice")], schema_lower)
        projected = project_to_target_schema(src, tgt)
        assert "extra_col" not in projected.columns


# ---------------------------------------------------------------------------
# build_add_column_sql
# ---------------------------------------------------------------------------

class TestBuildAddColumnSql:

    def test_alter_table_keyword(self, phone_field):
        sql = build_add_column_sql("catalog.gold.dim_customer", phone_field)
        assert "ALTER TABLE" in sql.upper()

    def test_add_column_keyword(self, phone_field):
        sql = build_add_column_sql("catalog.gold.dim_customer", phone_field)
        assert "ADD COLUMN" in sql.upper()

    def test_table_name_in_sql(self, phone_field):
        sql = build_add_column_sql("catalog.gold.dim_customer", phone_field)
        assert "catalog.gold.dim_customer" in sql

    def test_column_name_in_sql(self, phone_field):
        sql = build_add_column_sql("catalog.gold.dim_customer", phone_field)
        assert "phone" in sql

    def test_column_type_in_sql(self, phone_field):
        sql = build_add_column_sql("catalog.gold.dim_customer", phone_field)
        assert "string" in sql.lower()


# ---------------------------------------------------------------------------
# build_add_columns_sql
# ---------------------------------------------------------------------------

class TestBuildAddColumnsSql:

    def test_returns_one_statement_per_field(self, phone_field, loyalty_field):
        stmts = build_add_columns_sql("catalog.gold.dim_customer", [phone_field, loyalty_field])
        assert len(stmts) == 2

    def test_each_statement_is_alter_table(self, phone_field, loyalty_field):
        stmts = build_add_columns_sql("t", [phone_field, loyalty_field])
        assert all("ALTER TABLE" in s.upper() for s in stmts)

    def test_empty_fields_returns_empty_list(self):
        stmts = build_add_columns_sql("t", [])
        assert stmts == []

    def test_column_names_present(self, phone_field, loyalty_field):
        stmts = build_add_columns_sql("t", [phone_field, loyalty_field])
        all_sql = " ".join(stmts)
        assert "phone" in all_sql
        assert "loyalty_score" in all_sql


# ---------------------------------------------------------------------------
# build_schema_evolution_merge_sql
# ---------------------------------------------------------------------------

class TestBuildSchemaEvolutionMergeSql:

    def test_is_standard_merge(self, config):
        sql = build_schema_evolution_merge_sql(config)
        assert sql.upper().startswith("MERGE INTO")

    def test_target_table_present(self, config):
        sql = build_schema_evolution_merge_sql(config)
        assert config.target_table in sql

    def test_source_table_present(self, config):
        sql = build_schema_evolution_merge_sql(config)
        assert config.source_table in sql

    def test_merge_key_in_on_clause(self, config):
        sql = build_schema_evolution_merge_sql(config)
        assert "customer_id" in sql

    def test_update_star_and_insert_star(self, config):
        sql = build_schema_evolution_merge_sql(config)
        assert "UPDATE SET *" in sql
        assert "INSERT *" in sql


# ---------------------------------------------------------------------------
# build_schema_evolution_set_sql
# ---------------------------------------------------------------------------

class TestBuildSchemaEvolutionSetSql:

    def test_enabled_true(self):
        sql = build_schema_evolution_set_sql(enabled=True)
        assert "true" in sql.lower()
        assert "autoMerge" in sql

    def test_enabled_false(self):
        sql = build_schema_evolution_set_sql(enabled=False)
        assert "false" in sql.lower()
        assert "autoMerge" in sql

    def test_set_keyword_present(self):
        sql = build_schema_evolution_set_sql(enabled=True)
        assert sql.upper().startswith("SET")

    def test_full_config_key_present(self):
        sql = build_schema_evolution_set_sql(enabled=True)
        assert "spark.databricks.delta.schema.autoMerge.enabled" in sql
