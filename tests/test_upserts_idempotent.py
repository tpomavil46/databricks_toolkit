"""
tests/test_upserts_idempotent.py — Unit tests for idempotent MERGE patterns.

Tests cover:
- deduplicate_source: pure transform — one row per key, correct winner.
- add_content_hash: deterministic SHA-256 hash, null handling.
- build_dedup_cte_sql: CTE structure with correct ORDER direction.
- build_idempotent_merge_sql: hash guard in WHEN MATCHED condition.

apply_idempotent_merge() is integration-only; see tests/integration/.
"""

import pytest
from tests.fixtures.upsert_data import (
    source_with_duplicates,
    source_timestamped_with_duplicates,
    source_basic,
    CUSTOMER_SCHEMA,
)
from upserts.config import UpsertConfig
from upserts.idempotent_pyspark import add_content_hash, deduplicate_source
from upserts.idempotent_sql import build_dedup_cte_sql, build_idempotent_merge_sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return UpsertConfig(
        source_table="v_customers_incoming",
        target_table="catalog.gold.dim_customer",
        merge_keys=["customer_id"],
    )


# ---------------------------------------------------------------------------
# deduplicate_source
# ---------------------------------------------------------------------------

class TestDeduplicateSource:

    def test_deduplicates_to_one_row_per_key(self, spark, config):
        src = source_with_duplicates(spark)
        df = deduplicate_source(src, ["customer_id"], order_col="email")
        count_by_key = df.groupBy("customer_id").count()
        assert all(r["count"] == 1 for r in count_by_key.collect())

    def test_latest_wins_by_default(self, spark, config):
        src = source_timestamped_with_duplicates(spark)
        df = deduplicate_source(src, ["customer_id"], order_col="event_ts", desc=True)
        c001 = df.filter(df.customer_id == "C001").collect()[0]
        assert c001.email == "alice@v2.com"
        assert c001.tier == "Platinum"

    def test_first_wins_when_asc(self, spark, config):
        src = source_timestamped_with_duplicates(spark)
        df = deduplicate_source(src, ["customer_id"], order_col="event_ts", desc=False)
        c001 = df.filter(df.customer_id == "C001").collect()[0]
        assert c001.email == "alice@v1.com"

    def test_no_rn_column_in_output(self, spark, config):
        src = source_with_duplicates(spark)
        df = deduplicate_source(src, ["customer_id"], order_col="email")
        assert "_rn" not in df.columns

    def test_schema_preserved(self, spark, config):
        src = source_with_duplicates(spark)
        df = deduplicate_source(src, ["customer_id"], order_col="email")
        assert set(df.columns) == set(src.columns)

    def test_no_duplicates_returned_unchanged_count(self, spark, config):
        src = source_basic(spark)
        df = deduplicate_source(src, ["customer_id"], order_col="email")
        assert df.count() == src.count()

    def test_composite_key_dedup(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("line_id", StringType()),
            StructField("qty", StringType()),
            StructField("seq", StringType()),
        ])
        data = [
            ("O1", "L1", "5", "2"),
            ("O1", "L1", "7", "3"),
            ("O1", "L2", "3", "1"),
        ]
        src = spark.createDataFrame(data, schema)
        df = deduplicate_source(src, ["order_id", "line_id"], order_col="seq", desc=True)
        assert df.count() == 2
        o1l1 = df.filter((df.order_id == "O1") & (df.line_id == "L1")).collect()[0]
        assert o1l1.qty == "7"


# ---------------------------------------------------------------------------
# add_content_hash
# ---------------------------------------------------------------------------

class TestAddContentHash:

    def test_hash_column_added(self, spark):
        src = source_basic(spark)
        df = add_content_hash(src, ["name", "email", "tier"])
        assert "_content_hash" in df.columns

    def test_custom_hash_col_name(self, spark):
        src = source_basic(spark)
        df = add_content_hash(src, ["name", "email"], hash_col="my_hash")
        assert "my_hash" in df.columns
        assert "_content_hash" not in df.columns

    def test_hash_is_64_char_hex(self, spark):
        src = source_basic(spark)
        df = add_content_hash(src, ["name", "email", "tier"])
        hashes = [r._content_hash for r in df.select("_content_hash").collect()]
        assert all(len(h) == 64 for h in hashes)
        assert all(all(c in "0123456789abcdef" for c in h) for h in hashes)

    def test_same_values_same_hash(self, spark):
        src = source_basic(spark)
        df1 = add_content_hash(src, ["name", "email", "tier"])
        df2 = add_content_hash(src, ["name", "email", "tier"])
        hashes1 = sorted(r._content_hash for r in df1.collect())
        hashes2 = sorted(r._content_hash for r in df2.collect())
        assert hashes1 == hashes2

    def test_different_values_different_hash(self, spark):
        from tests.fixtures.upsert_data import target_basic
        src = source_basic(spark)
        tgt = target_basic(spark)
        src_hashed = add_content_hash(src, ["email", "tier"])
        tgt_hashed = add_content_hash(tgt, ["email", "tier"])

        src_c001 = src_hashed.filter(src_hashed.customer_id == "C001").collect()[0]._content_hash
        tgt_c001 = tgt_hashed.filter(tgt_hashed.customer_id == "C001").collect()[0]._content_hash
        assert src_c001 != tgt_c001

    def test_same_values_same_hash_unchanged_row(self, spark):
        from tests.fixtures.upsert_data import target_basic
        src = source_basic(spark)
        tgt = target_basic(spark)
        src_hashed = add_content_hash(src, ["name", "email", "tier"])
        tgt_hashed = add_content_hash(tgt, ["name", "email", "tier"])

        src_c002 = src_hashed.filter(src_hashed.customer_id == "C002").collect()[0]._content_hash
        tgt_c002 = tgt_hashed.filter(tgt_hashed.customer_id == "C002").collect()[0]._content_hash
        assert src_c002 == tgt_c002

    def test_null_values_produce_consistent_hash(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([
            StructField("id", StringType()),
            StructField("val", StringType(), True),
        ])
        row1 = spark.createDataFrame([("A", None)], schema)
        row2 = spark.createDataFrame([("A", None)], schema)
        h1 = add_content_hash(row1, ["val"]).collect()[0]._content_hash
        h2 = add_content_hash(row2, ["val"]).collect()[0]._content_hash
        assert h1 == h2

    def test_null_vs_value_produces_different_hash(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([
            StructField("id", StringType()),
            StructField("val", StringType(), True),
        ])
        null_row = spark.createDataFrame([("A", None)], schema)
        val_row = spark.createDataFrame([("A", "Gold")], schema)
        h_null = add_content_hash(null_row, ["val"]).collect()[0]._content_hash
        h_val = add_content_hash(val_row, ["val"]).collect()[0]._content_hash
        assert h_null != h_val

    def test_original_columns_preserved(self, spark):
        src = source_basic(spark)
        df = add_content_hash(src, ["name", "email", "tier"])
        for col in src.columns:
            assert col in df.columns


# ---------------------------------------------------------------------------
# build_dedup_cte_sql
# ---------------------------------------------------------------------------

class TestBuildDedupCteSql:

    def test_contains_row_number(self, config):
        sql = build_dedup_cte_sql(config, order_col="event_ts")
        assert "ROW_NUMBER()" in sql.upper()

    def test_desc_by_default(self, config):
        sql = build_dedup_cte_sql(config, order_col="event_ts", desc=True)
        assert "DESC" in sql.upper()

    def test_asc_when_desc_false(self, config):
        sql = build_dedup_cte_sql(config, order_col="event_ts", desc=False)
        assert "ASC" in sql.upper()

    def test_source_table_in_sql(self, config):
        sql = build_dedup_cte_sql(config, order_col="event_ts")
        assert config.source_table in sql

    def test_partition_by_merge_key(self, config):
        sql = build_dedup_cte_sql(config, order_col="event_ts")
        assert "customer_id" in sql

    def test_filters_rn_equals_one(self, config):
        sql = build_dedup_cte_sql(config, order_col="event_ts")
        assert "_rn = 1" in sql

    def test_excludes_rn_column_from_select(self, config):
        sql = build_dedup_cte_sql(config, order_col="event_ts")
        assert "EXCEPT(_rn)" in sql or "EXCEPT" in sql.upper()


# ---------------------------------------------------------------------------
# build_idempotent_merge_sql
# ---------------------------------------------------------------------------

class TestBuildIdempotentMergeSql:

    def test_contains_hash_guard_in_when_matched(self, config):
        sql = build_idempotent_merge_sql(config, tracked_columns=["name", "email", "tier"])
        assert "_content_hash" in sql
        assert "!=" in sql

    def test_uses_sha2(self, config):
        sql = build_idempotent_merge_sql(config, tracked_columns=["name", "email"])
        assert "sha2" in sql.lower()

    def test_tracked_columns_in_hash_expr(self, config):
        sql = build_idempotent_merge_sql(config, tracked_columns=["name", "email"])
        assert "s.name" in sql
        assert "s.email" in sql

    def test_null_handling_coalesce(self, config):
        sql = build_idempotent_merge_sql(config, tracked_columns=["name"])
        assert "COALESCE" in sql.upper()
        assert "__null__" in sql

    def test_contains_insert_clause(self, config):
        sql = build_idempotent_merge_sql(config, tracked_columns=["name"])
        assert "INSERT" in sql.upper()

    def test_target_table_in_sql(self, config):
        sql = build_idempotent_merge_sql(config, tracked_columns=["name"])
        assert config.target_table in sql

    def test_custom_hash_col_name(self):
        cfg = UpsertConfig(
            source_table="s", target_table="t",
            merge_keys=["id"], content_hash_col="my_hash",
        )
        sql = build_idempotent_merge_sql(cfg, tracked_columns=["val"])
        assert "my_hash" in sql

    def test_custom_dedup_view(self, config):
        sql = build_idempotent_merge_sql(
            config, tracked_columns=["name"], dedup_view="my_deduped_view"
        )
        assert "my_deduped_view" in sql
