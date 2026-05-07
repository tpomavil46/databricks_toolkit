"""Tests for medallion.pipeline_sql and medallion.idempotency SQL — no Spark required."""

from __future__ import annotations

import pytest

from medallion.config import MedallionConfig
from medallion.pipeline_sql import (
    build_bronze_create_sql,
    build_silver_create_sql,
    build_gold_create_sql,
    build_silver_merge_sql,
    build_bronze_insert_sql,
    build_gold_insert_overwrite_sql,
)
from medallion.idempotency import (
    MIN_TIMESTAMP,
    build_watermark_table_sql,
    build_get_watermark_sql,
    build_upsert_watermark_sql,
    build_incremental_source_sql,
)


@pytest.fixture
def cfg():
    return MedallionConfig(
        source_path="s3://bucket/raw",
        bronze_table="cat.bronze.events",
        silver_table="cat.silver.events",
        gold_table="cat.gold.events",
        checkpoint_base="dbfs:/checkpoints",
        pipeline_name="events",
        id_col="event_id",
        ts_col="event_ts",
        partition_col="report_date",
    )


@pytest.fixture
def cfg_no_partition():
    return MedallionConfig(
        source_path="s3://bucket/raw",
        bronze_table="cat.bronze.t",
        silver_table="cat.silver.t",
        gold_table="cat.gold.t",
        checkpoint_base="dbfs:/cp",
        pipeline_name="pipe",
        id_col="id",
        ts_col="ts",
    )


# ---------------------------------------------------------------------------
# build_bronze_create_sql
# ---------------------------------------------------------------------------

class TestBuildBronzeCreateSql:
    def test_table_name_in_sql(self, cfg):
        sql = build_bronze_create_sql(cfg)
        assert "cat.bronze.events" in sql

    def test_using_delta(self, cfg):
        sql = build_bronze_create_sql(cfg)
        assert "USING DELTA" in sql.upper()

    def test_if_not_exists(self, cfg):
        sql = build_bronze_create_sql(cfg)
        assert "IF NOT EXISTS" in sql.upper()

    def test_metadata_cols_present(self, cfg):
        sql = build_bronze_create_sql(cfg)
        assert "_ingest_ts" in sql
        assert "_batch_id" in sql
        assert "_source_file" in sql

    def test_partition_clause_when_set(self, cfg):
        sql = build_bronze_create_sql(cfg)
        assert "PARTITIONED BY" in sql.upper()
        assert "report_date" in sql

    def test_no_partition_clause_when_not_set(self, cfg_no_partition):
        sql = build_bronze_create_sql(cfg_no_partition)
        assert "PARTITIONED BY" not in sql.upper()

    def test_extra_cols_included(self, cfg):
        sql = build_bronze_create_sql(cfg, extra_cols=["customer_id STRING", "amount DOUBLE"])
        assert "customer_id STRING" in sql
        assert "amount DOUBLE" in sql

    def test_cdf_enabled(self, cfg):
        sql = build_bronze_create_sql(cfg)
        assert "enableChangeDataFeed" in sql


# ---------------------------------------------------------------------------
# build_silver_create_sql
# ---------------------------------------------------------------------------

class TestBuildSilverCreateSql:
    def test_table_name(self, cfg):
        sql = build_silver_create_sql(cfg, ["id STRING NOT NULL", "name STRING"])
        assert "cat.silver.events" in sql

    def test_col_defs_present(self, cfg):
        sql = build_silver_create_sql(cfg, ["event_id STRING NOT NULL", "ts TIMESTAMP"])
        assert "event_id STRING NOT NULL" in sql
        assert "ts TIMESTAMP" in sql

    def test_empty_col_defs_raises(self, cfg):
        with pytest.raises(ValueError, match="col_defs"):
            build_silver_create_sql(cfg, [])

    def test_using_delta(self, cfg):
        sql = build_silver_create_sql(cfg, ["id STRING"])
        assert "USING DELTA" in sql.upper()

    def test_cdf_enabled(self, cfg):
        sql = build_silver_create_sql(cfg, ["id STRING"])
        assert "enableChangeDataFeed" in sql


# ---------------------------------------------------------------------------
# build_gold_create_sql
# ---------------------------------------------------------------------------

class TestBuildGoldCreateSql:
    def test_table_name(self, cfg):
        sql = build_gold_create_sql(cfg, ["date DATE", "revenue DOUBLE"])
        assert "cat.gold.events" in sql

    def test_partition_clause(self, cfg):
        sql = build_gold_create_sql(cfg, ["report_date DATE", "revenue DOUBLE"])
        assert "PARTITIONED BY" in sql.upper()
        assert "report_date" in sql

    def test_no_partition_when_not_set(self, cfg_no_partition):
        sql = build_gold_create_sql(cfg_no_partition, ["id STRING"])
        assert "PARTITIONED BY" not in sql.upper()

    def test_empty_col_defs_raises(self, cfg):
        with pytest.raises(ValueError, match="col_defs"):
            build_gold_create_sql(cfg, [])


# ---------------------------------------------------------------------------
# build_silver_merge_sql
# ---------------------------------------------------------------------------

class TestBuildSilverMergeSql:
    def test_merge_into_silver(self, cfg):
        sql = build_silver_merge_sql(cfg, "v_incoming")
        assert "MERGE INTO cat.silver.events" in sql

    def test_using_source_view(self, cfg):
        sql = build_silver_merge_sql(cfg, "v_incoming")
        assert "USING v_incoming" in sql

    def test_on_clause_uses_id_col(self, cfg):
        sql = build_silver_merge_sql(cfg, "v_incoming")
        assert "t.event_id = s.event_id" in sql

    def test_when_matched_update(self, cfg):
        sql = build_silver_merge_sql(cfg, "v_incoming")
        assert "WHEN MATCHED" in sql.upper()
        assert "UPDATE SET *" in sql.upper()

    def test_when_not_matched_insert(self, cfg):
        sql = build_silver_merge_sql(cfg, "v_incoming")
        assert "WHEN NOT MATCHED" in sql.upper()
        assert "INSERT *" in sql.upper()

    def test_no_id_col_raises(self, cfg_no_partition):
        cfg_no_partition.id_col = ""
        with pytest.raises(ValueError, match="id_col"):
            build_silver_merge_sql(cfg_no_partition, "src")


# ---------------------------------------------------------------------------
# build_bronze_insert_sql
# ---------------------------------------------------------------------------

class TestBuildBronzeInsertSql:
    def test_insert_into_bronze(self, cfg):
        sql = build_bronze_insert_sql("raw_view", cfg)
        assert "INSERT INTO cat.bronze.events" in sql

    def test_from_source_view(self, cfg):
        sql = build_bronze_insert_sql("raw_view", cfg)
        assert "FROM raw_view" in sql

    def test_metadata_cols_in_select(self, cfg):
        sql = build_bronze_insert_sql("raw_view", cfg)
        assert "_ingest_ts" in sql
        assert "_batch_id" in sql
        assert "_source_file" in sql

    def test_current_timestamp_used(self, cfg):
        sql = build_bronze_insert_sql("raw_view", cfg)
        assert "current_timestamp()" in sql


# ---------------------------------------------------------------------------
# build_gold_insert_overwrite_sql
# ---------------------------------------------------------------------------

class TestBuildGoldInsertOverwriteSql:
    def test_insert_overwrite_gold(self, cfg):
        sql = build_gold_insert_overwrite_sql("agg_view", cfg, "2024-06-01")
        assert "INSERT OVERWRITE cat.gold.events" in sql

    def test_partition_clause(self, cfg):
        sql = build_gold_insert_overwrite_sql("agg_view", cfg, "2024-06-01")
        assert "PARTITION" in sql.upper()
        assert "report_date" in sql
        assert "2024-06-01" in sql

    def test_from_source_view(self, cfg):
        sql = build_gold_insert_overwrite_sql("agg_view", cfg, "2024-06-01")
        assert "FROM agg_view" in sql

    def test_where_filter(self, cfg):
        sql = build_gold_insert_overwrite_sql("agg_view", cfg, "2024-06-01")
        assert "WHERE report_date = '2024-06-01'" in sql

    def test_no_partition_col_raises(self, cfg_no_partition):
        with pytest.raises(ValueError, match="partition_col"):
            build_gold_insert_overwrite_sql("v", cfg_no_partition, "2024-01-01")


# ---------------------------------------------------------------------------
# Watermark SQL (idempotency module)
# ---------------------------------------------------------------------------

class TestBuildWatermarkTableSql:
    def test_table_name_in_sql(self):
        sql = build_watermark_table_sql("cat.audit.watermarks")
        assert "cat.audit.watermarks" in sql

    def test_using_delta(self):
        sql = build_watermark_table_sql("wm")
        assert "USING DELTA" in sql.upper()

    def test_pipeline_name_col(self):
        sql = build_watermark_table_sql("wm")
        assert "pipeline_name" in sql

    def test_last_ts_col(self):
        sql = build_watermark_table_sql("wm")
        assert "last_ts" in sql

    def test_updated_at_col(self):
        sql = build_watermark_table_sql("wm")
        assert "updated_at" in sql

    def test_cdf_enabled(self):
        sql = build_watermark_table_sql("wm")
        assert "enableChangeDataFeed" in sql


class TestBuildGetWatermarkSql:
    def test_selects_last_ts(self):
        sql = build_get_watermark_sql("wm", "my_pipeline")
        assert "last_ts" in sql
        assert "SELECT" in sql.upper()

    def test_filters_pipeline(self):
        sql = build_get_watermark_sql("wm", "my_pipeline")
        assert "my_pipeline" in sql

    def test_from_table(self):
        sql = build_get_watermark_sql("cat.audit.wm", "p")
        assert "FROM cat.audit.wm" in sql


class TestBuildUpsertWatermarkSql:
    def test_merge_into(self):
        sql = build_upsert_watermark_sql("wm", "p", "2024-06-01T12:00:00")
        assert "MERGE INTO wm" in sql

    def test_pipeline_name_in_sql(self):
        sql = build_upsert_watermark_sql("wm", "my_pipeline", "2024-06-01T12:00:00")
        assert "my_pipeline" in sql

    def test_new_ts_in_sql(self):
        sql = build_upsert_watermark_sql("wm", "p", "2024-06-01T12:00:00")
        assert "2024-06-01T12:00:00" in sql

    def test_when_matched_update(self):
        sql = build_upsert_watermark_sql("wm", "p", "2024-01-01T00:00:00")
        assert "WHEN MATCHED" in sql.upper()
        assert "UPDATE" in sql.upper()

    def test_when_not_matched_insert(self):
        sql = build_upsert_watermark_sql("wm", "p", "2024-01-01T00:00:00")
        assert "WHEN NOT MATCHED" in sql.upper()
        assert "INSERT" in sql.upper()


class TestBuildIncrementalSourceSql:
    def test_greater_than_watermark(self):
        sql = build_incremental_source_sql("src", "event_ts", "2024-01-01T00:00:00")
        assert "event_ts > TIMESTAMP '2024-01-01T00:00:00'" in sql

    def test_from_table(self):
        sql = build_incremental_source_sql("catalog.bronze.t", "ts", MIN_TIMESTAMP)
        assert "FROM catalog.bronze.t" in sql

    def test_upper_bound_when_set(self):
        sql = build_incremental_source_sql("t", "ts", "2024-01-01T00:00:00", "2024-02-01T00:00:00")
        assert "ts <= TIMESTAMP '2024-02-01T00:00:00'" in sql

    def test_no_upper_bound(self):
        sql = build_incremental_source_sql("t", "ts", "2024-01-01T00:00:00")
        assert "upper" not in sql.lower()
        assert "end_ts" not in sql.lower()

    def test_min_timestamp_constant(self):
        assert "1970" in MIN_TIMESTAMP
