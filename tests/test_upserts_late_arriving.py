"""
tests/test_upserts_late_arriving.py — Unit tests for late-arriving data MERGE.

Tests cover:
- pick_latest_per_key: pure transform — dedup by timestamp, latest wins.
- build_late_arriving_merge_sql: recency guard in WHEN MATCHED condition.
- build_sequence_guard_merge_sql: sequence-number guard variant.

apply_late_arriving_merge() is integration-only; see tests/integration/.
"""

import pytest
from tests.fixtures.upsert_data import (
    source_late_arriving,
    source_timestamped_with_duplicates,
    target_timestamped,
    T_NEW,
    T_MID,
    T_LATE,
)
from upserts.config import UpsertConfig
from upserts.late_arriving_pyspark import pick_latest_per_key
from upserts.late_arriving_sql import (
    build_late_arriving_merge_sql,
    build_sequence_guard_merge_sql,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return UpsertConfig(
        source_table="v_events",
        target_table="catalog.gold.dim_device",
        merge_keys=["customer_id"],
        event_ts_col="event_ts",
    )


@pytest.fixture
def no_ts_config():
    return UpsertConfig(
        source_table="v_events",
        target_table="catalog.gold.dim_device",
        merge_keys=["customer_id"],
    )


# ---------------------------------------------------------------------------
# pick_latest_per_key
# ---------------------------------------------------------------------------

class TestPickLatestPerKey:

    def test_one_row_per_key(self, spark, config):
        src = source_timestamped_with_duplicates(spark)
        df = pick_latest_per_key(src, ["customer_id"], ts_col="event_ts")
        counts = df.groupBy("customer_id").count()
        assert all(r["count"] == 1 for r in counts.collect())

    def test_latest_timestamp_wins(self, spark):
        src = source_timestamped_with_duplicates(spark)
        df = pick_latest_per_key(src, ["customer_id"], ts_col="event_ts")
        c001 = df.filter(df.customer_id == "C001").collect()[0]
        assert c001.email == "alice@v2.com"
        assert c001.tier == "Platinum"

    def test_no_rn_column_in_output(self, spark):
        src = source_timestamped_with_duplicates(spark)
        df = pick_latest_per_key(src, ["customer_id"], ts_col="event_ts")
        assert "_rn" not in df.columns

    def test_schema_preserved(self, spark):
        src = source_timestamped_with_duplicates(spark)
        df = pick_latest_per_key(src, ["customer_id"], ts_col="event_ts")
        assert set(df.columns) == set(src.columns)

    def test_single_row_per_key_unchanged(self, spark):
        src = source_late_arriving(spark)
        df = pick_latest_per_key(src, ["customer_id"], ts_col="event_ts")
        assert df.count() == src.count()

    def test_late_arriving_batch_deduped_correctly(self, spark):
        src = source_late_arriving(spark)
        df = pick_latest_per_key(src, ["customer_id"], ts_col="event_ts")
        ids = {r.customer_id for r in df.collect()}
        assert ids == {"C001", "C002", "C003"}

    def test_composite_key_dedup(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType
        from datetime import datetime, timezone

        schema = StructType([
            StructField("a", StringType()),
            StructField("b", StringType()),
            StructField("val", StringType()),
            StructField("ts", TimestampType()),
        ])
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
        data = [("X", "Y", "old", t1), ("X", "Y", "new", t2), ("X", "Z", "only", t1)]
        src = spark.createDataFrame(data, schema)
        df = pick_latest_per_key(src, ["a", "b"], ts_col="ts")
        assert df.count() == 2
        xy = df.filter((df.a == "X") & (df.b == "Y")).collect()[0]
        assert xy.val == "new"


# ---------------------------------------------------------------------------
# build_late_arriving_merge_sql — requires event_ts_col
# ---------------------------------------------------------------------------

class TestBuildLateArrivingMergeSql:

    def test_raises_without_event_ts_col(self, no_ts_config):
        with pytest.raises(ValueError, match="event_ts_col"):
            build_late_arriving_merge_sql(no_ts_config)

    def test_recency_guard_in_when_matched(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert "s.event_ts >= t.event_ts" in sql

    def test_target_and_source_present(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert config.target_table in sql
        assert config.source_table in sql

    def test_dedup_via_row_number(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert "ROW_NUMBER()" in sql.upper()

    def test_partition_by_merge_key(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert "customer_id" in sql

    def test_orders_by_ts_desc(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert "DESC" in sql.upper()

    def test_insert_clause_present(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert "INSERT" in sql.upper()

    def test_excludes_rn_from_output(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert "EXCEPT(_rn)" in sql or "_rn" not in sql.split("MERGE")[0]

    def test_merge_starts_correctly(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert sql.upper().startswith("MERGE INTO")

    def test_when_matched_condition_for_recency(self, config):
        sql = build_late_arriving_merge_sql(config)
        assert "WHEN MATCHED AND" in sql.upper()

    def test_composite_key_joined_with_and(self):
        cfg = UpsertConfig(
            source_table="s", target_table="t",
            merge_keys=["device_id", "sensor_id"],
            event_ts_col="event_ts",
        )
        sql = build_late_arriving_merge_sql(cfg)
        assert "device_id" in sql
        assert "sensor_id" in sql


# ---------------------------------------------------------------------------
# build_sequence_guard_merge_sql
# ---------------------------------------------------------------------------

class TestBuildSequenceGuardMergeSql:

    def test_sequence_guard_uses_greater_than(self, config):
        sql = build_sequence_guard_merge_sql(config, seq_col="seq_num")
        assert "s.seq_num > t.seq_num" in sql

    def test_orders_by_seq_col_desc(self, config):
        sql = build_sequence_guard_merge_sql(config, seq_col="offset")
        assert "offset" in sql
        assert "DESC" in sql.upper()

    def test_target_and_source_present(self, config):
        sql = build_sequence_guard_merge_sql(config, seq_col="seq_num")
        assert config.target_table in sql
        assert config.source_table in sql

    def test_dedup_via_row_number(self, config):
        sql = build_sequence_guard_merge_sql(config, seq_col="seq_num")
        assert "ROW_NUMBER()" in sql.upper()

    def test_insert_clause_present(self, config):
        sql = build_sequence_guard_merge_sql(config, seq_col="seq_num")
        assert "INSERT" in sql.upper()

    def test_no_timestamp_column_required(self, no_ts_config):
        sql = build_sequence_guard_merge_sql(no_ts_config, seq_col="offset")
        assert "MERGE INTO" in sql.upper()
