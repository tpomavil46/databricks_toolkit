"""Tests for performance.aqe_sql, performance.config — no Spark required.
   performance.cache_pyspark Spark tests skip locally."""

from __future__ import annotations

import pytest

from performance.aqe_sql import (
    build_set_sql,
    build_enable_aqe_sql,
    build_disable_aqe_sql,
    build_set_shuffle_partitions_sql,
    build_set_broadcast_threshold_sql,
    build_set_advisory_partition_size_sql,
    build_set_skew_factor_sql,
    build_enable_coalesce_partitions_sql,
    build_aqe_tuning_block,
)
from performance.config import (
    streaming_workload_config,
    batch_etl_config,
    ml_training_config,
    high_concurrency_config,
    merge_all_configs,
)


# ---------------------------------------------------------------------------
# aqe_sql
# ---------------------------------------------------------------------------

class TestBuildSetSql:
    def test_set_keyword(self):
        sql = build_set_sql("spark.foo", "bar")
        assert "SET spark.foo = bar" == sql

    def test_returns_string(self):
        assert isinstance(build_set_sql("k", "v"), str)


class TestBuildEnableDisableAqe:
    def test_enable_true(self):
        sql = build_enable_aqe_sql()
        assert "spark.sql.adaptive.enabled" in sql
        assert "true" in sql

    def test_disable_false(self):
        sql = build_disable_aqe_sql()
        assert "false" in sql


class TestBuildSetShufflePartitions:
    def test_value_in_sql(self):
        sql = build_set_shuffle_partitions_sql(500)
        assert "500" in sql
        assert "shuffle.partitions" in sql

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            build_set_shuffle_partitions_sql(0)

    def test_negative_raises(self):
        with pytest.raises(ValueError):
            build_set_shuffle_partitions_sql(-10)

    def test_one_accepted(self):
        sql = build_set_shuffle_partitions_sql(1)
        assert "1" in sql


class TestBuildSetBroadcastThreshold:
    def test_bytes_in_sql(self):
        sql = build_set_broadcast_threshold_sql(10_485_760)
        assert "10485760" in sql
        assert "autoBroadcastJoinThreshold" in sql

    def test_negative_one_disables(self):
        sql = build_set_broadcast_threshold_sql(-1)
        assert "-1" in sql


class TestBuildSetAdvisoryPartitionSize:
    def test_bytes_in_sql(self):
        sql = build_set_advisory_partition_size_sql(67_108_864)
        assert "67108864" in sql

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            build_set_advisory_partition_size_sql(0)


class TestBuildSetSkewFactor:
    def test_factor_in_sql(self):
        sql = build_set_skew_factor_sql(5.0)
        assert "5.0" in sql
        assert "skewedPartitionFactor" in sql

    def test_below_one_raises(self):
        with pytest.raises(ValueError):
            build_set_skew_factor_sql(0.5)

    def test_one_accepted(self):
        sql = build_set_skew_factor_sql(1.0)
        assert "1.0" in sql


class TestBuildEnableCoalescePartitions:
    def test_coalesce_sql(self):
        sql = build_enable_coalesce_partitions_sql()
        assert "coalescePartitions.enabled" in sql
        assert "true" in sql


class TestBuildAqeTuningBlock:
    def test_returns_list(self):
        block = build_aqe_tuning_block()
        assert isinstance(block, list)

    def test_five_statements(self):
        block = build_aqe_tuning_block()
        assert len(block) == 5

    def test_shuffle_partitions_present(self):
        block = build_aqe_tuning_block(shuffle_partitions=1000)
        combined = " ".join(block)
        assert "1000" in combined

    def test_advisory_size_converted_to_bytes(self):
        block = build_aqe_tuning_block(advisory_mb=64)
        combined = " ".join(block)
        assert str(64 * 1024 * 1024) in combined

    def test_all_are_set_statements(self):
        block = build_aqe_tuning_block()
        assert all(s.startswith("SET ") for s in block)


# ---------------------------------------------------------------------------
# performance.config
# ---------------------------------------------------------------------------

class TestStreamingWorkloadConfig:
    def test_returns_dict(self):
        cfg = streaming_workload_config()
        assert isinstance(cfg, dict)

    def test_aqe_enabled(self):
        cfg = streaming_workload_config()
        assert cfg.get("spark.sql.adaptive.enabled") == "true"

    def test_all_values_are_strings(self):
        cfg = streaming_workload_config()
        assert all(isinstance(v, str) for v in cfg.values())

    def test_custom_checkpoint_interval(self):
        cfg = streaming_workload_config(checkpoint_interval_ms=60_000)
        combined = " ".join(cfg.values())
        assert "60000" in combined


class TestBatchEtlConfig:
    def test_returns_dict(self):
        cfg = batch_etl_config()
        assert isinstance(cfg, dict)

    def test_shuffle_partitions_in_config(self):
        cfg = batch_etl_config(shuffle_partitions=3000)
        assert cfg["spark.sql.shuffle.partitions"] == "3000"

    def test_broadcast_threshold(self):
        cfg = batch_etl_config(broadcast_threshold_mb=50)
        expected = str(50 * 1024 * 1024)
        assert cfg["spark.sql.autoBroadcastJoinThreshold"] == expected

    def test_auto_compact_enabled(self):
        cfg = batch_etl_config()
        assert cfg.get("spark.databricks.delta.autoCompact.enabled") == "true"


class TestMlTrainingConfig:
    def test_arrow_enabled(self):
        cfg = ml_training_config()
        assert cfg.get("spark.sql.execution.arrow.pyspark.enabled") == "true"

    def test_memory_fraction_set(self):
        cfg = ml_training_config()
        assert "spark.memory.fraction" in cfg


class TestHighConcurrencyConfig:
    def test_preemption_enabled(self):
        cfg = high_concurrency_config()
        assert cfg.get("spark.databricks.preemption.enabled") == "true"

    def test_shuffle_partitions_present(self):
        cfg = high_concurrency_config()
        assert "spark.sql.shuffle.partitions" in cfg


class TestMergeAllConfigs:
    def test_merges_two_configs(self):
        a = {"k1": "v1"}
        b = {"k2": "v2"}
        merged = merge_all_configs(a, b)
        assert merged["k1"] == "v1"
        assert merged["k2"] == "v2"

    def test_later_overrides_earlier(self):
        a = {"key": "first"}
        b = {"key": "second"}
        merged = merge_all_configs(a, b)
        assert merged["key"] == "second"

    def test_empty_inputs(self):
        merged = merge_all_configs({}, {})
        assert merged == {}

    def test_single_config(self):
        cfg = {"k": "v"}
        assert merge_all_configs(cfg) == cfg


# ---------------------------------------------------------------------------
# performance.cache_pyspark — Spark tests (skip locally)
# ---------------------------------------------------------------------------

pytest.importorskip("pyspark")


class TestCacheDf:
    def test_cache_returns_df(self, spark):
        from performance.cache_pyspark import cache_df, unpersist_df
        df = spark.createDataFrame([(1,), (2,)], ["x"])
        cached = cache_df(df)
        assert cached is not None
        unpersist_df(cached, blocking=True)

    def test_unpersist_returns_df(self, spark):
        from performance.cache_pyspark import cache_df, unpersist_df
        df = spark.createDataFrame([(1,)], ["x"])
        cached = cache_df(df)
        result = unpersist_df(cached, blocking=True)
        assert result is not None

    def test_estimate_size_returns_float(self, spark):
        from performance.cache_pyspark import estimate_cache_size_mb
        df = spark.createDataFrame([(i,) for i in range(100)], ["x"])
        size = estimate_cache_size_mb(df, sample_fraction=0.1)
        assert isinstance(size, float)
        assert size >= 0.0

    def test_estimate_size_empty_df(self, spark):
        from performance.cache_pyspark import estimate_cache_size_mb
        from pyspark.sql.types import IntegerType, StructField, StructType
        df = spark.createDataFrame([], StructType([StructField("x", IntegerType())]))
        size = estimate_cache_size_mb(df, sample_fraction=0.1)
        assert size == 0.0
