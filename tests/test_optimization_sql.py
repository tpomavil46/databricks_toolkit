"""Tests for optimization SQL modules — no Spark required."""

from __future__ import annotations

import pytest

from optimization.partitioning_sql import (
    build_partition_by_sql,
    build_liquid_clustering_alter_sql,
    build_zorder_sql,
    build_optimize_sql,
    build_analyze_sql,
    build_show_partitions_sql,
    build_describe_detail_sql,
)
from optimization.small_files_sql import (
    MINIMUM_RETENTION_HOURS,
    DEFAULT_TARGET_FILE_SIZE_BYTES,
    build_enable_auto_compact_sql,
    build_disable_auto_compact_sql,
    build_enable_optimize_write_sql,
    build_disable_optimize_write_sql,
    build_set_target_file_size_sql,
    build_file_count_check_sql,
    build_vacuum_small_files_sql,
)
from optimization.bloom_filters_sql import (
    DEFAULT_FPP,
    build_create_bloom_filter_sql,
    build_drop_bloom_filter_sql,
    build_set_bloom_filter_tblproperties_sql,
    build_bloom_filter_for_all_cols_sql,
)
from optimization.join_sql import (
    build_broadcast_hint_sql,
    build_merge_hint_sql,
    build_shuffle_hash_hint_sql,
    build_skew_hint_sql,
    build_range_join_hint_sql,
)


# ---------------------------------------------------------------------------
# partitioning_sql
# ---------------------------------------------------------------------------

class TestBuildPartitionBySql:
    def test_table_in_output(self):
        sql = build_partition_by_sql("cat.bronze.t", ["date"])
        assert "cat.bronze.t" in sql

    def test_single_col(self):
        sql = build_partition_by_sql("t", ["date"])
        assert "date" in sql
        assert "PARTITIONED BY" in sql.upper()

    def test_multiple_cols(self):
        sql = build_partition_by_sql("t", ["year", "month"])
        assert "year" in sql
        assert "month" in sql

    def test_empty_cols_raises(self):
        with pytest.raises(ValueError):
            build_partition_by_sql("t", [])


class TestBuildLiquidClusteringAlterSql:
    def test_alter_table(self):
        sql = build_liquid_clustering_alter_sql("t", ["user_id"])
        assert "ALTER TABLE t" in sql
        assert "CLUSTER BY" in sql.upper()

    def test_up_to_four_cols(self):
        sql = build_liquid_clustering_alter_sql("t", ["a", "b", "c", "d"])
        assert "a" in sql

    def test_five_cols_raises(self):
        with pytest.raises(ValueError, match="4"):
            build_liquid_clustering_alter_sql("t", ["a", "b", "c", "d", "e"])

    def test_empty_cols_raises(self):
        with pytest.raises(ValueError):
            build_liquid_clustering_alter_sql("t", [])

    def test_col_in_output(self):
        sql = build_liquid_clustering_alter_sql("t", ["event_ts", "user_id"])
        assert "event_ts" in sql
        assert "user_id" in sql


class TestBuildZorderSql:
    def test_optimize_table(self):
        sql = build_zorder_sql("t", ["user_id"])
        assert "OPTIMIZE t" in sql

    def test_zorder_clause(self):
        sql = build_zorder_sql("t", ["user_id"])
        assert "ZORDER BY" in sql.upper()
        assert "user_id" in sql

    def test_where_predicate(self):
        sql = build_zorder_sql("t", ["id"], where_predicate="date = '2024-01-01'")
        assert "WHERE date = '2024-01-01'" in sql

    def test_no_where_when_not_set(self):
        sql = build_zorder_sql("t", ["id"])
        assert "WHERE" not in sql.upper()

    def test_empty_cols_raises(self):
        with pytest.raises(ValueError):
            build_zorder_sql("t", [])

    def test_multiple_zorder_cols(self):
        sql = build_zorder_sql("t", ["a", "b"])
        assert "a" in sql
        assert "b" in sql


class TestBuildOptimizeSql:
    def test_basic_optimize(self):
        sql = build_optimize_sql("t")
        assert "OPTIMIZE t" in sql

    def test_with_where(self):
        sql = build_optimize_sql("t", where_predicate="date = '2024-01-01'")
        assert "WHERE" in sql.upper()

    def test_with_zorder(self):
        sql = build_optimize_sql("t", zorder_cols=["user_id"])
        assert "ZORDER BY" in sql.upper()

    def test_no_zorder_when_not_set(self):
        sql = build_optimize_sql("t")
        assert "ZORDER" not in sql.upper()


class TestBuildAnalyzeSql:
    def test_compute_statistics(self):
        sql = build_analyze_sql("t")
        assert "ANALYZE TABLE t" in sql
        assert "COMPUTE STATISTICS" in sql.upper()

    def test_with_cols(self):
        sql = build_analyze_sql("t", ["id", "ts"])
        assert "FOR COLUMNS" in sql.upper()
        assert "id" in sql
        assert "ts" in sql

    def test_without_cols_no_for_columns(self):
        sql = build_analyze_sql("t")
        assert "FOR COLUMNS" not in sql.upper()


class TestBuildShowDescribeSql:
    def test_show_partitions(self):
        sql = build_show_partitions_sql("cat.bronze.t")
        assert "SHOW PARTITIONS" in sql.upper()
        assert "cat.bronze.t" in sql

    def test_describe_detail(self):
        sql = build_describe_detail_sql("cat.gold.t")
        assert "DESCRIBE DETAIL" in sql.upper()
        assert "cat.gold.t" in sql


# ---------------------------------------------------------------------------
# small_files_sql
# ---------------------------------------------------------------------------

class TestAutoCompactSql:
    def test_enable_contains_true(self):
        sql = build_enable_auto_compact_sql("t")
        assert "'true'" in sql or "true" in sql.lower()
        assert "autoCompact" in sql

    def test_disable_contains_false(self):
        sql = build_disable_auto_compact_sql("t")
        assert "'false'" in sql or "false" in sql.lower()

    def test_table_in_sql(self):
        sql = build_enable_auto_compact_sql("cat.silver.events")
        assert "cat.silver.events" in sql


class TestOptimizeWriteSql:
    def test_enable_optimize_write(self):
        sql = build_enable_optimize_write_sql("t")
        assert "optimizeWrite" in sql
        assert "'true'" in sql or "true" in sql.lower()

    def test_disable_optimize_write(self):
        sql = build_disable_optimize_write_sql("t")
        assert "optimizeWrite" in sql
        assert "'false'" in sql or "false" in sql.lower()


class TestTargetFileSizeSql:
    def test_size_in_sql(self):
        sql = build_set_target_file_size_sql("t", 134_217_728)
        assert "134217728" in sql

    def test_default_size(self):
        sql = build_set_target_file_size_sql("t")
        assert str(DEFAULT_TARGET_FILE_SIZE_BYTES) in sql

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            build_set_target_file_size_sql("t", 0)

    def test_negative_raises(self):
        with pytest.raises(ValueError):
            build_set_target_file_size_sql("t", -1)

    def test_tblproperties_keyword(self):
        sql = build_set_target_file_size_sql("t", 1024)
        assert "TBLPROPERTIES" in sql.upper()


class TestFileCountCheckSql:
    def test_num_files_in_sql(self):
        sql = build_file_count_check_sql("t")
        assert "num_files" in sql.lower() or "numFiles" in sql

    def test_table_in_sql(self):
        sql = build_file_count_check_sql("cat.silver.events")
        assert "cat.silver.events" in sql

    def test_describe_detail_used(self):
        sql = build_file_count_check_sql("t")
        assert "DESCRIBE DETAIL" in sql.upper()


class TestVacuumSql:
    def test_vacuum_table(self):
        sql = build_vacuum_small_files_sql("t")
        assert "VACUUM t" in sql

    def test_retain_hours(self):
        sql = build_vacuum_small_files_sql("t", 200)
        assert "200" in sql
        assert "HOURS" in sql.upper()

    def test_below_minimum_raises(self):
        with pytest.raises(ValueError, match=str(MINIMUM_RETENTION_HOURS)):
            build_vacuum_small_files_sql("t", retain_hours=24)

    def test_minimum_accepted(self):
        sql = build_vacuum_small_files_sql("t", MINIMUM_RETENTION_HOURS)
        assert str(MINIMUM_RETENTION_HOURS) in sql


# ---------------------------------------------------------------------------
# bloom_filters_sql
# ---------------------------------------------------------------------------

class TestBuildCreateBloomFilterSql:
    def test_create_bloomfilter_index(self):
        sql = build_create_bloom_filter_sql("t", "user_id")
        assert "CREATE BLOOMFILTER INDEX" in sql.upper()

    def test_table_and_col_in_sql(self):
        sql = build_create_bloom_filter_sql("cat.silver.t", "user_id")
        assert "cat.silver.t" in sql
        assert "user_id" in sql

    def test_fpp_in_sql(self):
        sql = build_create_bloom_filter_sql("t", "col", fpp=0.05)
        assert "0.05" in sql

    def test_num_items_in_sql(self):
        sql = build_create_bloom_filter_sql("t", "col", num_items=500_000)
        assert "500000" in sql

    def test_invalid_fpp_zero_raises(self):
        with pytest.raises(ValueError):
            build_create_bloom_filter_sql("t", "col", fpp=0.0)

    def test_invalid_fpp_one_raises(self):
        with pytest.raises(ValueError):
            build_create_bloom_filter_sql("t", "col", fpp=1.0)

    def test_invalid_num_items_raises(self):
        with pytest.raises(ValueError):
            build_create_bloom_filter_sql("t", "col", num_items=0)


class TestBuildDropBloomFilterSql:
    def test_drop_bloomfilter_index(self):
        sql = build_drop_bloom_filter_sql("t", "user_id")
        assert "DROP BLOOMFILTER INDEX" in sql.upper()
        assert "user_id" in sql

    def test_table_in_sql(self):
        sql = build_drop_bloom_filter_sql("cat.silver.t", "col")
        assert "cat.silver.t" in sql


class TestBuildBloomFilterTblpropertiesSql:
    def test_alter_table(self):
        sql = build_set_bloom_filter_tblproperties_sql("t", "col")
        assert "ALTER TABLE t" in sql

    def test_enabled_property(self):
        sql = build_set_bloom_filter_tblproperties_sql("t", "user_id")
        assert "user_id" in sql
        assert "enabled" in sql

    def test_fpp_in_properties(self):
        sql = build_set_bloom_filter_tblproperties_sql("t", "col", fpp=0.02)
        assert "0.02" in sql

    def test_invalid_fpp_raises(self):
        with pytest.raises(ValueError):
            build_set_bloom_filter_tblproperties_sql("t", "col", fpp=1.5)


class TestBuildBloomFilterForAllColsSql:
    def test_returns_one_per_col(self):
        sqls = build_bloom_filter_for_all_cols_sql("t", ["a", "b", "c"])
        assert len(sqls) == 3

    def test_each_is_string(self):
        sqls = build_bloom_filter_for_all_cols_sql("t", ["x"])
        assert all(isinstance(s, str) for s in sqls)

    def test_empty_cols_raises(self):
        with pytest.raises(ValueError):
            build_bloom_filter_for_all_cols_sql("t", [])

    def test_col_names_in_sqls(self):
        sqls = build_bloom_filter_for_all_cols_sql("t", ["user_id", "session_id"])
        combined = " ".join(sqls)
        assert "user_id" in combined
        assert "session_id" in combined


# ---------------------------------------------------------------------------
# join_sql
# ---------------------------------------------------------------------------

class TestBuildBroadcastHintSql:
    def test_broadcast_hint_present(self):
        sql = build_broadcast_hint_sql("a.*, b.name FROM t1 a JOIN t2 b ON a.id = b.id", "b")
        assert "BROADCAST(b)" in sql

    def test_select_keyword(self):
        sql = build_broadcast_hint_sql("* FROM t", "t")
        assert sql.startswith("SELECT")

    def test_hint_in_comment_block(self):
        sql = build_broadcast_hint_sql("* FROM t", "t")
        assert "/*+" in sql and "*/" in sql


class TestBuildMergeHintSql:
    def test_merge_hint(self):
        sql = build_merge_hint_sql("* FROM t1 a JOIN t2 b ON a.id = b.id", ["a", "b"])
        assert "MERGE(a, b)" in sql

    def test_select_keyword(self):
        sql = build_merge_hint_sql("* FROM t", ["t"])
        assert sql.startswith("SELECT")


class TestBuildShuffleHashHintSql:
    def test_shuffle_hash_hint(self):
        sql = build_shuffle_hash_hint_sql("* FROM t1 JOIN t2 ON t1.id = t2.id", ["t1"])
        assert "SHUFFLE_HASH(t1)" in sql


class TestBuildSkewHintSql:
    def test_skew_hint_basic(self):
        sql = build_skew_hint_sql("* FROM t", "t", "user_id")
        assert "SKEW" in sql
        assert "user_id" in sql

    def test_skew_with_values(self):
        sql = build_skew_hint_sql("* FROM t", "t", "user_id", skew_values=["null", "'bot'"])
        assert "null" in sql or "bot" in sql

    def test_hint_block_present(self):
        sql = build_skew_hint_sql("* FROM t", "t", "col")
        assert "/*+" in sql


class TestBuildRangeJoinHintSql:
    def test_range_join_hint(self):
        sql = build_range_join_hint_sql("* FROM events e JOIN windows w ON e.ts BETWEEN w.start AND w.end", 3600)
        assert "RANGE_JOIN" in sql
        assert "3600" in sql

    def test_zero_bin_size_raises(self):
        with pytest.raises(ValueError):
            build_range_join_hint_sql("* FROM t", 0)

    def test_negative_bin_size_raises(self):
        with pytest.raises(ValueError):
            build_range_join_hint_sql("* FROM t", -1)
