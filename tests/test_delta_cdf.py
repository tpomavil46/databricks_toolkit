"""
tests/test_delta_cdf.py — Unit tests for Change Data Feed helpers.

Tests cover:
- build_cdf_stream_options: readChangeFeed, version/timestamp keys.
- build_cdf_batch_options: version range and timestamp options.
- build_enable_cdf_sql / build_disable_cdf_sql: TBLPROPERTIES SQL.
- build_table_changes_sql: table_changes() function, version range, filter.
- build_table_changes_timestamp_sql: timestamp range variant.
- build_cdf_postimage_merge_sql: MERGE structure, delete handling.
- deletion_vectors: enable/disable/describe SQL.
- table_properties: set/unset/show SQL.

No Spark required — all tests run locally.
"""

import pytest
from delta_features.cdf_pyspark import (
    ALL_CHANGE_TYPES,
    CDF_CHANGE_TYPE_COL,
    build_cdf_batch_options,
    build_cdf_stream_options,
)
from delta_features.cdf_sql import (
    build_cdf_postimage_merge_sql,
    build_disable_cdf_sql,
    build_enable_cdf_sql,
    build_table_changes_sql,
    build_table_changes_timestamp_sql,
)
from delta_features.deletion_vectors import (
    build_disable_deletion_vectors_sql,
    build_enable_deletion_vectors_sql,
    build_optimize_after_deletes_sql,
    describe_deletion_vectors,
)
from delta_features.table_properties import (
    COMMON_PROPERTIES,
    build_production_defaults_sql,
    build_set_tblproperties_sql,
    build_show_tblproperties_sql,
    build_unset_tblproperties_sql,
)

TABLE = "catalog.bronze.events"
DOWNSTREAM = "catalog.silver.events"


# ---------------------------------------------------------------------------
# build_cdf_stream_options
# ---------------------------------------------------------------------------

class TestBuildCdfStreamOptions:

    def test_read_change_feed_always_set(self):
        opts = build_cdf_stream_options()
        assert opts.get("readChangeFeed") == "true"

    def test_no_version_by_default(self):
        opts = build_cdf_stream_options()
        assert "startingVersion" not in opts

    def test_starting_version_when_set(self):
        opts = build_cdf_stream_options(start_version=10)
        assert opts["startingVersion"] == "10"

    def test_starting_timestamp_when_set(self):
        opts = build_cdf_stream_options(start_timestamp="2024-01-15")
        assert opts["startingTimestamp"] == "2024-01-15"

    def test_version_and_timestamp_can_coexist(self):
        opts = build_cdf_stream_options(start_version=5, start_timestamp="2024-01-15")
        assert "startingVersion" in opts
        assert "startingTimestamp" in opts

    def test_version_zero(self):
        opts = build_cdf_stream_options(start_version=0)
        assert opts["startingVersion"] == "0"

    def test_returns_dict(self):
        assert isinstance(build_cdf_stream_options(), dict)


# ---------------------------------------------------------------------------
# build_cdf_batch_options
# ---------------------------------------------------------------------------

class TestBuildCdfBatchOptions:

    def test_read_change_feed_always_set(self):
        opts = build_cdf_batch_options(start_version=1)
        assert opts.get("readChangeFeed") == "true"

    def test_starting_version_set(self):
        opts = build_cdf_batch_options(start_version=5)
        assert opts["startingVersion"] == "5"

    def test_ending_version_when_set(self):
        opts = build_cdf_batch_options(start_version=5, end_version=10)
        assert opts["endingVersion"] == "10"

    def test_no_ending_version_by_default(self):
        opts = build_cdf_batch_options(start_version=5)
        assert "endingVersion" not in opts

    def test_timestamp_overrides_version(self):
        opts = build_cdf_batch_options(start_version=0, start_timestamp="2024-01-15")
        assert "startingTimestamp" in opts


# ---------------------------------------------------------------------------
# CDF SQL — enable/disable
# ---------------------------------------------------------------------------

class TestEnableDisableCdfSql:

    def test_enable_contains_alter_table(self):
        sql = build_enable_cdf_sql(TABLE)
        assert "ALTER TABLE" in sql.upper()

    def test_enable_contains_cdf_property(self):
        sql = build_enable_cdf_sql(TABLE)
        assert "enableChangeDataFeed" in sql

    def test_enable_sets_to_true(self):
        sql = build_enable_cdf_sql(TABLE)
        assert "true" in sql.lower()

    def test_disable_sets_to_false(self):
        sql = build_disable_cdf_sql(TABLE)
        assert "false" in sql.lower()

    def test_both_contain_table_name(self):
        for sql in [build_enable_cdf_sql(TABLE), build_disable_cdf_sql(TABLE)]:
            assert TABLE in sql


# ---------------------------------------------------------------------------
# build_table_changes_sql
# ---------------------------------------------------------------------------

class TestBuildTableChangesSql:

    def test_contains_table_changes_function(self):
        sql = build_table_changes_sql(TABLE, start_version=0)
        assert "table_changes" in sql.lower()

    def test_table_name_in_function_call(self):
        sql = build_table_changes_sql(TABLE, start_version=0)
        assert TABLE in sql

    def test_start_version_in_sql(self):
        sql = build_table_changes_sql(TABLE, start_version=5)
        assert "5" in sql

    def test_end_version_when_set(self):
        sql = build_table_changes_sql(TABLE, start_version=5, end_version=10)
        assert "10" in sql

    def test_no_where_without_change_types(self):
        sql = build_table_changes_sql(TABLE, start_version=0)
        assert "WHERE" not in sql.upper()

    def test_where_with_change_types(self):
        sql = build_table_changes_sql(TABLE, start_version=0, change_types=["insert"])
        assert "WHERE" in sql.upper()
        assert "_change_type" in sql

    def test_change_types_quoted(self):
        sql = build_table_changes_sql(TABLE, start_version=0, change_types=["insert", "delete"])
        assert "'insert'" in sql
        assert "'delete'" in sql

    def test_multiple_change_types_in_clause(self):
        sql = build_table_changes_sql(
            TABLE, start_version=0,
            change_types=["insert", "update_postimage"],
        )
        assert "IN" in sql.upper()


# ---------------------------------------------------------------------------
# build_table_changes_timestamp_sql
# ---------------------------------------------------------------------------

class TestBuildTableChangesTimestampSql:

    def test_contains_table_changes_function(self):
        sql = build_table_changes_timestamp_sql(TABLE, start_ts="2024-01-15")
        assert "table_changes" in sql.lower()

    def test_start_ts_quoted(self):
        sql = build_table_changes_timestamp_sql(TABLE, start_ts="2024-01-15")
        assert "'2024-01-15'" in sql

    def test_end_ts_when_set(self):
        sql = build_table_changes_timestamp_sql(TABLE, "2024-01-15", end_ts="2024-01-31")
        assert "'2024-01-31'" in sql

    def test_no_end_ts_by_default(self):
        sql = build_table_changes_timestamp_sql(TABLE, start_ts="2024-01-15")
        assert sql.count("'2024") == 1


# ---------------------------------------------------------------------------
# build_cdf_postimage_merge_sql
# ---------------------------------------------------------------------------

class TestBuildCdfPostimageMergeSql:

    def test_merge_into_target(self):
        sql = build_cdf_postimage_merge_sql(DOWNSTREAM, "_cdf_batch", ["event_id"])
        assert "MERGE INTO" in sql.upper()
        assert DOWNSTREAM in sql

    def test_source_view_present(self):
        sql = build_cdf_postimage_merge_sql(DOWNSTREAM, "_cdf_batch", ["event_id"])
        assert "_cdf_batch" in sql

    def test_key_in_on_clause(self):
        sql = build_cdf_postimage_merge_sql(DOWNSTREAM, "_cdf_batch", ["event_id"])
        assert "event_id" in sql

    def test_no_delete_by_default(self):
        sql = build_cdf_postimage_merge_sql(DOWNSTREAM, "_cdf_batch", ["event_id"])
        assert "DELETE" not in sql.upper()

    def test_delete_clause_when_has_deletes(self):
        sql = build_cdf_postimage_merge_sql(
            DOWNSTREAM, "_cdf_batch", ["event_id"], has_deletes=True
        )
        assert "DELETE" in sql.upper()

    def test_delete_condition_references_change_type(self):
        sql = build_cdf_postimage_merge_sql(
            DOWNSTREAM, "_cdf_batch", ["event_id"], has_deletes=True
        )
        assert "_change_type" in sql


# ---------------------------------------------------------------------------
# Deletion vectors
# ---------------------------------------------------------------------------

class TestDeletionVectors:

    def test_enable_contains_enable_dv_property(self):
        sql = build_enable_deletion_vectors_sql(TABLE)
        assert "enableDeletionVectors" in sql
        assert "true" in sql.lower()

    def test_disable_sets_false(self):
        sql = build_disable_deletion_vectors_sql(TABLE)
        assert "false" in sql.lower()

    def test_optimize_after_deletes_starts_with_optimize(self):
        sql = build_optimize_after_deletes_sql(TABLE)
        assert sql.upper().startswith("OPTIMIZE")

    def test_optimize_after_deletes_with_zorder(self):
        sql = build_optimize_after_deletes_sql(TABLE, zorder_cols=["user_id"])
        assert "ZORDER BY" in sql.upper()
        assert "user_id" in sql

    def test_describe_returns_string(self):
        desc = describe_deletion_vectors()
        assert isinstance(desc, str)
        assert len(desc) > 0

    def test_describe_mentions_bitmap(self):
        desc = describe_deletion_vectors()
        assert "bitmap" in desc.lower() or "dv" in desc.lower() or "deletion" in desc.lower()


# ---------------------------------------------------------------------------
# Table properties
# ---------------------------------------------------------------------------

class TestTableProperties:

    def test_set_contains_alter_table(self):
        sql = build_set_tblproperties_sql(TABLE, {"delta.enableChangeDataFeed": "true"})
        assert "ALTER TABLE" in sql.upper()

    def test_set_contains_key_and_value(self):
        sql = build_set_tblproperties_sql(TABLE, {"delta.enableChangeDataFeed": "true"})
        assert "enableChangeDataFeed" in sql
        assert "true" in sql

    def test_set_empty_props_raises(self):
        with pytest.raises(ValueError):
            build_set_tblproperties_sql(TABLE, {})

    def test_unset_contains_unset_keyword(self):
        sql = build_unset_tblproperties_sql(TABLE, ["delta.enableChangeDataFeed"])
        assert "UNSET" in sql.upper()

    def test_unset_contains_key(self):
        sql = build_unset_tblproperties_sql(TABLE, ["delta.enableChangeDataFeed"])
        assert "enableChangeDataFeed" in sql

    def test_unset_empty_keys_raises(self):
        with pytest.raises(ValueError):
            build_unset_tblproperties_sql(TABLE, [])

    def test_unset_if_exists_by_default(self):
        sql = build_unset_tblproperties_sql(TABLE, ["key"])
        assert "IF EXISTS" in sql.upper()

    def test_unset_no_if_exists_when_disabled(self):
        sql = build_unset_tblproperties_sql(TABLE, ["key"], if_exists=False)
        assert "IF EXISTS" not in sql.upper()

    def test_show_tblproperties(self):
        sql = build_show_tblproperties_sql(TABLE)
        assert "SHOW TBLPROPERTIES" in sql.upper()
        assert TABLE in sql

    def test_production_defaults_includes_cdf(self):
        sql = build_production_defaults_sql(TABLE)
        assert "enableChangeDataFeed" in sql

    def test_common_properties_is_dict(self):
        assert isinstance(COMMON_PROPERTIES, dict)
        assert len(COMMON_PROPERTIES) > 0
