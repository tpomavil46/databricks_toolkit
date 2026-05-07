"""
tests/test_delta_time_travel.py — Unit tests for Delta time travel helpers.

Tests cover:
- build_version_read_options / build_timestamp_read_options: correct dicts.
- build_version_sql / build_timestamp_sql: SQL structure, column selection.
- build_restore_to_version_sql / build_restore_to_timestamp_sql: RESTORE syntax.
- build_describe_history_sql: DESCRIBE HISTORY + optional LIMIT.
- build_diff_versions_sql: JOIN structure, key columns in ON clause.

No Spark required — all tests run locally.
"""

import pytest
from delta_features.time_travel_pyspark import (
    build_timestamp_read_options,
    build_version_read_options,
)
from delta_features.time_travel_sql import (
    build_describe_history_sql,
    build_diff_versions_sql,
    build_restore_to_timestamp_sql,
    build_restore_to_version_sql,
    build_timestamp_sql,
    build_version_sql,
)

TABLE = "catalog.gold.dim_customer"


# ---------------------------------------------------------------------------
# PySpark read options
# ---------------------------------------------------------------------------

class TestBuildVersionReadOptions:

    def test_returns_dict(self):
        opts = build_version_read_options(5)
        assert isinstance(opts, dict)

    def test_version_as_of_key(self):
        opts = build_version_read_options(5)
        assert "versionAsOf" in opts

    def test_version_value_is_string(self):
        opts = build_version_read_options(5)
        assert opts["versionAsOf"] == "5"

    def test_version_zero(self):
        opts = build_version_read_options(0)
        assert opts["versionAsOf"] == "0"

    def test_large_version_number(self):
        opts = build_version_read_options(100_000)
        assert opts["versionAsOf"] == "100000"

    def test_exactly_one_key(self):
        opts = build_version_read_options(1)
        assert len(opts) == 1


class TestBuildTimestampReadOptions:

    def test_returns_dict(self):
        opts = build_timestamp_read_options("2024-01-15")
        assert isinstance(opts, dict)

    def test_timestamp_as_of_key(self):
        opts = build_timestamp_read_options("2024-01-15")
        assert "timestampAsOf" in opts

    def test_timestamp_value_preserved(self):
        ts = "2024-01-15 12:00:00"
        opts = build_timestamp_read_options(ts)
        assert opts["timestampAsOf"] == ts

    def test_iso8601_format(self):
        opts = build_timestamp_read_options("2024-01-15T12:00:00Z")
        assert opts["timestampAsOf"] == "2024-01-15T12:00:00Z"

    def test_exactly_one_key(self):
        opts = build_timestamp_read_options("2024-01-15")
        assert len(opts) == 1


# ---------------------------------------------------------------------------
# Version SQL
# ---------------------------------------------------------------------------

class TestBuildVersionSql:

    def test_contains_version_as_of(self):
        sql = build_version_sql(TABLE, version=5)
        assert "VERSION AS OF 5" in sql

    def test_contains_table_name(self):
        sql = build_version_sql(TABLE, version=5)
        assert TABLE in sql

    def test_select_star_by_default(self):
        sql = build_version_sql(TABLE, version=5)
        assert "SELECT *" in sql

    def test_select_specific_columns(self):
        sql = build_version_sql(TABLE, version=5, select_cols=["customer_id", "email"])
        assert "customer_id" in sql
        assert "email" in sql
        assert "*" not in sql

    def test_starts_with_select(self):
        sql = build_version_sql(TABLE, version=0)
        assert sql.upper().startswith("SELECT")

    def test_version_zero(self):
        sql = build_version_sql(TABLE, version=0)
        assert "VERSION AS OF 0" in sql


# ---------------------------------------------------------------------------
# Timestamp SQL
# ---------------------------------------------------------------------------

class TestBuildTimestampSql:

    def test_contains_timestamp_as_of(self):
        sql = build_timestamp_sql(TABLE, "2024-01-15")
        assert "TIMESTAMP AS OF" in sql

    def test_timestamp_is_quoted(self):
        sql = build_timestamp_sql(TABLE, "2024-01-15")
        assert "'2024-01-15'" in sql

    def test_contains_table_name(self):
        sql = build_timestamp_sql(TABLE, "2024-01-15")
        assert TABLE in sql

    def test_select_star_by_default(self):
        sql = build_timestamp_sql(TABLE, "2024-01-15")
        assert "SELECT *" in sql

    def test_select_specific_columns(self):
        sql = build_timestamp_sql(TABLE, "2024-01-15", select_cols=["id", "name"])
        assert "id" in sql
        assert "name" in sql


# ---------------------------------------------------------------------------
# Restore SQL
# ---------------------------------------------------------------------------

class TestBuildRestoreVersionSql:

    def test_contains_restore_table(self):
        sql = build_restore_to_version_sql(TABLE, version=3)
        assert "RESTORE TABLE" in sql.upper()

    def test_contains_table_name(self):
        sql = build_restore_to_version_sql(TABLE, version=3)
        assert TABLE in sql

    def test_contains_version_as_of(self):
        sql = build_restore_to_version_sql(TABLE, version=3)
        assert "VERSION AS OF 3" in sql

    def test_contains_to_keyword(self):
        sql = build_restore_to_version_sql(TABLE, version=3)
        assert " TO " in sql.upper()


class TestBuildRestoreTimestampSql:

    def test_contains_restore_table(self):
        sql = build_restore_to_timestamp_sql(TABLE, "2024-01-15")
        assert "RESTORE TABLE" in sql.upper()

    def test_timestamp_is_quoted(self):
        sql = build_restore_to_timestamp_sql(TABLE, "2024-01-15")
        assert "'2024-01-15'" in sql

    def test_contains_timestamp_as_of(self):
        sql = build_restore_to_timestamp_sql(TABLE, "2024-01-15")
        assert "TIMESTAMP AS OF" in sql


# ---------------------------------------------------------------------------
# DESCRIBE HISTORY SQL
# ---------------------------------------------------------------------------

class TestBuildDescribeHistorySql:

    def test_starts_with_describe_history(self):
        sql = build_describe_history_sql(TABLE)
        assert sql.upper().startswith("DESCRIBE HISTORY")

    def test_contains_table_name(self):
        sql = build_describe_history_sql(TABLE)
        assert TABLE in sql

    def test_no_limit_by_default(self):
        sql = build_describe_history_sql(TABLE)
        assert "LIMIT" not in sql.upper()

    def test_limit_appended_when_set(self):
        sql = build_describe_history_sql(TABLE, limit=10)
        assert "LIMIT 10" in sql

    def test_limit_one(self):
        sql = build_describe_history_sql(TABLE, limit=1)
        assert "LIMIT 1" in sql


# ---------------------------------------------------------------------------
# Diff versions SQL
# ---------------------------------------------------------------------------

class TestBuildDiffVersionsSql:

    def test_contains_both_versions(self):
        sql = build_diff_versions_sql(TABLE, 5, 10, ["customer_id"])
        assert "VERSION AS OF 5" in sql
        assert "VERSION AS OF 10" in sql

    def test_key_col_in_on_clause(self):
        sql = build_diff_versions_sql(TABLE, 5, 10, ["customer_id"])
        assert "customer_id" in sql

    def test_composite_keys_all_present(self):
        sql = build_diff_versions_sql(TABLE, 1, 2, ["order_id", "line_id"])
        assert "order_id" in sql
        assert "line_id" in sql

    def test_uses_left_anti_join(self):
        sql = build_diff_versions_sql(TABLE, 1, 2, ["id"])
        assert "ANTI" in sql.upper() or "EXCEPT" in sql.upper()

    def test_contains_table_name(self):
        sql = build_diff_versions_sql(TABLE, 1, 2, ["id"])
        assert TABLE in sql
