"""Tests for data_quality.checks_sql and data_quality.quarantine_sql — no Spark required."""

from __future__ import annotations

import pytest

from data_quality.checks_sql import (
    build_null_check_sql,
    build_uniqueness_check_sql,
    build_range_check_sql,
    build_regex_check_sql,
    build_accepted_values_check_sql,
    build_row_count_check_sql,
    build_completeness_report_sql,
)
from data_quality.quarantine_sql import (
    build_quarantine_insert_sql,
    build_good_records_sql,
    build_null_quarantine_sql,
    build_range_quarantine_sql,
)


# ---------------------------------------------------------------------------
# build_null_check_sql
# ---------------------------------------------------------------------------

class TestBuildNullCheckSql:
    def test_check_name_literal(self):
        sql = build_null_check_sql("my_table", "email")
        assert "'not_null'" in sql

    def test_column_name_in_output(self):
        sql = build_null_check_sql("my_table", "email")
        assert "'email'" in sql

    def test_table_in_from(self):
        sql = build_null_check_sql("catalog.bronze.events", "ts")
        assert "FROM catalog.bronze.events" in sql

    def test_null_condition(self):
        sql = build_null_check_sql("t", "customer_id")
        assert "IS NULL" in sql.upper()

    def test_total_count_present(self):
        sql = build_null_check_sql("t", "x")
        assert "COUNT(*)" in sql.upper()

    def test_returns_string(self):
        assert isinstance(build_null_check_sql("t", "c"), str)


# ---------------------------------------------------------------------------
# build_uniqueness_check_sql
# ---------------------------------------------------------------------------

class TestBuildUniquenessSql:
    def test_check_name_literal(self):
        sql = build_uniqueness_check_sql("t", "order_id")
        assert "'unique'" in sql

    def test_single_column(self):
        sql = build_uniqueness_check_sql("t", "order_id")
        assert "order_id" in sql

    def test_composite_column(self):
        sql = build_uniqueness_check_sql("t", ["a", "b"])
        assert "a" in sql
        assert "b" in sql

    def test_count_distinct_present(self):
        sql = build_uniqueness_check_sql("t", "k")
        assert "DISTINCT" in sql.upper()

    def test_from_table(self):
        sql = build_uniqueness_check_sql("catalog.gold.dim", "id")
        assert "FROM catalog.gold.dim" in sql


# ---------------------------------------------------------------------------
# build_range_check_sql
# ---------------------------------------------------------------------------

class TestBuildRangeSql:
    def test_check_name_literal(self):
        sql = build_range_check_sql("t", "age", 0, 120)
        assert "'value_range'" in sql

    def test_min_condition(self):
        sql = build_range_check_sql("t", "age", 0, None)
        assert "age < 0" in sql

    def test_max_condition(self):
        sql = build_range_check_sql("t", "age", None, 120)
        assert "age > 120" in sql

    def test_both_bounds(self):
        sql = build_range_check_sql("t", "price", 0, 9999)
        assert "price < 0" in sql
        assert "price > 9999" in sql

    def test_no_bounds_false(self):
        sql = build_range_check_sql("t", "x")
        assert "FALSE" in sql.upper()

    def test_column_in_output(self):
        sql = build_range_check_sql("t", "score", 0, 100)
        assert "'score'" in sql


# ---------------------------------------------------------------------------
# build_regex_check_sql
# ---------------------------------------------------------------------------

class TestBuildRegexSql:
    def test_check_name_literal(self):
        sql = build_regex_check_sql("t", "email", r".+@.+\..+")
        assert "'regex_match'" in sql

    def test_regexp_like_present(self):
        sql = build_regex_check_sql("t", "code", r"^[A-Z]{2}\d{4}$")
        assert "REGEXP_LIKE" in sql.upper()

    def test_column_referenced(self):
        sql = build_regex_check_sql("t", "phone", r"\d{10}")
        assert "phone" in sql

    def test_pattern_in_sql(self):
        sql = build_regex_check_sql("t", "c", r"\d+")
        assert r"\d+" in sql

    def test_not_null_guard(self):
        sql = build_regex_check_sql("t", "c", r"\d+")
        assert "IS NOT NULL" in sql.upper()

    def test_single_quote_escaped(self):
        sql = build_regex_check_sql("t", "c", r"it\'s")
        assert "\\'" in sql


# ---------------------------------------------------------------------------
# build_accepted_values_check_sql
# ---------------------------------------------------------------------------

class TestBuildAcceptedValuesSql:
    def test_check_name_literal(self):
        sql = build_accepted_values_check_sql("t", "status", ["active", "inactive"])
        assert "'accepted_values'" in sql

    def test_accepted_values_in_clause(self):
        sql = build_accepted_values_check_sql("t", "status", ["active", "inactive"])
        assert "'active'" in sql
        assert "'inactive'" in sql

    def test_not_in_logic(self):
        sql = build_accepted_values_check_sql("t", "tier", ["gold", "silver"])
        assert "NOT IN" in sql.upper()

    def test_column_in_output(self):
        sql = build_accepted_values_check_sql("t", "color", ["red", "blue"])
        assert "'color'" in sql

    def test_from_table(self):
        sql = build_accepted_values_check_sql("catalog.silver.x", "c", ["a"])
        assert "FROM catalog.silver.x" in sql


# ---------------------------------------------------------------------------
# build_row_count_check_sql
# ---------------------------------------------------------------------------

class TestBuildRowCountSql:
    def test_check_name_literal(self):
        sql = build_row_count_check_sql("t", min_rows=100)
        assert "'row_count'" in sql

    def test_min_check(self):
        sql = build_row_count_check_sql("t", min_rows=100)
        assert "cnt < 100" in sql

    def test_max_check(self):
        sql = build_row_count_check_sql("t", max_rows=1000)
        assert "cnt > 1000" in sql

    def test_both_bounds(self):
        sql = build_row_count_check_sql("t", min_rows=10, max_rows=500)
        assert "cnt < 10" in sql
        assert "cnt > 500" in sql

    def test_no_bounds_false(self):
        sql = build_row_count_check_sql("t")
        assert "FALSE" in sql.upper()

    def test_uses_cte(self):
        sql = build_row_count_check_sql("t", min_rows=1)
        assert "WITH" in sql.upper()

    def test_count_star_in_cte(self):
        sql = build_row_count_check_sql("t", min_rows=1)
        assert "COUNT(*)" in sql.upper()


# ---------------------------------------------------------------------------
# build_completeness_report_sql
# ---------------------------------------------------------------------------

class TestBuildCompletenessReportSql:
    def test_single_column(self):
        sql = build_completeness_report_sql("t", ["email"])
        assert "'email'" in sql
        assert "IS NULL" in sql.upper()

    def test_multiple_columns_union(self):
        sql = build_completeness_report_sql("t", ["a", "b", "c"])
        assert sql.upper().count("UNION ALL") == 2

    def test_all_columns_present(self):
        sql = build_completeness_report_sql("t", ["first", "second"])
        assert "'first'" in sql
        assert "'second'" in sql

    def test_null_pct_computed(self):
        sql = build_completeness_report_sql("t", ["x"])
        assert "null_pct" in sql.lower() or "NULL_PCT" in sql.upper()


# ---------------------------------------------------------------------------
# build_quarantine_insert_sql
# ---------------------------------------------------------------------------

class TestBuildQuarantineInsertSql:
    def test_insert_into_target(self):
        sql = build_quarantine_insert_sql("src", "quar", "id IS NULL", "null id")
        assert "INSERT INTO quar" in sql

    def test_select_star_from_source(self):
        sql = build_quarantine_insert_sql("src", "quar", "id IS NULL", "null id")
        assert "FROM src" in sql

    def test_fail_reason_literal(self):
        sql = build_quarantine_insert_sql("src", "quar", "id IS NULL", "null id")
        assert "'null id'" in sql

    def test_where_condition(self):
        sql = build_quarantine_insert_sql("src", "quar", "age < 0", "negative age")
        assert "WHERE age < 0" in sql

    def test_default_fail_col_name(self):
        sql = build_quarantine_insert_sql("src", "quar", "x IS NULL", "no x")
        assert "_dq_fail_reason" in sql

    def test_custom_fail_col_name(self):
        sql = build_quarantine_insert_sql("src", "quar", "x IS NULL", "no x", dq_fail_col="_reason")
        assert "_reason" in sql
        assert "_dq_fail_reason" not in sql


# ---------------------------------------------------------------------------
# build_good_records_sql
# ---------------------------------------------------------------------------

class TestBuildGoodRecordsSql:
    def test_not_condition(self):
        sql = build_good_records_sql("src", "id IS NULL")
        assert "NOT" in sql.upper()

    def test_select_star_default(self):
        sql = build_good_records_sql("src", "id IS NULL")
        assert "SELECT *" in sql

    def test_select_specific_cols(self):
        sql = build_good_records_sql("src", "id IS NULL", select_cols=["a", "b"])
        assert "a, b" in sql
        assert "SELECT *" not in sql

    def test_from_source(self):
        sql = build_good_records_sql("catalog.bronze.raw", "x IS NULL")
        assert "FROM catalog.bronze.raw" in sql


# ---------------------------------------------------------------------------
# build_null_quarantine_sql
# ---------------------------------------------------------------------------

class TestBuildNullQuarantineSql:
    def test_generates_insert(self):
        sql = build_null_quarantine_sql("src", "quar", ["id", "email"])
        assert "INSERT INTO" in sql.upper()

    def test_or_condition_for_multiple_cols(self):
        sql = build_null_quarantine_sql("src", "quar", ["a", "b", "c"])
        assert "IS NULL" in sql.upper()
        assert "OR" in sql.upper()

    def test_reason_mentions_cols(self):
        sql = build_null_quarantine_sql("src", "quar", ["customer_id"])
        assert "customer_id" in sql

    def test_single_col(self):
        sql = build_null_quarantine_sql("src", "quar", ["id"])
        assert "id IS NULL" in sql


# ---------------------------------------------------------------------------
# build_range_quarantine_sql
# ---------------------------------------------------------------------------

class TestBuildRangeQuarantineSql:
    def test_min_condition(self):
        sql = build_range_quarantine_sql("src", "quar", "age", min_val=0)
        assert "age < 0" in sql

    def test_max_condition(self):
        sql = build_range_quarantine_sql("src", "quar", "score", max_val=100)
        assert "score > 100" in sql

    def test_both_bounds(self):
        sql = build_range_quarantine_sql("src", "quar", "price", min_val=0, max_val=9999)
        assert "price < 0" in sql
        assert "price > 9999" in sql

    def test_no_bounds_false(self):
        sql = build_range_quarantine_sql("src", "quar", "x")
        assert "FALSE" in sql.upper()

    def test_generates_insert(self):
        sql = build_range_quarantine_sql("src", "quar", "age", min_val=0)
        assert "INSERT INTO" in sql.upper()
