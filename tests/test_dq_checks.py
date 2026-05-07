"""Tests for data_quality.checks_pyspark — requires Spark, skipped locally."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_quality.checks_pyspark import (
    check_not_null,
    check_unique,
    check_value_range,
    check_regex,
    check_accepted_values,
    check_referential_integrity,
    check_row_count,
    check_freshness,
    run_checks,
)


# ---------------------------------------------------------------------------
# check_not_null
# ---------------------------------------------------------------------------

class TestCheckNotNull:
    def test_all_pass(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        result = check_not_null(df, "id")
        assert result.passed is True
        assert result.failing_count == 0
        assert result.total_count == 2
        assert result.check_name == "not_null"
        assert result.column == "id"

    def test_some_null(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (None, "b"), (None, "c")],
            StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ])
        )
        result = check_not_null(df, "id")
        assert result.passed is False
        assert result.failing_count == 2
        assert result.total_count == 3

    def test_all_null(self, spark):
        df = spark.createDataFrame(
            [(None,), (None,)],
            StructType([StructField("x", IntegerType(), True)])
        )
        result = check_not_null(df, "x")
        assert result.passed is False
        assert result.failing_count == 2

    def test_details_on_failure(self, spark):
        df = spark.createDataFrame(
            [(None,)],
            StructType([StructField("col", StringType(), True)])
        )
        result = check_not_null(df, "col")
        assert "col" in result.details

    def test_details_empty_on_pass(self, spark):
        df = spark.createDataFrame([("x",)], ["col"])
        result = check_not_null(df, "col")
        assert result.details == ""


# ---------------------------------------------------------------------------
# check_unique
# ---------------------------------------------------------------------------

class TestCheckUnique:
    def test_all_unique(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        result = check_unique(df, "id")
        assert result.passed is True
        assert result.failing_count == 0

    def test_duplicates_detected(self, spark):
        df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
        result = check_unique(df, "id")
        assert result.passed is False
        assert result.failing_count == 2

    def test_composite_key_unique(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (1, "b"), (2, "a")],
            ["user_id", "product_id"]
        )
        result = check_unique(df, ["user_id", "product_id"])
        assert result.passed is True

    def test_composite_key_duplicate(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (1, "a"), (2, "b")],
            ["user_id", "product_id"]
        )
        result = check_unique(df, ["user_id", "product_id"])
        assert result.passed is False
        assert result.failing_count == 2

    def test_check_name(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        result = check_unique(df, "id")
        assert result.check_name == "unique"

    def test_total_count(self, spark):
        df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
        result = check_unique(df, "id")
        assert result.total_count == 3


# ---------------------------------------------------------------------------
# check_value_range
# ---------------------------------------------------------------------------

class TestCheckValueRange:
    def test_all_in_range(self, spark):
        df = spark.createDataFrame([(10,), (50,), (99,)], ["score"])
        result = check_value_range(df, "score", 0, 100)
        assert result.passed is True

    def test_below_min(self, spark):
        df = spark.createDataFrame([(-1,), (50,)], ["score"])
        result = check_value_range(df, "score", 0, 100)
        assert result.passed is False

    def test_above_max(self, spark):
        df = spark.createDataFrame([(50,), (101,)], ["score"])
        result = check_value_range(df, "score", 0, 100)
        assert result.passed is False

    def test_only_min_bound(self, spark):
        df = spark.createDataFrame([(-5,), (10,)], ["x"])
        result = check_value_range(df, "x", min_val=0)
        assert result.passed is False
        assert result.failing_count == 1

    def test_only_max_bound(self, spark):
        df = spark.createDataFrame([(5,), (200,)], ["x"])
        result = check_value_range(df, "x", max_val=100)
        assert result.passed is False
        assert result.failing_count == 1

    def test_no_bounds_always_pass(self, spark):
        df = spark.createDataFrame([(999999,)], ["x"])
        result = check_value_range(df, "x")
        assert result.passed is True
        assert result.failing_count == 0

    def test_check_name(self, spark):
        df = spark.createDataFrame([(1,)], ["x"])
        result = check_value_range(df, "x", 0, 10)
        assert result.check_name == "value_range"


# ---------------------------------------------------------------------------
# check_regex
# ---------------------------------------------------------------------------

class TestCheckRegex:
    def test_all_match(self, spark):
        df = spark.createDataFrame([("abc@x.com",), ("def@y.org",)], ["email"])
        result = check_regex(df, "email", r".+@.+\..+")
        assert result.passed is True

    def test_some_no_match(self, spark):
        df = spark.createDataFrame([("valid@x.com",), ("notanemail",)], ["email"])
        result = check_regex(df, "email", r".+@.+\..+")
        assert result.passed is False
        assert result.failing_count == 1

    def test_null_not_counted_as_failure(self, spark):
        df = spark.createDataFrame(
            [(None,), ("valid@x.com",)],
            StructType([StructField("email", StringType(), True)])
        )
        result = check_regex(df, "email", r".+@.+\..+")
        assert result.passed is True
        assert result.failing_count == 0

    def test_check_name(self, spark):
        df = spark.createDataFrame([("abc",)], ["c"])
        result = check_regex(df, "c", r".+")
        assert result.check_name == "regex_match"


# ---------------------------------------------------------------------------
# check_accepted_values
# ---------------------------------------------------------------------------

class TestCheckAcceptedValues:
    def test_all_accepted(self, spark):
        df = spark.createDataFrame([("active",), ("inactive",)], ["status"])
        result = check_accepted_values(df, "status", ["active", "inactive", "pending"])
        assert result.passed is True

    def test_some_not_accepted(self, spark):
        df = spark.createDataFrame([("active",), ("unknown",)], ["status"])
        result = check_accepted_values(df, "status", ["active", "inactive"])
        assert result.passed is False
        assert result.failing_count == 1

    def test_null_not_failure(self, spark):
        df = spark.createDataFrame(
            [(None,), ("active",)],
            StructType([StructField("status", StringType(), True)])
        )
        result = check_accepted_values(df, "status", ["active"])
        assert result.passed is True

    def test_check_name(self, spark):
        df = spark.createDataFrame([("a",)], ["c"])
        result = check_accepted_values(df, "c", ["a", "b"])
        assert result.check_name == "accepted_values"


# ---------------------------------------------------------------------------
# check_referential_integrity
# ---------------------------------------------------------------------------

class TestCheckReferentialIntegrity:
    def test_all_keys_match(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["customer_id"])
        ref = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        result = check_referential_integrity(df, "customer_id", ref, "id")
        assert result.passed is True

    def test_missing_key(self, spark):
        df = spark.createDataFrame([(1,), (99,)], ["customer_id"])
        ref = spark.createDataFrame([(1,), (2,)], ["id"])
        result = check_referential_integrity(df, "customer_id", ref, "id")
        assert result.passed is False
        assert result.failing_count == 1

    def test_null_not_checked(self, spark):
        df = spark.createDataFrame(
            [(None,), (1,)],
            StructType([StructField("customer_id", IntegerType(), True)])
        )
        ref = spark.createDataFrame([(1,)], ["id"])
        result = check_referential_integrity(df, "customer_id", ref, "id")
        assert result.passed is True

    def test_check_name(self, spark):
        df = spark.createDataFrame([(1,)], ["fk"])
        ref = spark.createDataFrame([(1,)], ["pk"])
        result = check_referential_integrity(df, "fk", ref, "pk")
        assert result.check_name == "referential_integrity"


# ---------------------------------------------------------------------------
# check_row_count
# ---------------------------------------------------------------------------

class TestCheckRowCount:
    def test_count_in_range(self, spark):
        df = spark.createDataFrame([(i,) for i in range(10)], ["x"])
        result = check_row_count(df, min_rows=5, max_rows=20)
        assert result.passed is True
        assert result.failing_count == 0

    def test_below_min(self, spark):
        df = spark.createDataFrame([(1,)], ["x"])
        result = check_row_count(df, min_rows=10)
        assert result.passed is False
        assert result.failing_count == 1

    def test_above_max(self, spark):
        df = spark.createDataFrame([(i,) for i in range(100)], ["x"])
        result = check_row_count(df, max_rows=50)
        assert result.passed is False
        assert result.failing_count == 1

    def test_no_bounds_always_pass(self, spark):
        df = spark.createDataFrame([(1,)], ["x"])
        result = check_row_count(df)
        assert result.passed is True

    def test_total_count_is_actual(self, spark):
        df = spark.createDataFrame([(i,) for i in range(7)], ["x"])
        result = check_row_count(df, min_rows=1)
        assert result.total_count == 7

    def test_check_name(self, spark):
        df = spark.createDataFrame([(1,)], ["x"])
        result = check_row_count(df)
        assert result.check_name == "row_count"


# ---------------------------------------------------------------------------
# check_freshness
# ---------------------------------------------------------------------------

class TestCheckFreshness:
    def test_fresh_data(self, spark):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        recent = now - timedelta(hours=1)
        df = spark.createDataFrame(
            [(recent,)],
            StructType([StructField("ts", TimestampType(), True)])
        )
        result = check_freshness(df, "ts", max_age_hours=2)
        assert result.passed is True

    def test_stale_data(self, spark):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        old = now - timedelta(hours=48)
        df = spark.createDataFrame(
            [(old,)],
            StructType([StructField("ts", TimestampType(), True)])
        )
        result = check_freshness(df, "ts", max_age_hours=24)
        assert result.passed is False
        assert result.failing_count == 1

    def test_empty_table(self, spark):
        df = spark.createDataFrame(
            [],
            StructType([StructField("ts", TimestampType(), True)])
        )
        result = check_freshness(df, "ts", max_age_hours=24)
        assert result.passed is False
        assert "empty" in result.details.lower()

    def test_check_name(self, spark):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        df = spark.createDataFrame(
            [(now,)],
            StructType([StructField("ts", TimestampType(), True)])
        )
        result = check_freshness(df, "ts", max_age_hours=24)
        assert result.check_name == "freshness"


# ---------------------------------------------------------------------------
# run_checks
# ---------------------------------------------------------------------------

class TestRunChecks:
    def test_returns_list_of_results(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        results = run_checks(df, [
            lambda d: check_not_null(d, "id"),
            lambda d: check_unique(d, "id"),
        ])
        assert len(results) == 2

    def test_empty_check_list(self, spark):
        df = spark.createDataFrame([(1,)], ["x"])
        results = run_checks(df, [])
        assert results == []

    def test_all_results_are_dqresult(self, spark):
        from data_quality.config import DQResult
        df = spark.createDataFrame([(1,)], ["x"])
        results = run_checks(df, [lambda d: check_not_null(d, "x")])
        assert all(isinstance(r, DQResult) for r in results)

    def test_failure_rate_computed(self, spark):
        df = spark.createDataFrame(
            [(None,), (1,)],
            StructType([StructField("x", IntegerType(), True)])
        )
        results = run_checks(df, [lambda d: check_not_null(d, "x")])
        assert results[0].failure_rate == pytest.approx(0.5)
