"""Tests for data_quality.quarantine_pyspark — requires Spark, skipped locally."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_quality.quarantine_pyspark import (
    DQ_FAIL_COL,
    tag_null_failures,
    tag_range_failures,
    tag_regex_failures,
    tag_accepted_value_failures,
    tag_with_all_failures,
    split_good_bad,
)


SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("status", StringType(), True),
])


# ---------------------------------------------------------------------------
# tag_null_failures
# ---------------------------------------------------------------------------

class TestTagNullFailures:
    def test_fail_col_added(self, spark):
        df = spark.createDataFrame([(1, "a@b.com", 25, "active")], SCHEMA)
        tagged = tag_null_failures(df, ["id"])
        assert DQ_FAIL_COL in tagged.columns

    def test_non_null_row_passes(self, spark):
        df = spark.createDataFrame([(1, "a@b.com", 25, "active")], SCHEMA)
        tagged = tag_null_failures(df, ["id", "email"])
        row = tagged.collect()[0]
        assert row[DQ_FAIL_COL] is None

    def test_null_row_tagged(self, spark):
        df = spark.createDataFrame(
            [(None, "a@b.com", 25, "active")], SCHEMA
        )
        tagged = tag_null_failures(df, ["id"])
        row = tagged.collect()[0]
        assert row[DQ_FAIL_COL] is not None
        assert "id" in row[DQ_FAIL_COL]

    def test_multiple_required_cols_any_null_fails(self, spark):
        df = spark.createDataFrame(
            [(1, None, 25, "active")], SCHEMA
        )
        tagged = tag_null_failures(df, ["id", "email"])
        row = tagged.collect()[0]
        assert row[DQ_FAIL_COL] is not None

    def test_all_pass(self, spark):
        df = spark.createDataFrame(
            [(1, "a@b.com", 25, "active"), (2, "c@d.com", 30, "inactive")],
            SCHEMA,
        )
        tagged = tag_null_failures(df, ["id", "email"])
        null_count = tagged.filter(F.col(DQ_FAIL_COL).isNotNull()).count()
        assert null_count == 0

    def test_custom_fail_col(self, spark):
        df = spark.createDataFrame([(None, None, None, None)], SCHEMA)
        tagged = tag_null_failures(df, ["id"], fail_col="_reason")
        assert "_reason" in tagged.columns
        assert DQ_FAIL_COL not in tagged.columns


# ---------------------------------------------------------------------------
# tag_range_failures
# ---------------------------------------------------------------------------

class TestTagRangeFailures:
    def test_in_range_passes(self, spark):
        df = spark.createDataFrame([(1, "e", 25, "s")], SCHEMA)
        tagged = tag_range_failures(df, "age", 0, 100)
        assert tagged.collect()[0][DQ_FAIL_COL] is None

    def test_below_min_tagged(self, spark):
        df = spark.createDataFrame([(1, "e", -1, "s")], SCHEMA)
        tagged = tag_range_failures(df, "age", 0, 100)
        row = tagged.collect()[0]
        assert row[DQ_FAIL_COL] is not None
        assert "age" in row[DQ_FAIL_COL]

    def test_above_max_tagged(self, spark):
        df = spark.createDataFrame([(1, "e", 150, "s")], SCHEMA)
        tagged = tag_range_failures(df, "age", 0, 100)
        row = tagged.collect()[0]
        assert row[DQ_FAIL_COL] is not None

    def test_only_min(self, spark):
        df = spark.createDataFrame([(1, "e", -5, "s")], SCHEMA)
        tagged = tag_range_failures(df, "age", min_val=0)
        assert tagged.collect()[0][DQ_FAIL_COL] is not None

    def test_only_max(self, spark):
        df = spark.createDataFrame([(1, "e", 200, "s")], SCHEMA)
        tagged = tag_range_failures(df, "age", max_val=100)
        assert tagged.collect()[0][DQ_FAIL_COL] is not None

    def test_reason_includes_value(self, spark):
        df = spark.createDataFrame([(1, "e", -5, "s")], SCHEMA)
        tagged = tag_range_failures(df, "age", min_val=0)
        reason = tagged.collect()[0][DQ_FAIL_COL]
        assert "-5" in reason


# ---------------------------------------------------------------------------
# tag_regex_failures
# ---------------------------------------------------------------------------

class TestTagRegexFailures:
    def test_matching_passes(self, spark):
        df = spark.createDataFrame([(1, "user@example.com", 25, "active")], SCHEMA)
        tagged = tag_regex_failures(df, "email", r".+@.+\..+")
        assert tagged.collect()[0][DQ_FAIL_COL] is None

    def test_non_matching_tagged(self, spark):
        df = spark.createDataFrame([(1, "notanemail", 25, "active")], SCHEMA)
        tagged = tag_regex_failures(df, "email", r".+@.+\..+")
        row = tagged.collect()[0]
        assert row[DQ_FAIL_COL] is not None

    def test_null_not_tagged(self, spark):
        df = spark.createDataFrame([(1, None, 25, "active")], SCHEMA)
        tagged = tag_regex_failures(df, "email", r".+@.+\..+")
        assert tagged.collect()[0][DQ_FAIL_COL] is None

    def test_reason_includes_col_name(self, spark):
        df = spark.createDataFrame([(1, "bad", 25, "active")], SCHEMA)
        tagged = tag_regex_failures(df, "email", r".+@.+\..+")
        reason = tagged.collect()[0][DQ_FAIL_COL]
        assert "email" in reason


# ---------------------------------------------------------------------------
# tag_accepted_value_failures
# ---------------------------------------------------------------------------

class TestTagAcceptedValueFailures:
    def test_accepted_passes(self, spark):
        df = spark.createDataFrame([(1, "e", 25, "active")], SCHEMA)
        tagged = tag_accepted_value_failures(df, "status", ["active", "inactive"])
        assert tagged.collect()[0][DQ_FAIL_COL] is None

    def test_not_accepted_tagged(self, spark):
        df = spark.createDataFrame([(1, "e", 25, "unknown")], SCHEMA)
        tagged = tag_accepted_value_failures(df, "status", ["active", "inactive"])
        assert tagged.collect()[0][DQ_FAIL_COL] is not None

    def test_null_not_tagged(self, spark):
        df = spark.createDataFrame([(1, "e", 25, None)], SCHEMA)
        tagged = tag_accepted_value_failures(df, "status", ["active"])
        assert tagged.collect()[0][DQ_FAIL_COL] is None

    def test_reason_includes_value(self, spark):
        df = spark.createDataFrame([(1, "e", 25, "bad_status")], SCHEMA)
        tagged = tag_accepted_value_failures(df, "status", ["active"])
        reason = tagged.collect()[0][DQ_FAIL_COL]
        assert "bad_status" in reason


# ---------------------------------------------------------------------------
# tag_with_all_failures — multi-check composition
# ---------------------------------------------------------------------------

class TestTagWithAllFailures:
    def test_no_failures_all_null(self, spark):
        df = spark.createDataFrame([(1, "a@b.com", 25, "active")], SCHEMA)
        tagged = tag_with_all_failures(df, [
            lambda d: tag_null_failures(d, ["id"]),
            lambda d: tag_range_failures(d, "age", 0, 100),
        ])
        assert tagged.collect()[0][DQ_FAIL_COL] is None

    def test_first_failure_wins(self, spark):
        df = spark.createDataFrame([(None, "bad", -5, "unknown")], SCHEMA)
        tagged = tag_with_all_failures(df, [
            lambda d: tag_null_failures(d, ["id"]),
            lambda d: tag_range_failures(d, "age", 0, 100),
        ])
        reason = tagged.collect()[0][DQ_FAIL_COL]
        assert reason is not None
        assert "id" in reason  # null check fires first

    def test_second_fn_fires_when_first_passes(self, spark):
        df = spark.createDataFrame([(1, "bademail", 150, "active")], SCHEMA)
        tagged = tag_with_all_failures(df, [
            lambda d: tag_null_failures(d, ["id"]),
            lambda d: tag_range_failures(d, "age", 0, 100),
        ])
        reason = tagged.collect()[0][DQ_FAIL_COL]
        assert reason is not None
        assert "age" in reason

    def test_empty_fns_adds_null_col(self, spark):
        df = spark.createDataFrame([(1, "e", 25, "active")], SCHEMA)
        tagged = tag_with_all_failures(df, [])
        assert DQ_FAIL_COL in tagged.columns
        assert tagged.collect()[0][DQ_FAIL_COL] is None

    def test_multiple_rows_independent(self, spark):
        df = spark.createDataFrame(
            [(None, "a@b.com", 25, "active"), (1, "a@b.com", 25, "active")],
            SCHEMA,
        )
        tagged = tag_with_all_failures(df, [
            lambda d: tag_null_failures(d, ["id"]),
        ])
        rows = tagged.orderBy("age").collect()
        bad_row = [r for r in rows if r["id"] is None][0]
        good_row = [r for r in rows if r["id"] == 1][0]
        assert bad_row[DQ_FAIL_COL] is not None
        assert good_row[DQ_FAIL_COL] is None


# ---------------------------------------------------------------------------
# split_good_bad
# ---------------------------------------------------------------------------

class TestSplitGoodBad:
    def test_good_rows_no_fail_col(self, spark):
        df = spark.createDataFrame(
            [(None, "e", 25, "a"), (1, "e", 25, "a")], SCHEMA
        )
        tagged = tag_null_failures(df, ["id"])
        good, bad = split_good_bad(tagged)
        assert DQ_FAIL_COL not in good.columns

    def test_bad_rows_retain_fail_col(self, spark):
        df = spark.createDataFrame([(None, "e", 25, "a")], SCHEMA)
        tagged = tag_null_failures(df, ["id"])
        good, bad = split_good_bad(tagged)
        assert DQ_FAIL_COL in bad.columns

    def test_counts_split_correctly(self, spark):
        df = spark.createDataFrame(
            [(None, "e", 25, "a"), (1, "e", 30, "b"), (2, "e", 35, "c")],
            SCHEMA,
        )
        tagged = tag_null_failures(df, ["id"])
        good, bad = split_good_bad(tagged)
        assert good.count() == 2
        assert bad.count() == 1

    def test_all_good(self, spark):
        df = spark.createDataFrame([(1, "e", 25, "a"), (2, "e", 30, "b")], SCHEMA)
        tagged = tag_null_failures(df, ["id"])
        good, bad = split_good_bad(tagged)
        assert good.count() == 2
        assert bad.count() == 0

    def test_all_bad(self, spark):
        df = spark.createDataFrame(
            [(None, "e", 25, "a"), (None, "e", 30, "b")], SCHEMA
        )
        tagged = tag_null_failures(df, ["id"])
        good, bad = split_good_bad(tagged)
        assert good.count() == 0
        assert bad.count() == 2

    def test_custom_fail_col(self, spark):
        df = spark.createDataFrame([(None, "e", 25, "a")], SCHEMA)
        tagged = tag_null_failures(df, ["id"], fail_col="_my_reason")
        good, bad = split_good_bad(tagged, fail_col="_my_reason")
        assert "_my_reason" not in good.columns
        assert "_my_reason" in bad.columns

    def test_good_schema_minus_fail_col(self, spark):
        df = spark.createDataFrame([(1, "e", 25, "a")], SCHEMA)
        tagged = tag_null_failures(df, ["id"])
        good, _ = split_good_bad(tagged)
        assert set(good.columns) == set(df.columns)
