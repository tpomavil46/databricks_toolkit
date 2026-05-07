"""Tests for optimization.skew_pyspark — requires Spark, skipped locally."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from optimization.skew_pyspark import (
    broadcast_join,
    detect_skew,
    salt_skewed_join,
    filter_union_join,
)


SCHEMA_LEFT = StructType([
    StructField("id", IntegerType(), True),
    StructField("key", StringType(), True),
    StructField("value", IntegerType(), True),
])

SCHEMA_RIGHT = StructType([
    StructField("key", StringType(), True),
    StructField("label", StringType(), True),
])


# ---------------------------------------------------------------------------
# broadcast_join
# ---------------------------------------------------------------------------

class TestBroadcastJoin:
    def test_returns_correct_row_count(self, spark):
        left = spark.createDataFrame(
            [(1, "a", 10), (2, "b", 20), (3, "a", 30)], SCHEMA_LEFT
        )
        right = spark.createDataFrame([("a", "alpha"), ("b", "beta")], SCHEMA_RIGHT)
        result = broadcast_join(left, right, on="key")
        assert result.count() == 3

    def test_joined_cols_present(self, spark):
        left = spark.createDataFrame([(1, "a", 10)], SCHEMA_LEFT)
        right = spark.createDataFrame([("a", "alpha")], SCHEMA_RIGHT)
        result = broadcast_join(left, right, on="key")
        assert "label" in result.columns
        assert "value" in result.columns

    def test_left_join_includes_unmatched(self, spark):
        left = spark.createDataFrame(
            [(1, "a", 10), (2, "z", 20)], SCHEMA_LEFT
        )
        right = spark.createDataFrame([("a", "alpha")], SCHEMA_RIGHT)
        result = broadcast_join(left, right, on="key", join_type="left")
        assert result.count() == 2

    def test_inner_join_excludes_unmatched(self, spark):
        left = spark.createDataFrame(
            [(1, "a", 10), (2, "z", 20)], SCHEMA_LEFT
        )
        right = spark.createDataFrame([("a", "alpha")], SCHEMA_RIGHT)
        result = broadcast_join(left, right, on="key", join_type="inner")
        assert result.count() == 1


# ---------------------------------------------------------------------------
# detect_skew
# ---------------------------------------------------------------------------

class TestDetectSkew:
    def test_no_skew_returns_empty(self, spark):
        df = spark.createDataFrame(
            [(i, str(i % 10)) for i in range(100)],
            StructType([
                StructField("id", IntegerType(), True),
                StructField("key", StringType(), True),
            ])
        )
        results = detect_skew(df, "key", sample_fraction=1.0, threshold=3.0)
        assert results == []

    def test_skewed_key_detected(self, spark):
        rows = [(i, "hot") for i in range(90)] + [(i + 90, str(i)) for i in range(10)]
        df = spark.createDataFrame(
            rows,
            StructType([
                StructField("id", IntegerType(), True),
                StructField("key", StringType(), True),
            ])
        )
        results = detect_skew(df, "key", sample_fraction=1.0, threshold=3.0)
        assert len(results) > 0
        hot_result = next((r for r in results if r["value"] == "hot"), None)
        assert hot_result is not None
        assert hot_result["skew_factor"] > 3.0

    def test_result_has_expected_keys(self, spark):
        rows = [(i, "x") for i in range(50)] + [(i + 50, "y") for i in range(5)]
        df = spark.createDataFrame(
            rows,
            StructType([
                StructField("id", IntegerType(), True),
                StructField("key", StringType(), True),
            ])
        )
        results = detect_skew(df, "key", sample_fraction=1.0, threshold=2.0)
        if results:
            assert "value" in results[0]
            assert "count" in results[0]
            assert "skew_factor" in results[0]

    def test_empty_df_returns_empty(self, spark):
        df = spark.createDataFrame(
            [],
            StructType([
                StructField("id", IntegerType(), True),
                StructField("key", StringType(), True),
            ])
        )
        results = detect_skew(df, "key", sample_fraction=1.0)
        assert results == []


# ---------------------------------------------------------------------------
# salt_skewed_join
# ---------------------------------------------------------------------------

class TestSaltSkewedJoin:
    def test_all_matching_rows_present(self, spark):
        left = spark.createDataFrame(
            [(1, "a", 10), (2, "a", 20), (3, "b", 30)], SCHEMA_LEFT
        )
        right = spark.createDataFrame([("a", "alpha"), ("b", "beta")], SCHEMA_RIGHT)
        result = salt_skewed_join(left, right, join_col="key", num_buckets=3)
        assert result.count() == 3

    def test_no_salt_col_in_output(self, spark):
        left = spark.createDataFrame([(1, "a", 10)], SCHEMA_LEFT)
        right = spark.createDataFrame([("a", "alpha")], SCHEMA_RIGHT)
        result = salt_skewed_join(left, right, join_col="key", num_buckets=2)
        assert "_salt" not in result.columns

    def test_result_has_both_side_cols(self, spark):
        left = spark.createDataFrame([(1, "a", 10)], SCHEMA_LEFT)
        right = spark.createDataFrame([("a", "alpha")], SCHEMA_RIGHT)
        result = salt_skewed_join(left, right, join_col="key", num_buckets=2)
        assert "value" in result.columns
        assert "label" in result.columns

    def test_num_buckets_one_raises(self, spark):
        left = spark.createDataFrame([(1, "a", 10)], SCHEMA_LEFT)
        right = spark.createDataFrame([("a", "alpha")], SCHEMA_RIGHT)
        with pytest.raises(ValueError):
            salt_skewed_join(left, right, join_col="key", num_buckets=1)


# ---------------------------------------------------------------------------
# filter_union_join
# ---------------------------------------------------------------------------

class TestFilterUnionJoin:
    def test_result_row_count(self, spark):
        left = spark.createDataFrame(
            [(1, "hot", 10), (2, "hot", 20), (3, "normal", 30)], SCHEMA_LEFT
        )
        right = spark.createDataFrame([("hot", "skewed"), ("normal", "ok")], SCHEMA_RIGHT)
        result = filter_union_join(left, right, "key", skew_values=["hot"])
        assert result.count() == 3

    def test_no_skew_values_raises(self, spark):
        left = spark.createDataFrame([(1, "a", 10)], SCHEMA_LEFT)
        right = spark.createDataFrame([("a", "x")], SCHEMA_RIGHT)
        with pytest.raises(ValueError):
            filter_union_join(left, right, "key", skew_values=[])

    def test_cols_present_in_output(self, spark):
        left = spark.createDataFrame([(1, "hot", 10)], SCHEMA_LEFT)
        right = spark.createDataFrame([("hot", "skewed")], SCHEMA_RIGHT)
        result = filter_union_join(left, right, "key", skew_values=["hot"])
        assert "label" in result.columns
        assert "value" in result.columns

    def test_unmatched_normal_rows_excluded_in_inner(self, spark):
        left = spark.createDataFrame(
            [(1, "hot", 10), (2, "missing", 20)], SCHEMA_LEFT
        )
        right = spark.createDataFrame([("hot", "skewed")], SCHEMA_RIGHT)
        result = filter_union_join(left, right, "key", skew_values=["hot"], join_type="inner")
        assert result.count() == 1
