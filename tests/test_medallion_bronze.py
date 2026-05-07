"""Tests for medallion.bronze_pyspark — requires Spark, skipped locally."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from medallion.bronze_pyspark import add_bronze_metadata
from medallion.config import BRONZE_METADATA_COLS


SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("payload", StringType(), True),
])


class TestAddBronzeMetadata:
    def test_metadata_cols_added(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], SCHEMA)
        tagged = add_bronze_metadata(df)
        for col in BRONZE_METADATA_COLS:
            assert col in tagged.columns

    def test_original_cols_preserved(self, spark):
        df = spark.createDataFrame([(1, "a")], SCHEMA)
        tagged = add_bronze_metadata(df)
        assert "id" in tagged.columns
        assert "payload" in tagged.columns

    def test_ingest_ts_not_null(self, spark):
        df = spark.createDataFrame([(1, "a")], SCHEMA)
        tagged = add_bronze_metadata(df)
        null_count = tagged.filter(F.col("_ingest_ts").isNull()).count()
        assert null_count == 0

    def test_batch_id_default_zero(self, spark):
        df = spark.createDataFrame([(1, "a")], SCHEMA)
        tagged = add_bronze_metadata(df)
        assert tagged.collect()[0]["_batch_id"] == 0

    def test_batch_id_custom(self, spark):
        df = spark.createDataFrame([(1, "a")], SCHEMA)
        tagged = add_bronze_metadata(df, batch_id=42)
        assert tagged.collect()[0]["_batch_id"] == 42

    def test_source_file_col_present(self, spark):
        df = spark.createDataFrame([(1, "a")], SCHEMA)
        tagged = add_bronze_metadata(df)
        assert "_source_file" in tagged.columns

    def test_row_count_unchanged(self, spark):
        data = [(i, f"val_{i}") for i in range(10)]
        df = spark.createDataFrame(data, SCHEMA)
        tagged = add_bronze_metadata(df)
        assert tagged.count() == 10

    def test_batch_id_is_long(self, spark):
        df = spark.createDataFrame([(1, "a")], SCHEMA)
        tagged = add_bronze_metadata(df)
        batch_field = [f for f in tagged.schema.fields if f.name == "_batch_id"][0]
        assert "LongType" in str(type(batch_field.dataType))
