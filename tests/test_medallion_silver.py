"""Tests for medallion.silver_pyspark — requires Spark, skipped locally."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from medallion.bronze_pyspark import add_bronze_metadata
from medallion.config import BRONZE_METADATA_COLS, MedallionConfig
from medallion.silver_pyspark import (
    deduplicate_silver,
    drop_bronze_metadata,
    apply_silver_transform,
)


SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("ts", TimestampType(), True),
])


@pytest.fixture
def cfg():
    return MedallionConfig(
        source_path="s3://b/r",
        bronze_table="b.bronze.t",
        silver_table="b.silver.t",
        gold_table="b.gold.t",
        checkpoint_base="dbfs:/cp",
        pipeline_name="pipe",
        id_col="id",
        ts_col="ts",
    )


# ---------------------------------------------------------------------------
# drop_bronze_metadata
# ---------------------------------------------------------------------------

class TestDropBronzeMetadata:
    def test_metadata_cols_removed(self, spark):
        df = spark.createDataFrame([(1, "a", None)], SCHEMA)
        tagged = add_bronze_metadata(df)
        cleaned = drop_bronze_metadata(tagged)
        for col in BRONZE_METADATA_COLS:
            assert col not in cleaned.columns

    def test_payload_cols_preserved(self, spark):
        df = spark.createDataFrame([(1, "a", None)], SCHEMA)
        tagged = add_bronze_metadata(df)
        cleaned = drop_bronze_metadata(tagged)
        assert "id" in cleaned.columns
        assert "name" in cleaned.columns

    def test_safe_when_cols_absent(self, spark):
        df = spark.createDataFrame([(1, "a", None)], SCHEMA)
        cleaned = drop_bronze_metadata(df)
        assert set(cleaned.columns) == set(df.columns)

    def test_row_count_unchanged(self, spark):
        data = [(i, f"n{i}", None) for i in range(5)]
        df = spark.createDataFrame(data, SCHEMA)
        tagged = add_bronze_metadata(df)
        cleaned = drop_bronze_metadata(tagged)
        assert cleaned.count() == 5


# ---------------------------------------------------------------------------
# deduplicate_silver
# ---------------------------------------------------------------------------

class TestDeduplicateSilver:
    def test_unique_keys_unchanged(self, spark):
        from datetime import datetime, timezone
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
        df = spark.createDataFrame(
            [(1, "a", t1), (2, "b", t2)], SCHEMA
        )
        deduped = deduplicate_silver(df, "id", "ts")
        assert deduped.count() == 2

    def test_latest_wins(self, spark):
        from datetime import datetime, timezone
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
        df = spark.createDataFrame(
            [(1, "old", t1), (1, "new", t2)], SCHEMA
        )
        deduped = deduplicate_silver(df, "id", "ts")
        assert deduped.count() == 1
        assert deduped.collect()[0]["name"] == "new"

    def test_no_rn_col_in_output(self, spark):
        from datetime import datetime, timezone
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = spark.createDataFrame([(1, "a", t)], SCHEMA)
        deduped = deduplicate_silver(df, "id", "ts")
        assert "_rn" not in deduped.columns

    def test_multiple_keys_each_deduplicated(self, spark):
        from datetime import datetime, timezone
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
        df = spark.createDataFrame(
            [(1, "old_1", t1), (1, "new_1", t2),
             (2, "old_2", t1), (2, "new_2", t2)],
            SCHEMA,
        )
        deduped = deduplicate_silver(df, "id", "ts")
        assert deduped.count() == 2
        names = {r["name"] for r in deduped.collect()}
        assert names == {"new_1", "new_2"}

    def test_all_cols_preserved(self, spark):
        from datetime import datetime, timezone
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = spark.createDataFrame([(1, "a", t)], SCHEMA)
        deduped = deduplicate_silver(df, "id", "ts")
        assert set(deduped.columns) == {"id", "name", "ts"}


# ---------------------------------------------------------------------------
# apply_silver_transform
# ---------------------------------------------------------------------------

class TestApplySilverTransform:
    def test_drops_metadata_cols(self, spark, cfg):
        from datetime import datetime, timezone
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = spark.createDataFrame([(1, "a", t)], SCHEMA)
        tagged = add_bronze_metadata(df)
        result = apply_silver_transform(tagged, cfg)
        for col in BRONZE_METADATA_COLS:
            assert col not in result.columns

    def test_deduplicates_when_id_and_ts_set(self, spark, cfg):
        from datetime import datetime, timezone
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
        df = spark.createDataFrame(
            [(1, "old", t1), (1, "new", t2)], SCHEMA
        )
        result = apply_silver_transform(df, cfg)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "new"

    def test_transform_fn_applied(self, spark, cfg):
        from datetime import datetime, timezone
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = spark.createDataFrame([(1, "hello", t)], SCHEMA)
        upcase = lambda d: d.withColumn("name", F.upper(F.col("name")))
        result = apply_silver_transform(df, cfg, transform_fn=upcase)
        assert result.collect()[0]["name"] == "HELLO"

    def test_no_dedup_when_id_col_not_set(self, spark):
        from datetime import datetime, timezone
        cfg_no_id = MedallionConfig(
            source_path="s3://b/r",
            bronze_table="b.bronze.t",
            silver_table="b.silver.t",
            gold_table="b.gold.t",
            checkpoint_base="dbfs:/cp",
            pipeline_name="pipe",
        )
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = spark.createDataFrame([(1, "a", t), (1, "b", t)], SCHEMA)
        result = apply_silver_transform(df, cfg_no_id)
        assert result.count() == 2
