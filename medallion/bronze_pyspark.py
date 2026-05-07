"""
medallion/bronze_pyspark.py — Bronze Layer (PySpark).

Module   : medallion.bronze_pyspark
Concept  : Raw ingestion layer — accept data as-is, add traceability metadata
When     : First hop in any medallion pipeline.  Run once per source file/batch.

What it is
----------
Bronze is the "landing zone" in Delta.  The rule is: NEVER transform the payload.
Keep every source field exactly as received.  Add three metadata columns so every
row is traceable back to its origin:

    _ingest_ts    TIMESTAMP   When this row was written to Bronze (UTC).
    _batch_id     BIGINT      Structured Streaming batch ID (0 for batch jobs).
    _source_file  STRING      Source file path (from input_file_name() or AutoLoader).

Bronze should accumulate history.  Use APPEND mode, never OVERWRITE, so that
source re-delivery can be identified (duplicate _source_file) without losing data.

Why not transform in Bronze?
----------------------------
- Schema drift: if the source changes shape, you want the raw original preserved.
- Debugging: transformations fail; you need the pre-transform record to reproduce.
- Re-processing: Silver/Gold can be rebuilt from Bronze at any time.

Public API
----------
add_bronze_metadata(df, batch_id) -> DataFrame
build_bronze_autoloader_stream(spark, config) -> DataFrame
build_bronze_file_stream(spark, config) -> DataFrame
make_bronze_batch_fn(config) -> Callable
write_bronze_batch(spark, df, config) -> None
"""

from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion.config import MedallionConfig


def add_bronze_metadata(
    df: DataFrame,
    batch_id: int = 0,
) -> DataFrame:
    """
    Append traceability metadata columns to a raw DataFrame.

    Does not modify any existing columns.  The three added columns follow the
    BRONZE_METADATA_COLS convention used by drop_bronze_metadata() in silver.

    Parameters
    ----------
    df : DataFrame
        Raw source DataFrame (payload unchanged).
    batch_id : int
        Streaming batch ID.  Pass 0 for batch (non-streaming) jobs.

    Returns
    -------
    DataFrame
        Original columns + _ingest_ts, _batch_id, _source_file.
    """
    return (
        df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id).cast("long"))
        .withColumn(
            "_source_file",
            F.coalesce(F.input_file_name(), F.lit("")),
        )
    )


def build_bronze_autoloader_stream(
    spark: SparkSession,
    config: MedallionConfig,
) -> DataFrame:
    """
    Build an AutoLoader streaming DataFrame for Bronze ingestion.

    Uses cloudFiles format with schema inference and evolution enabled.
    Schema is persisted at config.schema_location to avoid re-inference
    on restart.

    Parameters
    ----------
    spark : SparkSession
    config : MedallionConfig

    Returns
    -------
    DataFrame
        Streaming DataFrame from cloudFiles.  Not yet started — call
        write_stream_to_delta() or make_bronze_batch_fn() to trigger.
    """
    source_format = config.source_format if config.source_format != "cloudFiles" else "json"
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", config.schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(config.source_path)
    )


def build_bronze_file_stream(
    spark: SparkSession,
    config: MedallionConfig,
) -> DataFrame:
    """
    Build a file-based streaming DataFrame for Bronze ingestion (non-AutoLoader).

    Use this when AutoLoader is not available or when reading Delta/Parquet streams
    from a known schema.

    Parameters
    ----------
    spark : SparkSession
    config : MedallionConfig

    Returns
    -------
    DataFrame
        Streaming DataFrame.
    """
    reader = spark.readStream.format(config.source_format)
    if config.source_format in ("json", "csv"):
        reader = reader.option("inferSchema", "true")
    return reader.load(config.source_path)


def make_bronze_batch_fn(
    config: MedallionConfig,
) -> Callable[[DataFrame, int], None]:
    """
    Return a foreachBatch function that tags and appends each micro-batch to Bronze.

    The returned function is passed to writeStream.foreachBatch().  Metadata
    columns are added inside the batch fn so batch_id is captured correctly.

    Parameters
    ----------
    config : MedallionConfig

    Returns
    -------
    Callable[[DataFrame, int], None]
    """
    def _fn(df_batch: DataFrame, batch_id: int) -> None:
        tagged = add_bronze_metadata(df_batch, batch_id)
        (
            tagged.write
            .format("delta")
            .mode("append")
            .saveAsTable(config.bronze_table)
        )

    return _fn


def write_bronze_batch(
    spark: SparkSession,
    df: DataFrame,
    config: MedallionConfig,
) -> None:
    """
    Tag and append a static DataFrame to the Bronze Delta table.

    Use for batch (non-streaming) ingestion: reading a file, processing a date
    partition, or replaying historic data.

    Parameters
    ----------
    spark : SparkSession
    df : DataFrame
        Raw payload DataFrame (schema as-received from source).
    config : MedallionConfig

    Returns
    -------
    None
    """
    tagged = add_bronze_metadata(df)
    (
        tagged.write
        .format("delta")
        .mode("append")
        .saveAsTable(config.bronze_table)
    )
