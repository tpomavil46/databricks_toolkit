"""
streaming/readers_pyspark.py — Structured Streaming Source Readers (PySpark).

Module   : streaming.readers_pyspark
Concept  : Read data into a streaming DataFrame from Delta, Auto Loader, or files
When     : The entry point of every streaming pipeline.  Choose the reader based
           on your source type and delivery guarantees.

Source types
------------
Delta Lake (read_delta_stream)
    Reads a Delta table as a stream.  Each micro-batch delivers rows added since
    the last committed batch offset.  Supports maxFilesPerTrigger to cap the
    number of Delta transaction log files read per micro-batch.

    Strengths: native Delta integration, schema evolution, time travel,
    exactly-once delivery with Delta transaction log as the offset store.
    Use when: Bronze Delta → Silver Delta pipelines.

Auto Loader (read_autoloader)
    Reads files landing in cloud storage (S3, ADLS, GCS) as a stream.
    Uses file notification (recommended) or directory listing to discover
    new files.  Handles schema inference and evolution natively.

    Strengths: scales to millions of files, schema inference, format flexibility,
    exactly-once via file tracking in the checkpoint directory.
    Use when: raw files land in a cloud storage landing zone.
    Requires: DBR 8.2+ (file notification mode requires cloud-specific setup).

Generic file stream (read_file_stream)
    Classic Spark Structured Streaming file source.  Simple but scales less well
    than Auto Loader for large landing zones — lists the directory every trigger.
    Use when: small landing zones or non-Databricks environments.

Public API
----------
read_delta_stream(spark, source, config) -> DataFrame
read_autoloader(spark, config) -> DataFrame
read_file_stream(spark, path, format, schema, options) -> DataFrame
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from streaming.config import StreamingConfig


def read_delta_stream(
    spark: SparkSession,
    source: str,
    config: Optional[StreamingConfig] = None,
) -> DataFrame:
    """
    Open a Delta table as a streaming DataFrame.

    Parameters
    ----------
    spark : SparkSession
    source : str
        Delta table name ('catalog.schema.table') or path ('dbfs:/...').
        Paths (containing '/') use .load(); table names use .table().
    config : StreamingConfig | None
        When provided, applies max_files_per_trigger if set.

    Returns
    -------
    DataFrame
        Streaming DataFrame.  Not yet executed — attach a writeStream to run.

    Notes
    -----
    - maxFilesPerTrigger limits how many Delta log files are scanned per batch.
      One log file typically corresponds to one WRITE/MERGE commit.  Setting
      this to 1 gives the smallest possible batches; omit it to read all new
      files each batch (default).
    - ignoreChanges (legacy) / ignoreDeletes: set these options if the source
      table has UPDATE or DELETE operations and you're reading with 'append'
      output mode.  Delta CDF is generally preferred for change tracking.
    """
    reader = spark.readStream.format("delta")

    if config and config.max_files_per_trigger > 0:
        reader = reader.option("maxFilesPerTrigger", config.max_files_per_trigger)

    if "/" in source or source.startswith("dbfs:") or source.startswith("abfss:"):
        return reader.load(source)
    return reader.table(source)


def read_autoloader(
    spark: SparkSession,
    config: StreamingConfig,
) -> DataFrame:
    """
    Read files landing in cloud storage using Databricks Auto Loader.

    Auto Loader automatically discovers new files using file notifications
    (preferred, near-real-time) or directory listing.  Schema inference
    occurs on the first batch and is stored in cloudfiles_schema_location
    so subsequent batches use the inferred schema.

    Parameters
    ----------
    spark : SparkSession
    config : StreamingConfig
        source_path: landing zone directory.
        source_format: file format ('json', 'csv', 'parquet', 'avro', etc.).
        cloudfiles_schema_location: where Auto Loader stores the schema.
            Defaults to checkpoint_path/_schema.
        max_files_per_trigger: cap files per batch (0 = no limit).

    Returns
    -------
    DataFrame
        Streaming DataFrame with inferred or provided schema.

    Notes
    -----
    - Requires: DBR 8.2+ for file notification mode.
    - For CSV/JSON: set cloudFiles.inferColumnTypes = true to get typed
      columns instead of all-string inference.
    - rescuedDataColumn: set to '_rescued_data' to capture malformed records
      rather than failing the batch.
    - Schema evolution: when source schema changes, Auto Loader can add new
      columns automatically (cloudFiles.schemaEvolutionMode = 'addNewColumns').
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", config.source_format)
        .option("cloudFiles.schemaLocation", config.schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
    )

    if config.max_files_per_trigger > 0:
        reader = reader.option("cloudFiles.maxFilesPerTrigger", config.max_files_per_trigger)

    return reader.load(config.source_path)


def read_file_stream(
    spark: SparkSession,
    path: str,
    format: str,
    schema: Optional[StructType] = None,
    options: Optional[dict] = None,
) -> DataFrame:
    """
    Read files from a directory as a streaming DataFrame (generic file source).

    Simpler than Auto Loader but uses directory listing — performance degrades
    on landing zones with millions of files.  Prefer Auto Loader on Databricks.

    Parameters
    ----------
    spark : SparkSession
    path : str
        Directory to watch for new files.
    format : str
        File format: 'json', 'csv', 'parquet', 'avro', 'text', etc.
    schema : StructType | None
        Required for JSON/CSV; optional for self-describing formats like Parquet.
    options : dict | None
        Additional reader options (e.g., {'header': 'true'} for CSV).

    Returns
    -------
    DataFrame
        Streaming DataFrame.

    Notes
    -----
    - The file source treats each file as an atomic unit.  Partial files are
      not processed until the write is complete.
    - latestFirst: set to 'true' to process newest files first (useful for
      backfill where you want recent data prioritized).
    - maxFilesPerTrigger: limit files per batch (same as Delta reader).
    """
    reader = spark.readStream.format(format)

    if schema is not None:
        reader = reader.schema(schema)

    for k, v in (options or {}).items():
        reader = reader.option(k, v)

    return reader.load(path)
