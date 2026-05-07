"""
ingestion/readers.py — Format-aware batch and streaming readers.

All data reading for the ingestion layer goes through this module.  It
provides two reader families:

Batch readers (read_batch)
--------------------------
Uses spark.read to produce a static DataFrame.  Best for:
- Full-refresh loads where you read the entire source each run.
- Small-to-medium files where latency is acceptable.
- Sources that don't change while the job runs.
- Unit testing (no streaming overhead).

Auto Loader reader (read_stream)
---------------------------------
Uses spark.readStream with format='cloudFiles'.  This is Databricks'
recommended pattern for incremental ingestion from cloud storage.
Best for:
- Large or frequently-updated data lakes where reading everything every
  run is too expensive.
- Near-real-time ingestion pipelines.
- Sources that grow continuously (logs, IoT, clickstream).

Auto Loader advantages over plain readStream:
- Tracks processed files via an internal commit log (no duplicate reads).
- Supports both directory listing (default) and cloud event notifications
  (lower latency, fewer LIST API calls at scale).
- Handles schema inference and evolution: new columns in source files are
  detected and can be added to the target table automatically.
- Provides _rescue_data column to catch rows that don't match the schema
  instead of failing the entire job.

Supported formats
-----------------
csv, parquet, delta, json, avro, orc, text  (both batch and Auto Loader)

Functions
---------
read_batch(spark, source) -> DataFrame
    Read a static source using spark.read.

read_stream(spark, source, schema_location) -> DataFrame (streaming)
    Read a continuously-updating source using Auto Loader.

read_batch_multi(spark, sources) -> dict[str, DataFrame]
    Read several SourceConfigs independently, returning one DataFrame per
    source keyed by label.  Used internally by multiplex ingestion.

read_stream_multi(spark, sources, schema_base) -> dict[str, DataFrame]
    Same as above for streaming.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict

from pyspark.sql import DataFrame, SparkSession

from ingestion.config import SourceConfig

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Batch reading
# ---------------------------------------------------------------------------

def read_batch(spark: SparkSession, source: SourceConfig) -> DataFrame:
    """
    Read a static data source into a batch DataFrame.

    Parameters
    ----------
    spark : SparkSession
    source : SourceConfig
        Describes format, path, options, and optional explicit schema.

    Returns
    -------
    DataFrame
        Static (non-streaming) DataFrame.

    Notes
    -----
    For CSV, common read_options:
        header=true, inferSchema=true, delimiter=",", multiLine=false,
        encoding=UTF-8, nullValue="", escape='"'

    For JSON:
        multiLine=true (for pretty-printed files), primitivesAsString=false

    For Parquet/Delta:
        mergeSchema=true (when schema evolves across files/versions)
    """
    reader = spark.read.format(source.format)

    if source.schema is not None:
        reader = reader.schema(source.schema)

    if source.read_options:
        reader = reader.options(**source.read_options)

    return reader.load(source.path)


def read_batch_multi(
    spark: SparkSession,
    sources: Dict[str, SourceConfig],
) -> Dict[str, DataFrame]:
    """
    Read multiple sources independently.

    Returns a dict of {label: DataFrame}.  Each DataFrame is read with the
    options and schema defined in its SourceConfig; no union is performed
    here — that happens in multiplex.py after normalization.

    Parameters
    ----------
    spark : SparkSession
    sources : dict[str, SourceConfig]
        Keyed by label string (matches MultiplexIngestionConfig.sources).

    Returns
    -------
    dict[str, DataFrame]
        One static DataFrame per source, keyed by label.
    """
    return {label: read_batch(spark, src) for label, src in sources.items()}


# ---------------------------------------------------------------------------
# Streaming reading (Auto Loader)
# ---------------------------------------------------------------------------

def read_stream(
    spark: SparkSession,
    source: SourceConfig,
    schema_location: str,
) -> DataFrame:
    """
    Read a cloud storage path as a streaming DataFrame using Auto Loader.

    Auto Loader (format='cloudFiles') is the Databricks-recommended way to
    ingest data incrementally.  It tracks which files have been processed
    and handles schema inference/evolution automatically.

    Parameters
    ----------
    spark : SparkSession
    source : SourceConfig
        path must be a directory (Auto Loader monitors the whole directory).
        format specifies the underlying file format (csv, parquet, json, …).
        read_options are passed through as cloudFiles options.
        schema is used as the explicit schema when provided.
    schema_location : str
        Cloud storage path where Auto Loader stores its inferred schema.
        Must persist between runs.  Typically:
            <checkpoint_base>/_schema/<source_label>
        Auto Loader reads this on startup to avoid re-inferring schema from
        every file on each run.

    Returns
    -------
    DataFrame
        Streaming DataFrame (isStreaming=True).  Must be written with
        writeStream, not write.

    Auto Loader options applied
    ---------------------------
    cloudFiles.format           set from source.format
    cloudFiles.schemaLocation   set from schema_location argument
    cloudFiles.inferColumnTypes when no explicit schema: infer non-string types
    cloudFiles.schemaEvolutionMode  addNewColumns (default) — new source
                                columns are added to the target table
    cloudFiles.useNotifications set to 'true' for cloud-event-driven
                                file discovery (lower latency, recommended
                                for high-volume sources)

    All additional source.read_options are passed through as-is, allowing
    format-specific settings like header, delimiter, multiLine, etc.
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", source.format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )

    if source.schema is None:
        # Let Auto Loader infer non-string primitive types (dates, numbers)
        reader = reader.option("cloudFiles.inferColumnTypes", "true")
    else:
        reader = reader.schema(source.schema)

    if source.read_options:
        reader = reader.options(**source.read_options)

    return reader.load(source.path)


def read_stream_multi(
    spark: SparkSession,
    sources: Dict[str, SourceConfig],
    schema_base: str,
) -> Dict[str, DataFrame]:
    """
    Read multiple sources as independent streaming DataFrames.

    Each source gets its own schema location subdirectory under schema_base
    so their schemas don't interfere with each other.

    Parameters
    ----------
    spark : SparkSession
    sources : dict[str, SourceConfig]
        Keyed by label string.
    schema_base : str
        Base path for schema locations.  Each source's schema is stored at
        <schema_base>/<label>.

    Returns
    -------
    dict[str, DataFrame]
        One streaming DataFrame per source, keyed by label.
    """
    return {
        label: read_stream(
            spark,
            src,
            schema_location=f"{schema_base.rstrip('/')}/{label}",
        )
        for label, src in sources.items()
    }
