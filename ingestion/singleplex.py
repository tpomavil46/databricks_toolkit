"""
ingestion/singleplex.py — Single-source Bronze ingestion.

Use singleplex when your pipeline has exactly one input source writing to
one Bronze Delta table.  This covers the majority of ingestion scenarios:
a daily file drop from a vendor, a CDC snapshot from an OLTP system, an
export from an SaaS API, or a Parquet dump from another data team.

When NOT to use singleplex
--------------------------
If you need to combine data from two or more related sources into a single
table (e.g., yellow + green taxi, Partner A + Partner B), use
multiplex.ingest() instead.  It handles per-source schema normalization,
source tagging, and union with missing-column tolerance.

Batch vs streaming
------------------
ingest()        — Batch.  Use for:
    - Scheduled full-refresh loads (overwrite mode).
    - Daily/hourly file drops appended to a partitioned table (append mode).
    - CDC snapshots merged with MERGE INTO (merge mode).
    - Simpler operational model; no streaming state to manage.

ingest_stream() — Auto Loader streaming.  Use for:
    - Continuous or near-real-time ingestion.
    - Sources that grow incrementally and where reading the full directory
      every run is too expensive (thousands of small files, large volumes).
    - Pipelines that need exactly-once processing guarantees.
    - The trigger_once=True default gives you batch scheduling semantics
      with streaming reliability.

Pipeline steps (both modes)
----------------------------
1. Read source (batch or Auto Loader streaming).
2. Apply column mapping / normalization (if config.source.column_mapping set).
3. Add Bronze metadata columns (if config.target.add_metadata=True).
4. Write to Delta target (overwrite / append / merge for batch;
   append with checkpoint for streaming).

Public API
----------
ingest(spark, config)               -> None
ingest_stream(spark, config, ...)   -> StreamingQuery
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from ingestion.config import SingleplexIngestionConfig
from ingestion.normalizer import normalize
from ingestion.readers import read_batch, read_stream
from ingestion.writers import write_batch, write_stream


def ingest(
    spark: SparkSession,
    config: SingleplexIngestionConfig,
) -> None:
    """
    Run a full batch ingestion: read → normalize → write.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : SingleplexIngestionConfig
        All parameters for source, target, and pipeline identity.

    Returns
    -------
    None
        The function completes when the write is done.

    Examples
    --------
    Overwrite Bronze with a daily vendor CSV:

        from ingestion.config import SingleplexIngestionConfig, SourceConfig, BronzeWriteConfig
        from ingestion.singleplex import ingest

        config = SingleplexIngestionConfig(
            source=SourceConfig(
                path="abfss://raw@storage.dfs.core.windows.net/orders/2024-01-15/",
                format="csv",
                read_options={"header": "true", "inferSchema": "true"},
                column_mapping={"order_id": "id", "order_ts": "created_at"},
            ),
            target=BronzeWriteConfig(
                table_name="catalog.bronze.orders",
                write_mode="overwrite",
                partition_by=["_bronze_env"],
            ),
            pipeline_name="orders_bronze_daily",
            env="prod",
        )

        ingest(spark, config)

    Append-only with explicit schema:

        from pyspark.sql.types import StructType, StructField, StringType, TimestampType

        schema = StructType([
            StructField("event_id", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("payload", StringType()),
        ])

        config = SingleplexIngestionConfig(
            source=SourceConfig(path="s3://bucket/events/", format="json", schema=schema),
            target=BronzeWriteConfig(table_name="catalog.bronze.events", write_mode="append"),
            pipeline_name="events_bronze",
            env="prod",
        )

        ingest(spark, config)
    """
    df = read_batch(spark, config.source)

    if config.source.column_mapping:
        df = normalize(df, mapping=config.source.column_mapping, snake_case_fallback=False)

    write_batch(
        df,
        config.target,
        pipeline_name=config.pipeline_name,
        env=config.env,
    )


def ingest_stream(
    spark: SparkSession,
    config: SingleplexIngestionConfig,
    schema_location: str = "",
    trigger_once: bool = True,
    await_termination: bool = False,
) -> StreamingQuery:
    """
    Run a streaming ingestion using Auto Loader: read → normalize → write.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : SingleplexIngestionConfig
        All parameters for source, target, and pipeline identity.
        config.target.checkpoint_path must be set.
    schema_location : str
        Where Auto Loader stores the inferred schema between runs.  If
        empty, defaults to <checkpoint_path>/_schema.
    trigger_once : bool
        True (default): process all available files then stop.
        False: run continuously.
    await_termination : bool
        Block until the stream finishes (only meaningful when trigger_once=True).

    Returns
    -------
    StreamingQuery
        Handle to the running stream.

    Examples
    --------
    Scheduled incremental load (trigger once, await before returning):

        from ingestion.config import SingleplexIngestionConfig, SourceConfig, BronzeWriteConfig
        from ingestion.singleplex import ingest_stream

        config = SingleplexIngestionConfig(
            source=SourceConfig(
                path="abfss://raw@storage.dfs.core.windows.net/logs/",
                format="json",
            ),
            target=BronzeWriteConfig(
                table_name="catalog.bronze.app_logs",
                write_mode="append",
                checkpoint_path="abfss://checkpoints@storage.dfs.core.windows.net/logs_bronze",
                partition_by=["_bronze_env"],
            ),
            pipeline_name="app_logs_bronze",
            env="prod",
        )

        query = ingest_stream(spark, config, trigger_once=True, await_termination=True)

    Continuous stream (long-running job):

        query = ingest_stream(spark, config, trigger_once=False)
        query.awaitTermination()
    """
    resolved_schema_location = schema_location or (
        config.target.checkpoint_path.rstrip("/") + "/_schema"
    )

    df = read_stream(spark, config.source, schema_location=resolved_schema_location)

    if config.source.column_mapping:
        df = normalize(df, mapping=config.source.column_mapping, snake_case_fallback=False)

    return write_stream(
        df,
        config.target,
        pipeline_name=config.pipeline_name,
        env=config.env,
        trigger_once=trigger_once,
        await_termination=await_termination,
    )
