"""
ingestion/multiplex.py — Multi-source Bronze ingestion.

Use multiplex when two or more sources represent the same logical entity
but arrive via separate files, APIs, or feeds.  The pipeline normalizes
each source independently, tags rows with their origin, then unions
everything into one Bronze table.

Classic examples
----------------
- NYC yellow taxi + green taxi: both describe trips but have different
  column names.  A shared mapping dict per source renames them to a
  canonical set before the union.
- Partner A orders (CSV) + Partner B orders (JSON): different schemas,
  same business object.  Per-source column_mapping handles renaming;
  allowMissingColumns=True handles extra fields from each partner.
- IoT device type X + device type Y: different sensor columns; a 'source'
  column lets analysts filter by device family.

Why not just call singleplex twice?
-------------------------------------
You could load each source into its own Bronze table, but that forces
downstream Silver and Gold logic to join or union them manually on every
query.  Multiplex Bronze gives you a single source of truth for the entity,
already normalized and tagged, with one query surface for Silver.

Discriminator column
--------------------
Each row gets a 'source' column (name configurable via
MultiplexIngestionConfig.source_column_name) set to the label key from
MultiplexIngestionConfig.sources.  This lets consumers filter:
    WHERE source = 'yellow'
or compute metrics per-source:
    GROUP BY source

Union strategy
--------------
unionByName(allowMissingColumns=True) is used instead of union() or a
schema-unifying SELECT.  This means:
- Columns that exist in source A but not source B arrive as null in source B.
- Adding a new source later only requires adding its SourceConfig; no
  schema migration of the existing table is needed.
- Column order doesn't matter — union is by name, not position.

Batch vs streaming
------------------
ingest()        — Read all sources as batch DataFrames, normalize, union, write.
ingest_stream() — Read all sources as Auto Loader streams, normalize, union
                  via foreachBatch, write.  Each source gets its own
                  checkpoint subdirectory so they progress independently.

Public API
----------
ingest(spark, config)               -> None
ingest_stream(spark, config, ...)   -> list[StreamingQuery]
"""

from __future__ import annotations

from functools import reduce
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ingestion.config import MultiplexIngestionConfig, SourceConfig, BronzeWriteConfig
from ingestion.normalizer import normalize
from ingestion.readers import read_batch, read_stream
from ingestion.writers import write_batch, write_stream


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_and_tag(
    df: DataFrame,
    source: SourceConfig,
    source_column_name: str,
) -> DataFrame:
    """
    Apply column normalization and add the source discriminator column.

    Order matters:
    1. Rename columns per source.column_mapping so the discriminator is
       added after renaming (consistent column order in the output).
    2. Add the source tag as a literal column.
    """
    if source.column_mapping:
        df = normalize(df, mapping=source.column_mapping, snake_case_fallback=False)
    return df.withColumn(source_column_name, F.lit(source.label))


def _union_all(dfs: List[DataFrame]) -> DataFrame:
    """Union a list of DataFrames by name, tolerating missing columns."""
    if len(dfs) == 1:
        return dfs[0]
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


# ---------------------------------------------------------------------------
# Batch ingestion
# ---------------------------------------------------------------------------

def ingest(
    spark: SparkSession,
    config: MultiplexIngestionConfig,
) -> None:
    """
    Run a full batch multiplex ingestion: read N sources → normalize each →
    tag with source label → union → write to one Bronze table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : MultiplexIngestionConfig
        Describes all sources and the single target table.

    Returns
    -------
    None

    Examples
    --------
    NYC taxi multiplex Bronze:

        from ingestion.config import (
            MultiplexIngestionConfig, SourceConfig, BronzeWriteConfig,
        )
        from ingestion.multiplex import ingest

        YELLOW_MAP = {
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "pickup_at",
            "tpep_dropoff_datetime": "dropoff_at",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "fare_amount": "fare_amount",
            "tip_amount": "tip_amount",
            "total_amount": "total_amount",
        }

        GREEN_MAP = {
            "VendorID": "vendor_id",
            "lpep_pickup_datetime": "pickup_at",
            "lpep_dropoff_datetime": "dropoff_at",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "fare_amount": "fare_amount",
            "tip_amount": "tip_amount",
            "total_amount": "total_amount",
        }

        config = MultiplexIngestionConfig(
            sources={
                "yellow": SourceConfig(
                    path="dbfs:/mnt/raw/nyc_taxi/yellow/",
                    format="csv",
                    read_options={"header": "true", "inferSchema": "true"},
                    column_mapping=YELLOW_MAP,
                ),
                "green": SourceConfig(
                    path="dbfs:/mnt/raw/nyc_taxi/green/",
                    format="csv",
                    read_options={"header": "true", "inferSchema": "true"},
                    column_mapping=GREEN_MAP,
                ),
            },
            target=BronzeWriteConfig(
                table_name="catalog.bronze.nyc_trips",
                write_mode="overwrite",
                partition_by=["source"],
            ),
            source_column_name="source",
            pipeline_name="nyc_taxi_bronze",
            env="prod",
        )

        ingest(spark, config)

    Multiple partners with different schemas:

        config = MultiplexIngestionConfig(
            sources={
                "partner_a": SourceConfig(
                    path="s3://raw/partner_a/orders/",
                    format="csv",
                    read_options={"header": "true"},
                    column_mapping={"ord_id": "order_id", "cust": "customer_id"},
                ),
                "partner_b": SourceConfig(
                    path="s3://raw/partner_b/orders/",
                    format="json",
                    column_mapping={"orderId": "order_id", "customerId": "customer_id"},
                ),
            },
            target=BronzeWriteConfig(
                table_name="catalog.bronze.partner_orders",
                write_mode="overwrite",
            ),
            pipeline_name="partner_orders_bronze",
            env="prod",
        )

        ingest(spark, config)
    """
    normalized_dfs = []
    for label, source in config.sources.items():
        df = read_batch(spark, source)
        df = _normalize_and_tag(df, source, config.source_column_name)
        normalized_dfs.append(df)

    unified = _union_all(normalized_dfs)

    write_batch(
        unified,
        config.target,
        pipeline_name=config.pipeline_name,
        env=config.env,
    )


# ---------------------------------------------------------------------------
# Streaming ingestion
# ---------------------------------------------------------------------------

def ingest_stream(
    spark: SparkSession,
    config: MultiplexIngestionConfig,
    schema_base: str = "",
    trigger_once: bool = True,
    await_termination: bool = False,
) -> List[StreamingQuery]:
    """
    Run multiplex ingestion using Auto Loader streams.

    Each source is read as an independent Auto Loader stream.  All streams
    write to the same target Delta table via separate writeStream queries
    (one per source).  Rows are tagged with their source label before
    writing, and union happens implicitly because all streams append to the
    same table.

    Note: using separate streams (rather than unioning streaming DataFrames)
    is the recommended Databricks pattern for multiplex Auto Loader because:
    - Each stream has its own checkpoint and progresses independently.
    - One source being slow or failing doesn't block the others.
    - Adding a new source doesn't require restarting existing streams.

    Parameters
    ----------
    spark : SparkSession
    config : MultiplexIngestionConfig
        config.target.checkpoint_path is used as the checkpoint base; each
        source gets a subdirectory: <checkpoint_path>/<label>.
    schema_base : str
        Base path for Auto Loader schema storage.  Defaults to
        <checkpoint_path>/_schema.
    trigger_once : bool
        True: process available data and stop (scheduled batch behaviour).
        False: run continuously.
    await_termination : bool
        Block until all streams finish (only meaningful with trigger_once=True).

    Returns
    -------
    list[StreamingQuery]
        One query per source.  To wait for all: call q.awaitTermination()
        on each, or use await_termination=True.

    Examples
    --------
        queries = ingest_stream(
            spark, config, trigger_once=True, await_termination=True
        )
        # All streams have completed at this point.

        # Or manage them yourself:
        queries = ingest_stream(spark, config, trigger_once=False)
        for q in queries:
            q.awaitTermination()
    """
    if not config.target.checkpoint_path:
        raise ValueError(
            "MultiplexIngestionConfig.target.checkpoint_path is required for "
            "streaming writes.  Set it to a persistent cloud storage path."
        )

    resolved_schema_base = schema_base or (
        config.target.checkpoint_path.rstrip("/") + "/_schema"
    )

    queries: List[StreamingQuery] = []

    for label, source in config.sources.items():
        schema_location = f"{resolved_schema_base}/{label}"

        df = read_stream(spark, source, schema_location=schema_location)
        df = _normalize_and_tag(df, source, config.source_column_name)

        # Give each source its own checkpoint subdirectory
        per_source_target = BronzeWriteConfig(
            table_name=config.target.table_name,
            write_mode="append",  # streaming multiplex always appends
            partition_by=config.target.partition_by,
            checkpoint_path=f"{config.target.checkpoint_path.rstrip('/')}/{label}",
            add_metadata=config.target.add_metadata,
        )

        query = write_stream(
            df,
            per_source_target,
            pipeline_name=f"{config.pipeline_name}__{label}",
            env=config.env,
            trigger_once=trigger_once,
            await_termination=False,  # handle termination below
        )
        queries.append(query)

    if trigger_once and await_termination:
        for q in queries:
            q.awaitTermination()

    return queries
