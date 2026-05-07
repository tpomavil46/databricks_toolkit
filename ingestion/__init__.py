"""
ingestion — Databricks Bronze ingestion library.

A parameterized, callable library for single-source and multi-source
ingestion into Bronze Delta tables.  All behaviour is controlled by config
objects — no hardcoded paths, table names, cluster IDs, or credentials
anywhere in the library code.

Quick reference
---------------

Single-source batch ingestion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    from ingestion import SingleplexIngestionConfig, SourceConfig, BronzeWriteConfig
    from ingestion import ingest

    config = SingleplexIngestionConfig(
        source=SourceConfig(path="...", format="csv"),
        target=BronzeWriteConfig(table_name="catalog.bronze.my_table"),
        pipeline_name="my_pipeline",
        env="prod",
    )
    ingest(spark, config)

Single-source streaming ingestion (Auto Loader)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    from ingestion import ingest_stream

    query = ingest_stream(spark, config, trigger_once=True, await_termination=True)

Multi-source batch ingestion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    from ingestion import MultiplexIngestionConfig
    from ingestion import ingest_multi

    config = MultiplexIngestionConfig(
        sources={"yellow": SourceConfig(...), "green": SourceConfig(...)},
        target=BronzeWriteConfig(table_name="catalog.bronze.trips"),
        pipeline_name="taxi_bronze",
        env="prod",
    )
    ingest_multi(spark, config)

Multi-source streaming ingestion (Auto Loader)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    from ingestion import ingest_multi_stream

    queries = ingest_multi_stream(spark, config, trigger_once=True, await_termination=True)

Config from environment variables (Databricks Jobs)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    from ingestion import SingleplexIngestionConfig
    config = SingleplexIngestionConfig.from_env()
    ingest(spark, config)

Config from dict (notebook widgets / JSON task params)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    from ingestion import SingleplexIngestionConfig
    config = SingleplexIngestionConfig.from_dict(dbutils.widgets.getAll())
    ingest(spark, config)

Module overview
---------------
config.py       Config dataclasses with validation and from_dict/from_env.
readers.py      Format-aware batch + Auto Loader streaming readers.
writers.py      Delta batch (overwrite/append/merge) + streaming writers.
metadata.py     Standard Bronze lineage columns (_bronze_ingested_at, etc.).
normalizer.py   Column renaming and snake_case normalization.
singleplex.py   Single-source ingestion (batch + stream).
multiplex.py    Multi-source ingestion with union (batch + stream).
"""

# Config
from ingestion.config import (
    SourceConfig,
    BronzeWriteConfig,
    SingleplexIngestionConfig,
    MultiplexIngestionConfig,
)

# Readers (exposed for advanced use — typically called internally)
from ingestion.readers import read_batch, read_stream, read_batch_multi, read_stream_multi

# Writers (exposed for advanced use — typically called internally)
from ingestion.writers import write_batch, write_stream

# Metadata helpers
from ingestion.metadata import add_bronze_columns, strip_bronze_columns, bronze_column_names

# Normalizer
from ingestion.normalizer import apply_column_mapping, to_snake_case, normalize

# Primary entry points — singleplex
from ingestion.singleplex import ingest, ingest_stream

# Primary entry points — multiplex
from ingestion.multiplex import ingest as ingest_multi
from ingestion.multiplex import ingest_stream as ingest_multi_stream

__all__ = [
    # Config
    "SourceConfig",
    "BronzeWriteConfig",
    "SingleplexIngestionConfig",
    "MultiplexIngestionConfig",
    # Readers
    "read_batch",
    "read_stream",
    "read_batch_multi",
    "read_stream_multi",
    # Writers
    "write_batch",
    "write_stream",
    # Metadata
    "add_bronze_columns",
    "strip_bronze_columns",
    "bronze_column_names",
    # Normalizer
    "apply_column_mapping",
    "to_snake_case",
    "normalize",
    # Singleplex
    "ingest",
    "ingest_stream",
    # Multiplex
    "ingest_multi",
    "ingest_multi_stream",
]
