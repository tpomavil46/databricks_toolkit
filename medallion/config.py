"""
medallion/config.py — Medallion Pipeline Configuration.

Module   : medallion.config
Concept  : Single config object wiring all three layers of a medallion pipeline
When     : Instantiate once per pipeline; pass to bronze/silver/gold functions.

Design
------
MedallionConfig centralises every path, table name, and behavioural flag that
can vary between pipelines or environments.  No hardcoded strings anywhere else
in the medallion package.

Layer checkpoints are derived properties so callers never construct paths by hand.
The convention is:
    <checkpoint_base>/<pipeline_name>/bronze
    <checkpoint_base>/<pipeline_name>/silver

Public API
----------
MedallionConfig  dataclass
BRONZE_METADATA_COLS  list[str]  — columns added by add_bronze_metadata()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

BRONZE_METADATA_COLS = ["_ingest_ts", "_batch_id", "_source_file"]

VALID_SOURCE_FORMATS = {"json", "csv", "parquet", "avro", "orc", "delta", "cloudFiles"}


@dataclass
class MedallionConfig:
    """
    Configuration for a three-layer medallion pipeline.

    Parameters
    ----------
    source_path : str
        Cloud storage path (or table name) for raw source data.
    bronze_table : str
        Fully-qualified Delta table for the Bronze layer.
    silver_table : str
        Fully-qualified Delta table for the Silver layer.
    gold_table : str
        Fully-qualified Delta table for the Gold layer.
    checkpoint_base : str
        Root path for Structured Streaming checkpoints.
        Bronze uses <checkpoint_base>/<pipeline_name>/bronze,
        Silver uses <checkpoint_base>/<pipeline_name>/silver.
    pipeline_name : str
        Unique identifier for this pipeline — used in checkpoint paths,
        watermark tables, and logging.
    source_format : str
        Spark read format for the Bronze source ('json', 'csv', etc.).
        Use 'cloudFiles' for AutoLoader.
    id_col : str
        Primary key column used for Silver deduplication and MERGE.
        Required for SCD or upsert Silver patterns; empty = no dedup.
    ts_col : str
        Timestamp column used to pick the latest record per id_col.
        Required when id_col is set; empty = sort by ingest order.
    partition_col : str
        Column used to partition Bronze and Gold tables.
        Empty = no partitioning (acceptable for small-medium tables).
    watermark_table : str
        Delta table tracking pipeline watermarks for idempotent batch runs.
        Empty = watermark tracking disabled (streaming handles this via checkpoint).
    env : str
        Deployment environment — propagated to audit fields.
    """

    source_path: str
    bronze_table: str
    silver_table: str
    gold_table: str
    checkpoint_base: str
    pipeline_name: str
    source_format: str = "json"
    id_col: str = ""
    ts_col: str = ""
    partition_col: str = ""
    watermark_table: str = ""
    env: str = "dev"

    def __post_init__(self) -> None:
        for attr in ("source_path", "bronze_table", "silver_table", "gold_table",
                     "checkpoint_base", "pipeline_name"):
            if not getattr(self, attr):
                raise ValueError(f"MedallionConfig.{attr} must not be empty")

    @property
    def bronze_checkpoint(self) -> str:
        base = self.checkpoint_base.rstrip("/")
        return f"{base}/{self.pipeline_name}/bronze"

    @property
    def silver_checkpoint(self) -> str:
        base = self.checkpoint_base.rstrip("/")
        return f"{base}/{self.pipeline_name}/silver"

    @property
    def schema_location(self) -> str:
        return f"{self.bronze_checkpoint}/_schema"
