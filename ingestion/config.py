"""
ingestion/config.py — Ingestion configuration dataclasses.

All ingestion behaviour is controlled by config objects, never by hardcoded
values inside pipeline code.  Pass config in from the caller (notebook,
job, CLI, test) so every parameter is overridable without touching library
code.

Classes
-------
SourceConfig
    Describes one input source: where it lives, what format it is, and how
    its columns should be renamed before the data reaches a Bronze table.

BronzeWriteConfig
    Describes the Bronze Delta target: table name, write mode, partitioning,
    and (for streaming) checkpoint location.

SingleplexIngestionConfig
    Combines one SourceConfig with one BronzeWriteConfig.  Use this when you
    have a single, known input (e.g., one daily CSV drop from a vendor).

MultiplexIngestionConfig
    Combines N labeled SourceConfigs with one BronzeWriteConfig.  Use this
    when several sources share the same logical domain (e.g., NYC yellow and
    green taxi files) and need to land in a single unified table with a
    'source' discriminator column.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Supported read formats
# ---------------------------------------------------------------------------

BATCH_FORMATS = {"csv", "parquet", "delta", "json", "avro", "orc", "text"}
STREAMING_FORMATS = BATCH_FORMATS  # Auto Loader wraps all of these


# ---------------------------------------------------------------------------
# SourceConfig
# ---------------------------------------------------------------------------

@dataclass
class SourceConfig:
    """
    Describes a single data source.

    Parameters
    ----------
    path : str
        Cloud storage path (DBFS, ADLS, S3, GCS) or Unity Catalog table path.
        For Auto Loader this is the *directory* to watch.
    format : str
        One of: csv, parquet, delta, json, avro, orc, text.
    label : str
        Human-readable name for this source.  In multiplex ingestion this
        value is written into the 'source' discriminator column so downstream
        queries can filter by origin.
    column_mapping : dict
        Rename map applied after reading: {original_name: target_name}.
        Columns not present in the map are kept as-is unless
        drop_unmapped=True is set on the normalizer call.
    read_options : dict
        Passed verbatim to spark.read.options(**read_options) or
        spark.readStream.options(**read_options).  Use this for format-
        specific settings such as header, delimiter, multiLine, etc.
    schema : StructType | None
        Explicit schema.  When None, Spark infers the schema (batch) or
        Auto Loader manages it (streaming).  Providing an explicit schema
        avoids the extra read pass and is recommended for production.
    """
    path: str
    format: str = "csv"
    label: str = ""
    column_mapping: Dict[str, str] = field(default_factory=dict)
    read_options: Dict[str, Any] = field(default_factory=dict)
    schema: Optional[Any] = None  # pyspark.sql.types.StructType at runtime

    def __post_init__(self) -> None:
        if self.format not in BATCH_FORMATS:
            raise ValueError(
                f"format must be one of {sorted(BATCH_FORMATS)}, got {self.format!r}"
            )
        if not self.path:
            raise ValueError("SourceConfig.path must not be empty")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SourceConfig":
        return cls(
            path=d["path"],
            format=d.get("format", "csv"),
            label=d.get("label", ""),
            column_mapping=d.get("column_mapping", {}),
            read_options=d.get("read_options", {}),
        )


# ---------------------------------------------------------------------------
# BronzeWriteConfig
# ---------------------------------------------------------------------------

@dataclass
class BronzeWriteConfig:
    """
    Describes the Bronze Delta target table.

    Parameters
    ----------
    table_name : str
        Fully-qualified Unity Catalog name: catalog.schema.table, or a
        DBFS path (starts with /dbfs/ or dbfs:/).
    write_mode : str
        'overwrite'  — full refresh; replaces all existing data each run.
                       Good for small tables or when source is authoritative.
        'append'     — adds new rows; use when source delivers only new
                       records (e.g., daily partition drops).
        'merge'      — Delta MERGE INTO; requires merge_keys.  Use for
                       CDC/upsert patterns where source may resend existing
                       rows with updated values.
    partition_by : list[str]
        Column names to partition the Delta table by.  Common choices:
        event_date, region, source.  Empty list = no partitioning.
    checkpoint_path : str
        Required for streaming writes.  Must be a cloud storage path that
        persists between runs so Structured Streaming can track progress.
        Typically: <table_path>/_checkpoints/<pipeline_name>
    merge_keys : list[str]
        Column(s) that uniquely identify a row for MERGE INTO.  Only used
        when write_mode='merge'.
    add_metadata : bool
        When True, the writer calls metadata.add_bronze_columns() before
        writing to attach lineage columns (_bronze_ingested_at, etc.).
    """
    table_name: str
    write_mode: str = "overwrite"
    partition_by: List[str] = field(default_factory=list)
    checkpoint_path: str = ""
    merge_keys: List[str] = field(default_factory=list)
    add_metadata: bool = True

    _VALID_MODES = {"overwrite", "append", "merge"}

    def __post_init__(self) -> None:
        if not self.table_name:
            raise ValueError("BronzeWriteConfig.table_name must not be empty")
        if self.write_mode not in self._VALID_MODES:
            raise ValueError(
                f"write_mode must be one of {self._VALID_MODES}, got {self.write_mode!r}"
            )
        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError("write_mode='merge' requires at least one merge_key")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BronzeWriteConfig":
        return cls(
            table_name=d["table_name"],
            write_mode=d.get("write_mode", "overwrite"),
            partition_by=d.get("partition_by", []),
            checkpoint_path=d.get("checkpoint_path", ""),
            merge_keys=d.get("merge_keys", []),
            add_metadata=d.get("add_metadata", True),
        )


# ---------------------------------------------------------------------------
# SingleplexIngestionConfig
# ---------------------------------------------------------------------------

@dataclass
class SingleplexIngestionConfig:
    """
    Full configuration for single-source Bronze ingestion.

    When to use
    -----------
    Use singleplex when you have exactly one input source writing to one
    Bronze table.  Examples:
    - A daily CSV file uploaded by a single vendor.
    - A Parquet snapshot exported from an OLTP system.
    - A single Kafka topic landing in cloud storage.

    Parameters
    ----------
    source : SourceConfig
        Where to read from and how.
    target : BronzeWriteConfig
        Where to write and with what behaviour.
    pipeline_name : str
        Identifier stamped into _bronze_pipeline metadata column.  Use a
        descriptive name like "orders_bronze_daily" so lineage is clear.
    env : str
        Environment tag ('dev', 'staging', 'prod') stamped into metadata.
    """
    source: SourceConfig
    target: BronzeWriteConfig
    pipeline_name: str = ""
    env: str = "dev"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SingleplexIngestionConfig":
        return cls(
            source=SourceConfig.from_dict(d["source"]),
            target=BronzeWriteConfig.from_dict(d["target"]),
            pipeline_name=d.get("pipeline_name", ""),
            env=d.get("env", "dev"),
        )

    @classmethod
    def from_env(cls) -> "SingleplexIngestionConfig":
        """
        Build config entirely from environment variables.  Useful for
        Databricks Jobs where parameters are passed as ENV vars.

        Required env vars
        -----------------
        INGESTION_SOURCE_PATH
        INGESTION_TARGET_TABLE

        Optional env vars
        -----------------
        INGESTION_SOURCE_FORMAT     (default: csv)
        INGESTION_SOURCE_LABEL      (default: '')
        INGESTION_WRITE_MODE        (default: overwrite)
        INGESTION_CHECKPOINT_PATH   (default: '')
        INGESTION_PIPELINE_NAME     (default: '')
        INGESTION_ENV               (default: dev)
        INGESTION_PARTITION_BY      (comma-separated column names)
        INGESTION_MERGE_KEYS        (comma-separated column names)
        """
        def _list(key: str) -> List[str]:
            raw = os.getenv(key, "")
            return [c.strip() for c in raw.split(",") if c.strip()]

        source = SourceConfig(
            path=os.environ["INGESTION_SOURCE_PATH"],
            format=os.getenv("INGESTION_SOURCE_FORMAT", "csv"),
            label=os.getenv("INGESTION_SOURCE_LABEL", ""),
        )
        target = BronzeWriteConfig(
            table_name=os.environ["INGESTION_TARGET_TABLE"],
            write_mode=os.getenv("INGESTION_WRITE_MODE", "overwrite"),
            partition_by=_list("INGESTION_PARTITION_BY"),
            checkpoint_path=os.getenv("INGESTION_CHECKPOINT_PATH", ""),
            merge_keys=_list("INGESTION_MERGE_KEYS"),
        )
        return cls(
            source=source,
            target=target,
            pipeline_name=os.getenv("INGESTION_PIPELINE_NAME", ""),
            env=os.getenv("INGESTION_ENV", "dev"),
        )


# ---------------------------------------------------------------------------
# MultiplexIngestionConfig
# ---------------------------------------------------------------------------

@dataclass
class MultiplexIngestionConfig:
    """
    Full configuration for multi-source Bronze ingestion.

    When to use
    -----------
    Use multiplex when multiple sources share the same logical entity but
    differ in schema, format, or origin, and you want them unified in one
    Bronze table with a discriminator column.  Examples:
    - NYC yellow taxi + green taxi (same trip data, different column names)
    - Partner A orders CSV + Partner B orders JSON (same domain, two senders)
    - IoT sensor readings from device type X and device type Y

    The pipeline reads each source independently, applies per-source column
    mapping (SourceConfig.column_mapping), adds a 'source' tag, then unions
    all frames with unionByName(allowMissingColumns=True).  Columns that
    exist in only some sources arrive as null in the others.

    Parameters
    ----------
    sources : dict[str, SourceConfig]
        Keyed by label string.  The label is written into the source column
        so downstream can filter with WHERE source = 'yellow'.
        The SourceConfig.label field is overwritten with the dict key.
    target : BronzeWriteConfig
        Single target table that receives the union of all sources.
    source_column_name : str
        Name of the discriminator column added to each row (default: 'source').
    pipeline_name : str
        Stamped into _bronze_pipeline metadata column.
    env : str
        Environment tag stamped into metadata.
    """
    sources: Dict[str, SourceConfig]
    target: BronzeWriteConfig
    source_column_name: str = "source"
    pipeline_name: str = ""
    env: str = "dev"

    def __post_init__(self) -> None:
        if not self.sources:
            raise ValueError("MultiplexIngestionConfig.sources must not be empty")
        # Ensure each SourceConfig.label matches its dict key
        for label, src in self.sources.items():
            src.label = label

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MultiplexIngestionConfig":
        sources = {
            label: SourceConfig.from_dict(src_d)
            for label, src_d in d["sources"].items()
        }
        return cls(
            sources=sources,
            target=BronzeWriteConfig.from_dict(d["target"]),
            source_column_name=d.get("source_column_name", "source"),
            pipeline_name=d.get("pipeline_name", ""),
            env=d.get("env", "dev"),
        )
