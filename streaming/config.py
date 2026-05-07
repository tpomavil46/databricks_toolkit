"""
streaming/config.py — Configuration for Structured Streaming pipelines.

Classes
-------
StreamingConfig
    All parameters for a Structured Streaming job: source, sink, trigger,
    checkpointing, watermark, and merge-key settings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

VALID_TRIGGER_MODES = {"micro_batch", "available_now", "once", "continuous"}
VALID_OUTPUT_MODES = {"append", "update", "complete"}


@dataclass
class StreamingConfig:
    """
    Configuration for a Structured Streaming pipeline.

    Parameters
    ----------
    source_path : str
        Path to the source data.  For Delta: table name or DBFS/Unity path.
        For Auto Loader: the landing zone directory.
    target_table : str
        Fully-qualified Delta target table (catalog.schema.table).
    checkpoint_base : str
        Root directory for checkpoint state.  A subdirectory named
        pipeline_name is created under this path.
        Example: 'dbfs:/checkpoints' → 'dbfs:/checkpoints/my_pipeline'.
    pipeline_name : str
        Human-readable identifier.  Used as the checkpoint subdirectory name
        and for logging.  Must be unique per target table.
    source_format : str
        Spark/Auto Loader source format.  Common values:
        'delta', 'cloudFiles', 'json', 'parquet', 'csv', 'avro'.
        Default: 'delta'.
    trigger_mode : str
        One of: 'micro_batch', 'available_now', 'once', 'continuous'.
        Default: 'micro_batch' (periodic micro-batch).
    trigger_interval : str
        Interval string for ProcessingTime ('30 seconds', '1 minute') or
        Continuous ('1 second') triggers.  Ignored for Once / AvailableNow.
        Default: '30 seconds'.
    output_mode : str
        Streaming output mode: 'append', 'update', or 'complete'.
        Default: 'append'.
        - append : only new rows added since last trigger (stateless / event).
        - update : rows that changed since last trigger (stateful agg).
        - complete: full result table rewritten each trigger (small agg only).
    max_files_per_trigger : int
        Maximum source files processed per micro-batch (Auto Loader / file
        source).  0 = no limit.  Use to cap backfill throughput.
        Default: 0.
    cloudfiles_schema_location : str
        Schema inference/evolution directory for Auto Loader.  If empty,
        defaults to checkpoint_path + '/_schema'.
    watermark_col : str
        Event-time column for stateful operations (aggregations, dedup).
        Required for bounded state; omit for stateless pipelines.
    watermark_delay : str
        Maximum expected event-time delay.  Data older than
        (max observed event time - delay) is dropped from state.
        Default: '10 minutes'.
    merge_keys : list[str]
        Column(s) identifying a row in the target.  Used by foreachBatch
        MERGE factories.  Empty for append-only pipelines.
    env : str
        Deployment environment ('dev', 'staging', 'prod').
    """

    source_path: str
    target_table: str
    checkpoint_base: str
    pipeline_name: str
    source_format: str = "delta"
    trigger_mode: str = "micro_batch"
    trigger_interval: str = "30 seconds"
    output_mode: str = "append"
    max_files_per_trigger: int = 0
    cloudfiles_schema_location: str = ""
    watermark_col: str = ""
    watermark_delay: str = "10 minutes"
    merge_keys: List[str] = field(default_factory=list)
    env: str = "dev"

    def __post_init__(self) -> None:
        if not self.source_path:
            raise ValueError("StreamingConfig.source_path must not be empty")
        if not self.target_table:
            raise ValueError("StreamingConfig.target_table must not be empty")
        if not self.checkpoint_base:
            raise ValueError("StreamingConfig.checkpoint_base must not be empty")
        if not self.pipeline_name:
            raise ValueError("StreamingConfig.pipeline_name must not be empty")
        if self.trigger_mode not in VALID_TRIGGER_MODES:
            raise ValueError(
                f"StreamingConfig.trigger_mode must be one of {VALID_TRIGGER_MODES}, "
                f"got '{self.trigger_mode}'"
            )
        if self.output_mode not in VALID_OUTPUT_MODES:
            raise ValueError(
                f"StreamingConfig.output_mode must be one of {VALID_OUTPUT_MODES}, "
                f"got '{self.output_mode}'"
            )

    @property
    def checkpoint_path(self) -> str:
        """Full checkpoint directory for this pipeline."""
        return f"{self.checkpoint_base.rstrip('/')}/{self.pipeline_name}"

    @property
    def schema_location(self) -> str:
        """Auto Loader schema location, defaulting to checkpoint subdir."""
        return self.cloudfiles_schema_location or f"{self.checkpoint_path}/_schema"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StreamingConfig":
        return cls(
            source_path=d["source_path"],
            target_table=d["target_table"],
            checkpoint_base=d["checkpoint_base"],
            pipeline_name=d["pipeline_name"],
            source_format=d.get("source_format", "delta"),
            trigger_mode=d.get("trigger_mode", "micro_batch"),
            trigger_interval=d.get("trigger_interval", "30 seconds"),
            output_mode=d.get("output_mode", "append"),
            max_files_per_trigger=d.get("max_files_per_trigger", 0),
            cloudfiles_schema_location=d.get("cloudfiles_schema_location", ""),
            watermark_col=d.get("watermark_col", ""),
            watermark_delay=d.get("watermark_delay", "10 minutes"),
            merge_keys=d.get("merge_keys", []),
            env=d.get("env", "dev"),
        )
