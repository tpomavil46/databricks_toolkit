"""
upserts/config.py — Configuration for Delta Lake upsert / MERGE operations.

One UpsertConfig covers all five upsert patterns.  Fields that are specific
to a single pattern (e.g. delete_indicator_col for delete-aware MERGE) are
simply unused when running other patterns.

Classes
-------
UpsertConfig
    All parameters for a MERGE INTO operation: source, target, join keys,
    delete signalling, late-arriving timestamps, content-hash tracking, and
    schema evolution toggle.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class UpsertConfig:
    """
    Configuration for a Delta Lake upsert (MERGE INTO) operation.

    Parameters
    ----------
    source_table : str
        Fully-qualified source table or temp view name.
    target_table : str
        Fully-qualified Delta target table.
    merge_keys : list[str]
        Column(s) used in the MERGE ON condition.  Must uniquely identify a
        row in the target.  Can be composite: ['order_id', 'line_id'].
    update_columns : list[str]
        Columns to set in the WHEN MATCHED UPDATE clause.  When empty, all
        source columns not in merge_keys are updated (equivalent to UPDATE SET *).
        Narrowing this list reduces write amplification on wide tables.
    delete_indicator_col : str
        Column in the source that signals whether a row should be deleted
        from the target.  Empty = no delete path.
        Example: 'op' where op = 'D' means delete.
    delete_indicator_value : str
        The value in delete_indicator_col that triggers the DELETE clause.
        Default: 'true' (works when delete_indicator_col is a boolean-like
        column; set to 'D' for CDC op-code columns).
    event_ts_col : str
        Timestamp column used to establish recency for late-arriving data.
        MERGE fires UPDATE only when s.event_ts_col >= t.event_ts_col.
        Empty = no recency guard applied.
    content_hash_col : str
        Column name for the SHA-256 content hash added by add_content_hash().
        Used in idempotent MERGE: UPDATE only when hash differs.
        Default: '_content_hash'.
    allow_schema_evolution : bool
        When True, sets spark.databricks.delta.schema.autoMerge.enabled=true
        before executing the MERGE.  New columns in the source are
        automatically added to the target.  Default: False.
        Note: requires DBR 10.5+ / Delta Lake 2.0+.
    env : str
        Deployment environment ('dev', 'staging', 'prod').
    pipeline_name : str
        Human-readable identifier for this upsert pipeline.
    """

    source_table: str
    target_table: str
    merge_keys: List[str]
    update_columns: List[str] = field(default_factory=list)
    delete_indicator_col: str = ""
    delete_indicator_value: str = "true"
    event_ts_col: str = ""
    content_hash_col: str = "_content_hash"
    allow_schema_evolution: bool = False
    env: str = "dev"
    pipeline_name: str = ""

    def __post_init__(self) -> None:
        if not self.source_table:
            raise ValueError("UpsertConfig.source_table must not be empty")
        if not self.target_table:
            raise ValueError("UpsertConfig.target_table must not be empty")
        if not self.merge_keys:
            raise ValueError("UpsertConfig.merge_keys must not be empty")

    def resolve_update_columns(self, source_columns: List[str]) -> List[str]:
        """Return update columns, defaulting to all non-key source columns."""
        if self.update_columns:
            return self.update_columns
        return [c for c in source_columns if c not in self.merge_keys]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "UpsertConfig":
        return cls(
            source_table=d["source_table"],
            target_table=d["target_table"],
            merge_keys=d["merge_keys"],
            update_columns=d.get("update_columns", []),
            delete_indicator_col=d.get("delete_indicator_col", ""),
            delete_indicator_value=d.get("delete_indicator_value", "true"),
            event_ts_col=d.get("event_ts_col", ""),
            content_hash_col=d.get("content_hash_col", "_content_hash"),
            allow_schema_evolution=d.get("allow_schema_evolution", False),
            env=d.get("env", "dev"),
            pipeline_name=d.get("pipeline_name", ""),
        )
