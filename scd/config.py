"""
scd/config.py — Configuration for Slowly Changing Dimension operations.

All SCD operations are driven by an SCDConfig object.  No table names,
column names, or environment-specific values are hardcoded inside the SCD
library — everything flows in from the caller (notebook, job, test, CLI).

Classes
-------
SCDConfig
    Describes the source table, target table, business key columns, columns
    to track for changes, and naming conventions for SCD metadata columns
    (effective dates, is_current flag, surrogate key).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class SCDConfig:
    """
    Configuration for a SCD operation.

    Parameters
    ----------
    source_table : str
        Fully-qualified source table or temp view name:
        'catalog.silver.customers_incoming' or 'v_customer_updates'.
    target_table : str
        Fully-qualified Delta target table: 'catalog.gold.dim_customer'.
    business_keys : list[str]
        Column(s) that uniquely identify a business entity.  These are the
        join keys for MERGE INTO.  Can be composite:
        ['order_id', 'line_item_id'].
    tracked_columns : list[str]
        Columns to monitor for changes.  When empty, all non-key columns
        in the source are tracked.  Narrowing this list improves MERGE
        performance and reduces false-positive change detection.
    effective_start_col : str
        Column name for when this row version became active.
        Relevant for Types 2, 4, 6.  Default: 'effective_start'.
    effective_end_col : str
        Column name for when this row version was superseded.
        NULL means the row is currently active.  Default: 'effective_end'.
    is_current_col : str
        Boolean flag column: True = active row for this entity.
        Default: 'is_current'.
    surrogate_key_col : str
        Name of the generated surrogate key column.  Set to '' to skip
        surrogate key generation.  Default: 'scd_key'.
    history_table : str
        Fully-qualified history table name for Type 4.  Empty for all other
        types.  Must be an existing Delta table.
    prev_value_columns : list[str]
        Columns for which to add a 'prev_<col>' column (Types 3 and 6).
        When empty and tracked_columns is set, all tracked_columns get a
        prev_ column.  When both are empty, all non-key source columns get
        a prev_ column.
    env : str
        Deployment environment ('dev', 'staging', 'prod').  Used for logging
        and lineage metadata; does not affect SCD logic.
    pipeline_name : str
        Human-readable identifier for the pipeline run.  Stamped into
        lineage metadata and logs.
    """

    source_table: str
    target_table: str
    business_keys: List[str]
    tracked_columns: List[str] = field(default_factory=list)
    effective_start_col: str = "effective_start"
    effective_end_col: str = "effective_end"
    is_current_col: str = "is_current"
    surrogate_key_col: str = "scd_key"
    history_table: str = ""
    prev_value_columns: List[str] = field(default_factory=list)
    env: str = "dev"
    pipeline_name: str = ""

    def __post_init__(self) -> None:
        if not self.source_table:
            raise ValueError("SCDConfig.source_table must not be empty")
        if not self.target_table:
            raise ValueError("SCDConfig.target_table must not be empty")
        if not self.business_keys:
            raise ValueError("SCDConfig.business_keys must not be empty")

    def resolve_tracked(self, source_columns: List[str]) -> List[str]:
        """Return tracked columns, defaulting to all non-key source columns."""
        if self.tracked_columns:
            return self.tracked_columns
        return [c for c in source_columns if c not in self.business_keys]

    def resolve_prev_columns(self, source_columns: List[str]) -> List[str]:
        """Return columns for prev_ tracking, defaulting to tracked columns."""
        if self.prev_value_columns:
            return self.prev_value_columns
        return self.resolve_tracked(source_columns)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SCDConfig":
        return cls(
            source_table=d["source_table"],
            target_table=d["target_table"],
            business_keys=d["business_keys"],
            tracked_columns=d.get("tracked_columns", []),
            effective_start_col=d.get("effective_start_col", "effective_start"),
            effective_end_col=d.get("effective_end_col", "effective_end"),
            is_current_col=d.get("is_current_col", "is_current"),
            surrogate_key_col=d.get("surrogate_key_col", "scd_key"),
            history_table=d.get("history_table", ""),
            prev_value_columns=d.get("prev_value_columns", []),
            env=d.get("env", "dev"),
            pipeline_name=d.get("pipeline_name", ""),
        )
