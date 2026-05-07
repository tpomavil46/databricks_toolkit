"""
data_quality/config.py — DQ Configuration and Result Types.

Classes
-------
DQConfig
    Pipeline-level settings: which table, which layer, where to log results,
    whether to fail the pipeline on DQ errors.

DQResult
    Output of a single DQ check: pass/fail, counts, and a human-readable detail.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

VALID_LAYERS = {"bronze", "silver", "gold", "raw"}


@dataclass
class DQConfig:
    """
    Configuration for a data quality check run.

    Parameters
    ----------
    table_name : str
        Table being validated (for logging purposes).
    layer : str
        Medallion layer: 'bronze', 'silver', 'gold', or 'raw'.
    dq_log_table : str
        Fully-qualified Delta table to write DQ results to.
        Schema: see logging_pyspark.build_dq_log_schema().
        Empty = skip logging.
    fail_on_error : bool
        When True, assert_dq_results() raises if any check fails.
        When False, failures are logged but the pipeline continues.
        Default: False (quarantine pattern — always continue).
    quarantine_table : str
        Delta table for quarantined (failing) records.
        Empty = no quarantine write (caller handles bad records).
    pipeline_name : str
        Human-readable label for this pipeline run (appears in the DQ log).
    env : str
        Deployment environment ('dev', 'staging', 'prod').
    """

    table_name: str
    layer: str
    dq_log_table: str = ""
    fail_on_error: bool = False
    quarantine_table: str = ""
    pipeline_name: str = ""
    env: str = "dev"

    def __post_init__(self) -> None:
        if not self.table_name:
            raise ValueError("DQConfig.table_name must not be empty")
        if self.layer not in VALID_LAYERS:
            raise ValueError(
                f"DQConfig.layer must be one of {VALID_LAYERS}, got '{self.layer}'"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DQConfig":
        return cls(
            table_name=d["table_name"],
            layer=d["layer"],
            dq_log_table=d.get("dq_log_table", ""),
            fail_on_error=d.get("fail_on_error", False),
            quarantine_table=d.get("quarantine_table", ""),
            pipeline_name=d.get("pipeline_name", ""),
            env=d.get("env", "dev"),
        )


@dataclass
class DQResult:
    """
    Result of a single data quality check.

    Parameters
    ----------
    check_name : str
        Identifier for the check type: 'not_null', 'unique', 'value_range', etc.
    column : str
        Column the check was run on.  Empty for table-level checks (row_count).
    passed : bool
        True if the check found no violations.
    failing_count : int
        Number of rows (or values) that violated the check.
    total_count : int
        Total rows evaluated.
    details : str
        Human-readable failure summary.  Empty when passed.
    """

    check_name: str
    column: str
    passed: bool
    failing_count: int
    total_count: int
    details: str = ""

    @property
    def failure_rate(self) -> float:
        """Fraction of rows that failed (0.0–1.0)."""
        return self.failing_count / self.total_count if self.total_count > 0 else 0.0

    @property
    def pass_rate(self) -> float:
        """Fraction of rows that passed (0.0–1.0)."""
        return 1.0 - self.failure_rate

    def to_dict(
        self,
        config: Optional[DQConfig] = None,
        run_ts: Optional[datetime] = None,
    ) -> dict:
        """
        Serialize to a dict suitable for Delta logging.

        Parameters
        ----------
        config : DQConfig | None
            When provided, adds table_name, layer, pipeline_name, env.
        run_ts : datetime | None
            Timestamp of the check run.  Defaults to now (UTC).

        Returns
        -------
        dict
        """
        ts = run_ts or datetime.now(timezone.utc)
        row: dict = {
            "run_ts": ts,
            "check_name": self.check_name,
            "column_name": self.column,
            "passed": self.passed,
            "failing_count": self.failing_count,
            "total_count": self.total_count,
            "failure_rate": self.failure_rate,
            "details": self.details,
        }
        if config:
            row.update({
                "table_name": config.table_name,
                "layer": config.layer,
                "pipeline_name": config.pipeline_name,
                "env": config.env,
            })
        return row


def assert_dq_results(results: List[DQResult], config: DQConfig) -> None:
    """
    Raise ValueError if any check failed and config.fail_on_error is True.

    Parameters
    ----------
    results : list[DQResult]
    config : DQConfig

    Raises
    ------
    ValueError
        Lists all failed check names.
    """
    if not config.fail_on_error:
        return
    failed = [r for r in results if not r.passed]
    if failed:
        summary = ", ".join(
            f"{r.check_name}({r.column or 'table'}): {r.failing_count}/{r.total_count} failing"
            for r in failed
        )
        raise ValueError(f"DQ checks failed for {config.table_name}: {summary}")
