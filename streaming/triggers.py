"""
streaming/triggers.py — Structured Streaming Trigger Types.

Module   : streaming.triggers
Concept  : Control when a streaming query processes data
When     : Every writeStream call needs a trigger.  The right choice depends
           on latency requirements, cost, and whether you're in production or
           running a one-shot backfill.

Trigger types
-------------
ProcessingTime ('micro_batch')
    The default.  Spark waits for the specified interval, then processes all
    data that arrived since the last trigger.  Batches run at most once per
    interval; if processing takes longer than the interval, the next batch
    starts immediately.

    When to use: continuous low-latency pipelines (seconds to minutes).
    Cost: Cluster runs continuously — use AvailableNow for batch-style runs.

AvailableNow ('available_now')
    Process all available data in one or more micro-batches, then stop.
    Databricks-preferred replacement for Once; more efficient on large backlogs
    because it can split the work across multiple batches.

    When to use: scheduled batch-style runs (cron-triggered jobs), backfill,
    cost-conscious workloads where you don't need a continuously running stream.
    Requires: Spark 3.3+ / DBR 11.3+.

Once ('once')
    Process all available data in a single micro-batch, then stop.
    Deprecated in Spark 3.3 in favor of AvailableNow.

    When to use: legacy pipelines or situations where exactly one batch is
    required regardless of available data volume.

Continuous ('continuous')
    Experimental low-latency mode: records are processed row-by-row with
    asynchronous checkpointing.  Sub-second latency but limited operator support
    (no aggregations, limited joins).

    When to use: ultra-low-latency row-level transformations.  Not production-
    ready for most use cases.  Requires Kafka or rate source.

Public API
----------
describe_trigger(config) -> str
    Human-readable trigger description.  No Spark import required — safe to
    call in any environment.

build_trigger_kwargs(config) -> dict
    Returns keyword arguments for DataStreamWriter.trigger(**kwargs).
    Avoids importing Trigger class; instead uses the keyword form of .trigger()
    which is available in all Spark 3.x versions.
"""

from __future__ import annotations

from streaming.config import StreamingConfig, VALID_TRIGGER_MODES


def describe_trigger(config: StreamingConfig) -> str:
    """
    Return a human-readable description of the trigger for this config.

    No Spark session or import required.

    Parameters
    ----------
    config : StreamingConfig

    Returns
    -------
    str
    """
    mode = config.trigger_mode
    interval = config.trigger_interval

    if mode == "available_now":
        return "AvailableNow: process all available data in micro-batches then stop (DBR 11.3+)"
    elif mode == "once":
        return "Once: single micro-batch then stop (deprecated — prefer AvailableNow)"
    elif mode == "continuous":
        return f"Continuous: row-by-row processing, checkpoint every {interval} (experimental)"
    else:
        return f"ProcessingTime: micro-batch every {interval}"


def build_trigger_kwargs(config: StreamingConfig) -> dict:
    """
    Return keyword arguments to pass to DataStreamWriter.trigger(**kwargs).

    Uses the keyword argument form of .trigger() to avoid importing the
    pyspark Trigger class — the keyword form is available in Spark 3.0+.

    Parameters
    ----------
    config : StreamingConfig

    Returns
    -------
    dict
        One of:
        {'processingTime': '<interval>'}
        {'availableNow': True}
        {'once': True}
        {'continuous': '<interval>'}

    Examples
    --------
        kwargs = build_trigger_kwargs(config)
        df.writeStream.trigger(**kwargs).start()
    """
    mode = config.trigger_mode

    if mode == "available_now":
        return {"availableNow": True}
    elif mode == "once":
        return {"once": True}
    elif mode == "continuous":
        return {"continuous": config.trigger_interval}
    else:
        return {"processingTime": config.trigger_interval}


def validate_trigger_mode(mode: str) -> None:
    """Raise ValueError if mode is not a valid trigger mode."""
    if mode not in VALID_TRIGGER_MODES:
        raise ValueError(
            f"Invalid trigger_mode '{mode}'. Must be one of: {sorted(VALID_TRIGGER_MODES)}"
        )
