"""
streaming/stateful_pyspark.py — Stateful Streaming Operations (PySpark).

Module   : streaming.stateful_pyspark
Concept  : Bounded-state streaming with watermarks: dedup, window aggregations
When     : Your streaming pipeline needs to:
           (a) deduplicate events within an acceptable late-arrival window, or
           (b) compute aggregations (counts, sums, averages) over time windows.

What is stateful streaming?
---------------------------
Stateless streaming (map, filter, select) processes each record independently
with no memory of prior records.  Stateful streaming accumulates state across
records — for example, tracking per-key event counts or deduplicating based on
a seen-IDs set.

The problem: unbounded state.  Without limits, state grows forever (one entry
per unique key ever seen).  Spark addresses this with watermarks.

Watermarks
----------
A watermark tells Spark: "I expect events to arrive no later than X after the
event time they carry."  Data older than (max observed event time − X) is
considered late and dropped from stateful state.

Example: watermark_delay = "10 minutes"
  If the latest event seen has event_ts = 12:00, then:
  - Events with event_ts < 11:50 are late and dropped from aggregation state.
  - Aggregation windows ending before 11:50 are finalized and can be emitted.

Watermark + output mode interaction:
  - 'update': emit updated rows each trigger (may emit a window multiple times).
  - 'append': emit a window only when it's finalized (after watermark passes it).
    Lower latency with 'update'; more correct semantics with 'append'.

Window types
------------
Tumbling window  Non-overlapping, fixed-size.  [0:00–0:10), [0:10–0:20), ...
                 Every event belongs to exactly one window.

Sliding window   Overlapping, fixed-size.  Window every 5 min of size 10 min.
                 [0:00–0:10), [0:05–0:15), ...  An event can fall in 2 windows.

Session window   Variable-size.  Groups events with no gap larger than gap_duration.
                 Useful for user activity sessions.  Requires Spark 3.2+.

Public API
----------
add_watermark(df, event_time_col, delay) -> DataFrame
deduplicate_stream(df, merge_keys, event_time_col, watermark_delay) -> DataFrame
tumbling_window_agg(df, event_time_col, window_duration, agg_exprs, group_cols) -> DataFrame
sliding_window_agg(df, event_time_col, window_duration, slide_duration, agg_exprs, group_cols) -> DataFrame
session_window_agg(df, event_time_col, gap_duration, agg_exprs, group_cols) -> DataFrame
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def add_watermark(
    df: DataFrame,
    event_time_col: str,
    delay: str,
) -> DataFrame:
    """
    Apply an event-time watermark to a streaming DataFrame.

    A watermark must be set before any stateful operation (dedup, aggregation,
    stream-stream join) to bound the state size.  Call this immediately after
    reading the source, before any filtering or transformation.

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame.
    event_time_col : str
        Column carrying the event timestamp.  Must be TimestampType.
    delay : str
        Maximum expected late-arrival delay.  Examples: '10 minutes', '1 hour'.
        Data older than (max event time − delay) is dropped from state.

    Returns
    -------
    DataFrame
        Streaming DataFrame with watermark applied.

    Notes
    -----
    The watermark column becomes the basis for window boundaries and dedup
    expiry.  Choose a delay that covers your P99 late arrival without making
    state unboundedly large.
    """
    return df.withWatermark(event_time_col, delay)


def deduplicate_stream(
    df: DataFrame,
    merge_keys: List[str],
    event_time_col: Optional[str] = None,
    watermark_delay: Optional[str] = None,
) -> DataFrame:
    """
    Deduplicate a streaming DataFrame on merge_keys within a watermark window.

    Uses Spark's stateful dropDuplicates: each unique key is tracked in state.
    The watermark bounds state expiry — keys older than the watermark are evicted
    so state does not grow forever.

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame, possibly with duplicate merge keys.
    merge_keys : list[str]
        Columns that together identify a unique event.
    event_time_col : str | None
        Event timestamp column for watermark.  Must be set for bounded state.
        If None, state is unbounded (use only for finite / bounded sources).
    watermark_delay : str | None
        Watermark delay (e.g., '10 minutes').  Required if event_time_col set.

    Returns
    -------
    DataFrame
        Streaming DataFrame with duplicates removed.

    Notes
    -----
    - dropDuplicates does not guarantee that the FIRST occurrence is kept —
      it keeps whichever arrives first within the watermark window.
    - For ordered deduplication (latest wins), use foreachBatch with
      deduplicate_source() instead.
    - Requires output mode 'append' (state is emitted when finalized by watermark).
    """
    if event_time_col and watermark_delay:
        df = df.withWatermark(event_time_col, watermark_delay)
    return df.dropDuplicates(merge_keys)


def tumbling_window_agg(
    df: DataFrame,
    event_time_col: str,
    window_duration: str,
    agg_exprs: List[Column],
    group_cols: Optional[List[str]] = None,
) -> DataFrame:
    """
    Aggregate events into non-overlapping tumbling time windows.

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame with watermark applied.
    event_time_col : str
        Event timestamp column (same as watermark column).
    window_duration : str
        Window size: '5 minutes', '1 hour', '1 day'.
    agg_exprs : list[Column]
        Aggregation expressions, e.g. [F.count("*").alias("n"), F.sum("amount")].
    group_cols : list[str] | None
        Additional grouping columns beyond the window (e.g., ['device_id']).

    Returns
    -------
    DataFrame
        Aggregated streaming DataFrame.  Write with output_mode='update' or 'append'.

    Examples
    --------
        df_watermarked = add_watermark(df, "event_ts", "10 minutes")
        df_agg = tumbling_window_agg(
            df_watermarked, "event_ts", "5 minutes",
            agg_exprs=[F.count("*").alias("event_count"), F.sum("amount").alias("total")],
            group_cols=["device_id"],
        )
    """
    window_col = F.window(F.col(event_time_col), window_duration)
    keys = ([F.col(c) for c in (group_cols or [])]) + [window_col]
    return df.groupBy(*keys).agg(*agg_exprs)


def sliding_window_agg(
    df: DataFrame,
    event_time_col: str,
    window_duration: str,
    slide_duration: str,
    agg_exprs: List[Column],
    group_cols: Optional[List[str]] = None,
) -> DataFrame:
    """
    Aggregate events into overlapping sliding time windows.

    Each event may fall into multiple windows (one per slide).

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame with watermark applied.
    event_time_col : str
    window_duration : str
        Window size: '10 minutes'.
    slide_duration : str
        Slide interval: '5 minutes'.  Must be <= window_duration.
    agg_exprs : list[Column]
    group_cols : list[str] | None

    Returns
    -------
    DataFrame
        Aggregated streaming DataFrame.
    """
    window_col = F.window(F.col(event_time_col), window_duration, slide_duration)
    keys = ([F.col(c) for c in (group_cols or [])]) + [window_col]
    return df.groupBy(*keys).agg(*agg_exprs)


def session_window_agg(
    df: DataFrame,
    event_time_col: str,
    gap_duration: str,
    agg_exprs: List[Column],
    group_cols: Optional[List[str]] = None,
) -> DataFrame:
    """
    Aggregate events into variable-length session windows.

    A session window groups events with no gap larger than gap_duration between
    consecutive events in the same group.  Each group of events with no long
    idle period forms a single session.

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame with watermark applied.
    event_time_col : str
    gap_duration : str
        Maximum gap between events in the same session: '5 minutes'.
    agg_exprs : list[Column]
    group_cols : list[str] | None
        Session key columns (e.g., ['user_id']).  Sessions are per-group.

    Returns
    -------
    DataFrame
        Aggregated streaming DataFrame.

    Notes
    -----
    Requires Spark 3.2+ / DBR 10.4+.
    """
    session_col = F.session_window(F.col(event_time_col), gap_duration)
    keys = ([F.col(c) for c in (group_cols or [])]) + [session_col]
    return df.groupBy(*keys).agg(*agg_exprs)
