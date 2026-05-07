"""
optimization/join_sql.py — SQL Join Hints.

Module   : optimization.join_sql
Concept  : Force or suggest a join strategy via SQL hints
When     : AQE picks the wrong join strategy, or you know the data distribution
           better than the query planner.

Join strategies in Spark SQL
-----------------------------
BROADCAST (aka BroadcastHashJoin)
  Spark broadcasts the smaller table to all executors and performs a hash join.
  Fastest when one side fits in memory (<= spark.sql.autoBroadcastJoinThreshold,
  default 10 MB; Databricks defaults vary).
  Use: /*+ BROADCAST(small_table) */

MERGE (SortMergeJoin)
  Both tables are sorted on the join key and merged.  Safe for any size.
  Default when broadcast threshold is exceeded.
  Use: /*+ MERGE(t1, t2) */

SHUFFLE_HASH (ShuffleHashJoin)
  Hash join after shuffling — faster than sort-merge if one side fits in memory
  after shuffle, but data is not pre-sorted.
  Use: /*+ SHUFFLE_HASH(t1) */

SHUFFLE_REPLICATE_NL (BroadcastNestedLoopJoin fallback)
  Replicates one side; used for non-equi joins (theta joins).
  Use: /*+ SHUFFLE_REPLICATE_NL(t1) */

SKEW
  DBR-specific hint that tells AQE a column is skewed.  AQE splits skewed
  partitions automatically when this hint is present.
  Use: /*+ SKEW('table_alias', 'skew_col') */

When to use hints vs AQE
-------------------------
AQE (spark.sql.adaptive.enabled = true, default in Databricks) handles most
join strategy decisions automatically by measuring shuffle partition sizes at
runtime.  Reach for hints only when:
  - AQE repeatedly picks sort-merge on a table you know is small.
  - A broadcast threshold is set conservatively by your org and you can't change it.
  - You need deterministic physical plans in a test or benchmark.

Public API
----------
build_broadcast_hint_sql(select_body, broadcast_alias) -> str
build_merge_hint_sql(select_body, aliases) -> str
build_shuffle_hash_hint_sql(select_body, aliases) -> str
build_skew_hint_sql(select_body, table_alias, skew_col) -> str
build_coalesce_after_broadcast_sql(select_body, broadcast_alias, num_partitions) -> str
"""

from __future__ import annotations

from typing import List, Optional


def build_broadcast_hint_sql(
    select_body: str,
    broadcast_alias: str,
) -> str:
    """
    Wrap a SELECT statement with a BROADCAST join hint.

    The hint instructs Spark to broadcast the table referenced by broadcast_alias.
    Effective only when that table alias appears in the FROM / JOIN clause of
    select_body.

    Parameters
    ----------
    select_body : str
        Full SELECT ... FROM ... JOIN ... WHERE ... SQL (without leading SELECT).
        Example: "a.id, b.name FROM orders a JOIN customers b ON a.cid = b.id"
    broadcast_alias : str
        Table alias to broadcast.

    Returns
    -------
    str

    Examples
    --------
        sql = build_broadcast_hint_sql(
            "a.*, b.country FROM events a JOIN geo b ON a.geo_id = b.id",
            broadcast_alias="b",
        )
    """
    return f"SELECT /*+ BROADCAST({broadcast_alias}) */ {select_body}"


def build_merge_hint_sql(
    select_body: str,
    aliases: List[str],
) -> str:
    """
    Wrap a SELECT with a MERGE (SortMergeJoin) hint.

    Forces sort-merge join even when broadcast threshold would normally trigger
    a broadcast.  Use when the "small" side is not actually small enough to
    broadcast safely, or when join output must be range-partitioned.

    Parameters
    ----------
    select_body : str
    aliases : list[str]
        Table aliases to include in the hint.

    Returns
    -------
    str
    """
    alias_list = ", ".join(aliases)
    return f"SELECT /*+ MERGE({alias_list}) */ {select_body}"


def build_shuffle_hash_hint_sql(
    select_body: str,
    aliases: List[str],
) -> str:
    """
    Wrap a SELECT with a SHUFFLE_HASH hint.

    ShuffleHashJoin builds a hash table from one side after shuffling.  Faster
    than SortMergeJoin when one side is small enough to hash into memory but too
    large to broadcast.

    Parameters
    ----------
    select_body : str
    aliases : list[str]

    Returns
    -------
    str
    """
    alias_list = ", ".join(aliases)
    return f"SELECT /*+ SHUFFLE_HASH({alias_list}) */ {select_body}"


def build_skew_hint_sql(
    select_body: str,
    table_alias: str,
    skew_col: str,
    skew_values: Optional[List[str]] = None,
) -> str:
    """
    Wrap a SELECT with a SKEW hint for AQE skew join optimisation.

    When AQE detects skew (one partition is >> others), it splits the skewed
    partition and processes it in parallel.  The SKEW hint accelerates this
    detection — AQE does not need to observe runtime stats to know it's skewed.

    Parameters
    ----------
    select_body : str
    table_alias : str
        Alias of the skewed table.
    skew_col : str
        Column that is skewed.
    skew_values : list[str] | None
        Optional list of known skew key values.
        Example: ["null", "'unknown'"] — AQE will split these specifically.

    Returns
    -------
    str
    """
    if skew_values:
        vals = ", ".join(str(v) for v in skew_values)
        hint = f"SKEW('{table_alias}', '{skew_col}', ({vals}))"
    else:
        hint = f"SKEW('{table_alias}', '{skew_col}')"
    return f"SELECT /*+ {hint} */ {select_body}"


def build_range_join_hint_sql(
    select_body: str,
    bin_size: int,
) -> str:
    """
    Wrap a SELECT with a RANGE_JOIN hint (Databricks-specific).

    Range joins (non-equi: a.start <= b.ts AND b.ts < a.end) are expensive by
    default.  The RANGE_JOIN hint tells Databricks to use its optimised range-join
    algorithm with the given bin size.

    Parameters
    ----------
    select_body : str
    bin_size : int
        Bin width in the same unit as the range column (e.g., seconds for a
        timestamp range).  Tune to match typical range width for best performance.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If bin_size <= 0.
    """
    if bin_size <= 0:
        raise ValueError(f"bin_size must be positive, got {bin_size}")
    return f"SELECT /*+ RANGE_JOIN(bin_size => {bin_size}) */ {select_body}"
