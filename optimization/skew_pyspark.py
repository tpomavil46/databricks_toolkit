"""
optimization/skew_pyspark.py — Skew Detection and Mitigation (PySpark).

Module   : optimization.skew_pyspark
Concept  : Identify and fix data skew — the #1 cause of straggler tasks
When     : A join or aggregation has one task that runs 10× longer than the rest.

What is data skew?
------------------
A skewed dataset has one (or a few) key values that account for a disproportionate
fraction of rows.  Examples: NULL, "unknown", a single viral user_id.

When Spark joins on a skewed key, all rows with that key land on one partition,
creating a "straggler" task.  The job cannot finish until the straggler completes.

Signs of skew
-------------
- Spark UI shows one task running 10–100× longer than the median.
- Most tasks finish in seconds; one takes minutes.
- OOM errors in a specific stage despite the cluster having plenty of memory.

Mitigation strategies
---------------------
1. AQE skew join (automatic, DBR default):
   spark.sql.adaptive.skewJoin.enabled = true (default)
   AQE detects skewed shuffle partitions at runtime and splits them.
   Works without code changes.  Best first option.

2. Broadcast join (broadcast the small side):
   If one side of the join is small (< broadcast threshold), broadcast it.
   Eliminates the shuffle entirely — no skew possible.

3. Salt-based skew join (manual, for AQE failures):
   Artificially distribute the skewed key across N buckets on both sides.
   Each skewed-key bucket becomes an independent task.
   Removes skew at the cost of N× more data read from the right side.

4. Filter-then-union (for known skew values like NULL):
   Process skewed keys and non-skewed keys separately, then UNION.
   Most control; most code.

Public API
----------
detect_skew(df, col, sample_fraction, threshold) -> list[dict]
broadcast_join(df_large, df_small, on, join_type) -> DataFrame
salt_skewed_join(df_left, df_right, join_col, num_buckets, join_type) -> DataFrame
filter_union_join(df_large, df_ref, join_col, skew_values, join_type) -> DataFrame
"""

from __future__ import annotations

import math
from typing import Any, List, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def detect_skew(
    df: DataFrame,
    col: str,
    sample_fraction: float = 0.1,
    threshold: float = 5.0,
) -> List[dict]:
    """
    Identify skewed key values in a column.

    Samples the DataFrame, counts rows per key value, and returns key values
    whose row count exceeds `threshold` × the median key count.

    Parameters
    ----------
    df : DataFrame
    col : str
        Column to analyse.
    sample_fraction : float
        Fraction of rows to sample (0, 1].  Use 0.01–0.1 for large tables.
    threshold : float
        A key is considered skewed when its count > threshold × median count.
        Default 5.0 means keys with 5× the median count are flagged.

    Returns
    -------
    list[dict]
        Each dict: {"value": key_value, "count": int, "skew_factor": float}.
        Empty list when no skewed keys found.
        Sorted by count descending.
    """
    sampled = df.sample(withReplacement=False, fraction=min(sample_fraction, 1.0))
    counts = (
        sampled.groupBy(col)
        .count()
        .orderBy(F.col("count").desc())
    )
    rows = counts.collect()
    if not rows:
        return []

    counts_list = sorted([r["count"] for r in rows])
    n = len(counts_list)
    if n % 2 == 0:
        median = (counts_list[n // 2 - 1] + counts_list[n // 2]) / 2
    else:
        median = counts_list[n // 2]

    if median == 0:
        return []

    return [
        {
            "value": r[col],
            "count": r["count"],
            "skew_factor": round(r["count"] / median, 2),
        }
        for r in rows
        if r["count"] > threshold * median
    ]


def broadcast_join(
    df_large: DataFrame,
    df_small: DataFrame,
    on: Union[str, List[str]],
    join_type: str = "inner",
) -> DataFrame:
    """
    Join two DataFrames, broadcasting the smaller one.

    Eliminates the shuffle on df_small.  Use when df_small fits in executor
    memory (rule of thumb: < 2 GB; verify against spark.sql.autoBroadcastJoinThreshold).

    Parameters
    ----------
    df_large : DataFrame
        The larger DataFrame (not broadcast).
    df_small : DataFrame
        The smaller DataFrame to broadcast to all executors.
    on : str | list[str]
        Join key column(s).
    join_type : str
        Spark join type: 'inner', 'left', 'right', 'full', 'left_semi',
        'left_anti', 'cross'.

    Returns
    -------
    DataFrame
    """
    return df_large.join(F.broadcast(df_small), on=on, how=join_type)


def salt_skewed_join(
    df_left: DataFrame,
    df_right: DataFrame,
    join_col: str,
    num_buckets: int = 10,
    join_type: str = "inner",
) -> DataFrame:
    """
    Perform a join with salt to distribute skewed key values.

    How it works:
      Left side: add a random salt value [0, num_buckets) to each row.
      Right side: explode each row into num_buckets copies, each with a different salt.
      Join on (join_col, _salt) — skewed keys are now spread across num_buckets tasks.
      Drop the _salt column from the result.

    Cost: Right side is read num_buckets times.  Use only when:
      - Broadcast is not possible (right side too large).
      - AQE skew join is not available or not working.
      - num_buckets is small enough that right side × num_buckets fits in cluster.

    Parameters
    ----------
    df_left : DataFrame
    df_right : DataFrame
    join_col : str
        Column to join on (the skewed column).
    num_buckets : int
        Number of salt buckets.  Higher = more even distribution, more right-side reads.
    join_type : str

    Returns
    -------
    DataFrame
        Joined result with _salt column dropped.

    Raises
    ------
    ValueError
        If num_buckets < 2.
    """
    if num_buckets < 2:
        raise ValueError(f"num_buckets must be >= 2, got {num_buckets}")

    left_salted = df_left.withColumn(
        "_salt", (F.rand() * num_buckets).cast("int")
    )

    right_exploded = df_right.withColumn(
        "_salt",
        F.explode(F.array([F.lit(i) for i in range(num_buckets)])),
    )

    return (
        left_salted
        .join(right_exploded, on=[join_col, "_salt"], how=join_type)
        .drop("_salt")
    )


def filter_union_join(
    df_large: DataFrame,
    df_ref: DataFrame,
    join_col: str,
    skew_values: List[Any],
    join_type: str = "inner",
) -> DataFrame:
    """
    Join by splitting skewed key values into a separate broadcast join path.

    Process skewed keys (broadcast join) and non-skewed keys (regular join)
    separately, then UNION ALL the results.  Most effective when:
      - skew_values is a small, known set (NULL, "unknown", top-10 viral IDs).
      - The regular join is well-balanced after excluding skewed keys.

    Parameters
    ----------
    df_large : DataFrame
    df_ref : DataFrame
        Reference table joined to df_large.
    join_col : str
    skew_values : list
        Values of join_col that are skewed.
    join_type : str

    Returns
    -------
    DataFrame
        UNION ALL of the two join paths.

    Raises
    ------
    ValueError
        If skew_values is empty.
    """
    if not skew_values:
        raise ValueError("skew_values must not be empty")

    skew_filter = F.col(join_col).isin(skew_values)

    df_skewed = df_large.filter(skew_filter)
    df_normal = df_large.filter(~skew_filter)

    ref_skewed = df_ref.filter(F.col(join_col).isin(skew_values))
    ref_normal = df_ref.filter(~F.col(join_col).isin(skew_values))

    result_skewed = df_skewed.join(F.broadcast(ref_skewed), on=join_col, how=join_type)
    result_normal = df_normal.join(ref_normal, on=join_col, how=join_type)

    return result_skewed.unionAll(result_normal)
