"""
scd/surrogate_key.py — Deterministic surrogate key generation for SCD tables.

What it is
----------
A surrogate key is a system-generated identifier that uniquely identifies
a row in a dimension table — independent of the business key from the source
system.  It decouples the data warehouse from changes in the operational
system's natural keys (e.g., a business that reuses customer IDs after
deletion).

When to use
-----------
- SCD Type 2: each *version* of an entity needs a unique key, so the
  surrogate is derived from business_keys + effective_start_col.
- SCD Type 4: history table rows need unique row identity.
- SCD Type 6: same as Type 2 for historical rows.
- SCD Types 0, 1, 3: surrogate key is optional; one key per entity is enough,
  so business_keys alone usually suffice.

Implementation
--------------
SHA-256 (via Spark's sha2 function) of pipe-delimited concatenation of the
specified columns.  Null values are replaced with the sentinel '__null__'
before hashing so that NULL is treated consistently (two NULLs in the same
position hash to the same value).

Trade-offs vs alternatives
--------------------------
sha2 (this module)
    Pro: No external dependency, deterministic across runs, string output
    (readable in UI, no collisions at realistic dimension cardinalities).
    Con: Cannot sort/range-scan by business key order (unlike sequences).

xxhash64 (Spark built-in, DBR 8.0+ / Spark 3.0+)
    Pro: Faster than sha2. Returns BIGINT — more storage-efficient.
    Con: Tiny collision probability at very high cardinality; BIGINT keys
    less human-readable during debugging.

Sequence / IDENTITY columns
    Pro: Compact, sortable.
    Con: Requires a stateful sequence; not idempotent — re-running the
    pipeline generates new IDs even for unchanged rows, breaking downstream
    joins to fact tables.

Functions
---------
add_surrogate_key(df, key_cols, output_col) -> DataFrame
    Attaches a SHA-256 surrogate key column to a DataFrame.

build_surrogate_key_expr(key_cols) -> Column
    Returns the Spark Column expression for use inside select() or MERGE.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def build_surrogate_key_expr(key_cols: List[str], output_col: str = "scd_key") -> Column:
    """
    Build a Spark Column expression for a SHA-256 surrogate key.

    Null-safe: NULL values are replaced with '__null__' before hashing so
    that two rows with the same null pattern produce the same key.

    Parameters
    ----------
    key_cols : list[str]
        Column names to include in the hash.  Order matters — include
        effective_start_col for Type 2 to make each version unique.
    output_col : str
        Alias for the resulting column.  Default: 'scd_key'.

    Returns
    -------
    Column
        Spark Column expression: sha2(concat_ws('|', coalesce(col, '__null__'), ...), 256)
    """
    coalesced = [F.coalesce(F.col(c).cast("string"), F.lit("__null__")) for c in key_cols]
    return F.sha2(F.concat_ws("|", *coalesced), 256).alias(output_col)


def add_surrogate_key(
    df: DataFrame,
    key_cols: List[str],
    output_col: str = "scd_key",
) -> DataFrame:
    """
    Attach a deterministic SHA-256 surrogate key column to a DataFrame.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    key_cols : list[str]
        Columns to hash.  For SCD2, pass business_keys + [effective_start_col]
        so each row version gets a distinct key.  For SCD1, pass business_keys
        only (one key per entity).
    output_col : str
        Name of the surrogate key column to add.  Default: 'scd_key'.

    Returns
    -------
    DataFrame
        Input DataFrame with `output_col` prepended.

    Examples
    --------
    SCD Type 1 (one key per entity):

        df = add_surrogate_key(df, key_cols=["customer_id"])

    SCD Type 2 (unique key per version):

        df = add_surrogate_key(
            df,
            key_cols=["customer_id", "effective_start"],
            output_col="scd_key",
        )
    """
    return df.withColumn(output_col, build_surrogate_key_expr(key_cols, output_col))
