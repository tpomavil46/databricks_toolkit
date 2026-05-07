"""
delta_features/deletion_vectors.py — Delta Deletion Vectors.

Module   : delta_features.deletion_vectors
Concept  : Mark rows as deleted without rewriting Parquet files
When     : Tables with frequent point deletes or updates where rewriting data
           files (the traditional Delta approach) causes write amplification.

What it is
----------
Traditionally, Delta handles DELETE and UPDATE by rewriting the affected Parquet
files with the deleted/changed rows removed.  For a 1 GB file where 10 rows need
to be deleted, Delta rewrites the full 1 GB.

Deletion Vectors (DVs) change this:
  - A DELETE marks affected rows in a small sidecar DV file (a bitmap).
  - The Parquet file is NOT rewritten.
  - Readers merge the DV bitmap with the Parquet file at read time to skip
    deleted rows.
  - DVs accumulate until OPTIMIZE runs, which actually removes deleted rows
    from the files (merging DV data into clean Parquet output).

When to use
-----------
- High-frequency point deletes (GDPR/CCPA right-to-forget requests, CDC deletes).
- UPDATE-heavy workloads where full file rewrites are too expensive.
- Tables with many small deletes spread across large files.

When NOT to use
---------------
- Mostly-append workloads with rare deletes: no benefit, adds read overhead.
- Tables read by older Delta readers that don't support DVs (Delta 2.3 + reader v3).
- Very large MERGE operations that affect most rows in most files: DV overhead
  may exceed the cost of a full file rewrite.

Performance trade-offs
-----------------------
Writes  : Much faster (write a bitmap, not a full file).
Reads   : Slightly slower — readers must merge DV bitmaps into read results.
          The overhead is small for sparse deletions (< 5% of rows), larger for
          heavily deleted files.  Run OPTIMIZE to "clean" DV-marked files.
Storage : DVs are tiny (one bit per row).  Long-lived DVs + OPTIMIZE = storage
          reduction because OPTIMIZE physically removes deleted rows.

Interaction with other features
---------------------------------
- OPTIMIZE respects DVs and removes marked rows during compaction.
- VACUUM removes orphan DV files that are no longer referenced.
- Time travel on DV-enabled tables works correctly — historical snapshots see
  the correct deleted/not-deleted state.
- CDF on DV-enabled tables: deletes emit 'delete' change records (same as
  without DVs — CDF is DV-aware).

Requires: Delta Lake 2.3+ / DBR 12.2+.  Reader/writer protocol version 3.

Public API
----------
build_enable_deletion_vectors_sql(table) -> str
build_disable_deletion_vectors_sql(table) -> str
build_optimize_and_clean_dvs_sql(table, zorder_cols) -> str
"""

from __future__ import annotations

from typing import List, Optional


def build_enable_deletion_vectors_sql(table: str) -> str:
    """
    Generate ALTER TABLE SQL to enable Deletion Vectors.

    Parameters
    ----------
    table : str
        Fully-qualified Delta table name.

    Returns
    -------
    str

    Notes
    -----
    After enabling, existing data files are NOT immediately affected.
    New DELETE and UPDATE operations will use DVs going forward.
    Readers must support Delta reader version 3 (DBR 12.2+).
    """
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.enableDeletionVectors' = 'true')"
    )


def build_disable_deletion_vectors_sql(table: str) -> str:
    """
    Generate ALTER TABLE SQL to disable Deletion Vectors.

    After disabling, new DELETE/UPDATE operations revert to full file rewrites.
    Existing DV sidecar files remain until OPTIMIZE removes them.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return (
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        f"('delta.enableDeletionVectors' = 'false')"
    )


def build_optimize_after_deletes_sql(
    table: str,
    zorder_cols: Optional[List[str]] = None,
) -> str:
    """
    Generate OPTIMIZE SQL to physically remove DV-marked rows from files.

    Running OPTIMIZE after accumulating many DV-marked rows reduces read
    overhead and reclaims storage.  A subsequent VACUUM removes the now-
    orphaned DV sidecar files and old Parquet files.

    Parameters
    ----------
    table : str
    zorder_cols : list[str] | None
        Z-order columns to apply during the OPTIMIZE.

    Returns
    -------
    str
    """
    sql = f"OPTIMIZE {table}"
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        sql += f" ZORDER BY ({cols})"
    return sql


def describe_deletion_vectors() -> str:
    """
    Return a concise explanation of Deletion Vector behaviour.

    Useful for notebooks or documentation generation.

    Returns
    -------
    str
    """
    return (
        "Deletion Vectors (DBR 12.2+ / Delta 2.3+): Mark rows deleted in a "
        "bitmap sidecar file instead of rewriting the Parquet file. "
        "Reads merge the bitmap at scan time to skip marked rows. "
        "Run OPTIMIZE to physically remove marked rows and VACUUM to clean "
        "orphan DV files. Best for high-frequency point deletes."
    )
