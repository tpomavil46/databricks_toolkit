"""
delta_features/table_properties.py — Delta Table Properties (TBLPROPERTIES).

Module   : delta_features.table_properties
Concept  : Set, inspect, and unset Delta table configuration via TBLPROPERTIES
When     : Configuring CDF, DVs, retention, Liquid Clustering, auto-optimize,
           and other per-table Delta behaviours.

What it is
----------
Delta table properties are key-value pairs stored in the Delta transaction log.
They configure per-table behaviour that overrides session/cluster-level defaults.

Common properties
-----------------
delta.enableChangeDataFeed          Enable CDF (see cdf_sql.py).
delta.enableDeletionVectors         Enable Deletion Vectors (see deletion_vectors.py).
delta.autoOptimize.autoCompact      Auto-compact small files after writes.
delta.autoOptimize.optimizeWrite    Coalesce partitions before writing to reduce file count.
delta.deletedFileRetentionDuration  VACUUM retention (e.g., 'interval 30 days').
delta.logRetentionDuration          How long to keep the transaction log.
delta.columnMapping.mode            Enable column renaming without rewriting data ('name').
delta.minReaderVersion              Minimum Delta reader protocol version.
delta.minWriterVersion              Minimum Delta writer protocol version.

Public API
----------
build_set_tblproperties_sql(table, properties) -> str
build_unset_tblproperties_sql(table, keys) -> str
build_show_tblproperties_sql(table) -> str
COMMON_PROPERTIES: dict — reference of well-known property keys.
"""

from __future__ import annotations

from typing import Dict, List

from pyspark.sql import SparkSession

COMMON_PROPERTIES: Dict[str, str] = {
    "delta.enableChangeDataFeed": "Enable Change Data Feed. Values: true/false.",
    "delta.enableDeletionVectors": "Enable Deletion Vectors. Values: true/false. Requires DBR 12.2+.",
    "delta.autoOptimize.autoCompact": "Auto-compact files after writes. Values: true/false.",
    "delta.autoOptimize.optimizeWrite": "Coalesce partitions before write. Values: true/false.",
    "delta.deletedFileRetentionDuration": "VACUUM retention. Value: 'interval N days'.",
    "delta.logRetentionDuration": "Transaction log retention. Value: 'interval N days'.",
    "delta.columnMapping.mode": "Enable column rename without rewrite. Values: 'name'/'id'/'none'.",
    "delta.minReaderVersion": "Minimum reader protocol version (1, 2, or 3).",
    "delta.minWriterVersion": "Minimum writer protocol version (1–7).",
    "delta.dataSkippingNumIndexedCols": "Number of columns indexed for data skipping (default: 32).",
}


def build_set_tblproperties_sql(
    table: str,
    properties: Dict[str, str],
) -> str:
    """
    Generate ALTER TABLE ... SET TBLPROPERTIES SQL.

    Parameters
    ----------
    table : str
        Fully-qualified Delta table name.
    properties : dict[str, str]
        Property key-value pairs to set.
        Example: {'delta.enableChangeDataFeed': 'true'}

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If properties is empty.

    Examples
    --------
        sql = build_set_tblproperties_sql(
            "catalog.gold.dim_customer",
            {
                "delta.enableChangeDataFeed": "true",
                "delta.autoOptimize.autoCompact": "true",
            }
        )
    """
    if not properties:
        raise ValueError("properties must not be empty")
    props_str = ",\n  ".join(f"'{k}' = '{v}'" for k, v in properties.items())
    return f"ALTER TABLE {table} SET TBLPROPERTIES (\n  {props_str}\n)"


def build_unset_tblproperties_sql(
    table: str,
    keys: List[str],
    if_exists: bool = True,
) -> str:
    """
    Generate ALTER TABLE ... UNSET TBLPROPERTIES SQL.

    Removing a property reverts the table to the cluster/session default for
    that setting.

    Parameters
    ----------
    table : str
    keys : list[str]
        Property keys to remove.
    if_exists : bool
        When True, adds IF EXISTS to avoid errors if a key is not set.
        Default: True.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If keys is empty.
    """
    if not keys:
        raise ValueError("keys must not be empty")
    keys_str = ", ".join(f"'{k}'" for k in keys)
    suffix = " IF EXISTS" if if_exists else ""
    return f"ALTER TABLE {table} UNSET TBLPROPERTIES{suffix} ({keys_str})"


def build_show_tblproperties_sql(table: str) -> str:
    """
    Generate SHOW TBLPROPERTIES <table> to list all set properties.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return f"SHOW TBLPROPERTIES {table}"


def build_production_defaults_sql(table: str) -> str:
    """
    Generate ALTER TABLE SQL that applies a sensible set of production defaults.

    Properties applied:
    - CDF enabled (for downstream CDC).
    - Auto-optimize write enabled (reduces small files at write time).
    - Auto-compact enabled (compacts small files after each write).

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return build_set_tblproperties_sql(table, {
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    })


def apply_tblproperties(
    spark: SparkSession,
    table: str,
    properties: Dict[str, str],
) -> None:
    """
    Execute ALTER TABLE SET TBLPROPERTIES against a live Delta table.

    Parameters
    ----------
    spark : SparkSession
    table : str
    properties : dict[str, str]
    """
    spark.sql(build_set_tblproperties_sql(table, properties))
