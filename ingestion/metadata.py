"""
ingestion/metadata.py — Bronze lineage and audit columns.

Every Bronze table should carry a standard set of metadata columns that
answer: when was this row ingested, from where, by which pipeline, and in
which environment?  These columns are system-managed — they are added by the
ingestion layer, not produced by the source — so they use a leading
underscore prefix by convention.

Standard Bronze columns
-----------------------
_bronze_ingested_at : timestamp
    Wall-clock time the row was written (not the event time in the data).
    Use for SLA monitoring and debugging late-arriving data.

_bronze_source_path : string
    The physical file or object from which this row was read.  Populated by
    Spark's input_file_name() function.  Invaluable for tracing a bad row
    back to its origin file.

_bronze_pipeline : string
    Free-form identifier for the job or notebook that ingested the row.
    Set from SingleplexIngestionConfig.pipeline_name /
    MultiplexIngestionConfig.pipeline_name.

_bronze_env : string
    Deployment environment ('dev', 'staging', 'prod').  Allows a single
    Unity Catalog schema to hold tables from multiple environments without
    naming conflicts.

Usage
-----
    from ingestion.metadata import add_bronze_columns
    df = add_bronze_columns(df, pipeline_name="orders_daily", env="prod")

Functions
---------
add_bronze_columns(df, pipeline_name, env) -> DataFrame
    Attaches all four standard columns.  Safe to call multiple times
    (existing _bronze_* columns are overwritten, not duplicated).

strip_bronze_columns(df) -> DataFrame
    Removes all _bronze_* columns.  Useful when promoting Bronze → Silver
    and replacing metadata with Silver-layer provenance.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


_METADATA_PREFIX = "_bronze"


def add_bronze_columns(
    df: DataFrame,
    pipeline_name: str = "",
    env: str = "dev",
) -> DataFrame:
    """
    Add standard Bronze lineage columns to a DataFrame.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame (batch or streaming).
    pipeline_name : str
        Identifier for the ingestion job, e.g. 'orders_bronze_daily'.
    env : str
        Deployment environment: 'dev', 'staging', or 'prod'.

    Returns
    -------
    DataFrame
        Same data with four additional columns prepended.  If any
        _bronze_* column already exists it is replaced.

    Notes
    -----
    input_file_name() returns an empty string for DataFrames not backed by
    files (e.g., in-memory DataFrames in unit tests).  This is expected and
    harmless.
    """
    return (
        df
        .withColumn(f"{_METADATA_PREFIX}_ingested_at", F.current_timestamp())
        .withColumn(f"{_METADATA_PREFIX}_source_path", F.input_file_name())
        .withColumn(f"{_METADATA_PREFIX}_pipeline", F.lit(pipeline_name))
        .withColumn(f"{_METADATA_PREFIX}_env", F.lit(env))
    )


def strip_bronze_columns(df: DataFrame) -> DataFrame:
    """
    Drop all columns whose name starts with '_bronze_'.

    Use this when handing off to Silver transformation logic that should
    not carry Bronze-layer metadata (Silver will add its own provenance).
    """
    cols_to_drop = [c for c in df.columns if c.startswith(f"{_METADATA_PREFIX}_")]
    return df.drop(*cols_to_drop)


def bronze_column_names() -> list:
    """Return the list of standard Bronze metadata column names."""
    return [
        f"{_METADATA_PREFIX}_ingested_at",
        f"{_METADATA_PREFIX}_source_path",
        f"{_METADATA_PREFIX}_pipeline",
        f"{_METADATA_PREFIX}_env",
    ]
