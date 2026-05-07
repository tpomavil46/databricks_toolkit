"""
dlt/table_sql.py — DLT SQL Table and View Templates.

Module   : dlt.table_sql
Concept  : SQL DDL patterns for DLT STREAMING TABLE and LIVE VIEW
When     : Writing DLT pipelines in SQL notebooks (as opposed to Python @dlt.table).

DLT SQL vs Python
-----------------
SQL DLT (this module):
    CREATE OR REFRESH STREAMING TABLE bronze_events
    TBLPROPERTIES (...)
    AS SELECT * FROM cloud_files(...)

Python DLT:
    @dlt.table
    def bronze_events():
        return spark.readStream.format("cloudFiles")...

SQL DLT is simpler for pure transformations.  Python DLT is better when you need
loops, conditional logic, or passing DataFrames between functions.

Key DLT SQL keywords
--------------------
STREAMING TABLE     → Stateful, append-only source.  Use for raw ingest.
LIVE VIEW           → Ephemeral; re-computed on every pipeline run.  No storage cost.
MATERIALIZED VIEW   → Persisted result of a query.  Like a computed table.

STREAM(table_name)  → Read a live table as a stream (enables incremental processing).
cloud_files(path)   → AutoLoader source for SQL DLT pipelines.

Expectations in SQL
-------------------
CONSTRAINT name EXPECT (condition)                       → monitor only
CONSTRAINT name EXPECT (condition) ON VIOLATION DROP ROW → drop bad rows
CONSTRAINT name EXPECT (condition) ON VIOLATION FAIL     → fail pipeline

Public API
----------
build_streaming_table_sql(name, select_body, constraints, tblproperties, partition_cols) -> str
build_live_view_sql(name, select_body) -> str
build_materialized_view_sql(name, select_body, constraints, partition_cols) -> str
build_autoloader_source_sql(path, format, schema_location) -> str
build_constraint_clause(name, condition, on_violation) -> str
"""

from __future__ import annotations

from typing import Dict, List, Optional

VALID_ON_VIOLATION = {"DROP ROW", "FAIL", ""}


def build_constraint_clause(
    name: str,
    condition: str,
    on_violation: str = "",
) -> str:
    """
    Build a DLT CONSTRAINT clause for embedding in CREATE TABLE SQL.

    Parameters
    ----------
    name : str
        Constraint name shown in the DLT event log.
    condition : str
        SQL boolean expression.
    on_violation : str
        '' (monitor only), 'DROP ROW', or 'FAIL'.

    Returns
    -------
    str
        CONSTRAINT ... EXPECT (...) [ON VIOLATION ...]

    Raises
    ------
    ValueError
        If on_violation is not valid.
    """
    if on_violation not in VALID_ON_VIOLATION:
        raise ValueError(
            f"on_violation must be one of {VALID_ON_VIOLATION}, got '{on_violation}'"
        )
    suffix = f" ON VIOLATION {on_violation}" if on_violation else ""
    return f"CONSTRAINT {name} EXPECT ({condition}){suffix}"


def build_streaming_table_sql(
    name: str,
    select_body: str,
    constraints: Optional[List[str]] = None,
    tblproperties: Optional[Dict[str, str]] = None,
    partition_cols: Optional[List[str]] = None,
    comment: Optional[str] = None,
) -> str:
    """
    Generate CREATE OR REFRESH STREAMING TABLE SQL.

    Use for append-only sources: AutoLoader, Kafka, raw Delta tables.

    Parameters
    ----------
    name : str
        Live table name.
    select_body : str
        The body of the SELECT (everything after SELECT, without the keyword).
        Example: "* FROM cloud_files('/mnt/raw', 'json')"
    constraints : list[str] | None
        From build_constraint_clause().
    tblproperties : dict | None
        Table properties.
    partition_cols : list[str] | None
    comment : str | None

    Returns
    -------
    str
    """
    constraint_lines = ""
    if constraints:
        constraint_lines = "\n" + "\n".join(f"  {c}," for c in constraints).rstrip(",")

    props_lines = ""
    if tblproperties:
        kv = ",\n".join(f"  '{k}' = '{v}'" for k, v in tblproperties.items())
        props_lines = f"\nTBLPROPERTIES (\n{kv}\n)"

    partition_clause = ""
    if partition_cols:
        partition_clause = f"\nPARTITIONED BY ({', '.join(partition_cols)})"

    comment_clause = f"\nCOMMENT '{comment}'" if comment else ""

    return (
        f"CREATE OR REFRESH STREAMING TABLE {name}{comment_clause}"
        f"{constraint_lines}"
        f"{partition_clause}"
        f"{props_lines}\n"
        f"AS SELECT {select_body}"
    )


def build_live_view_sql(
    name: str,
    select_body: str,
    comment: Optional[str] = None,
) -> str:
    """
    Generate CREATE OR REFRESH LIVE VIEW SQL.

    Views are re-computed on every pipeline update.  Use for lightweight
    intermediate transformations that don't need to be stored.

    Parameters
    ----------
    name : str
    select_body : str
    comment : str | None

    Returns
    -------
    str
    """
    comment_clause = f"\nCOMMENT '{comment}'" if comment else ""
    return f"CREATE OR REFRESH LIVE VIEW {name}{comment_clause}\nAS SELECT {select_body}"


def build_materialized_view_sql(
    name: str,
    select_body: str,
    constraints: Optional[List[str]] = None,
    partition_cols: Optional[List[str]] = None,
    comment: Optional[str] = None,
) -> str:
    """
    Generate CREATE OR REFRESH MATERIALIZED VIEW SQL.

    Materialized views are stored and incrementally maintained.  Use for
    aggregations, joins, and transformations where you want storage + incremental
    refresh (not full recompute on each run).

    Parameters
    ----------
    name : str
    select_body : str
    constraints : list[str] | None
    partition_cols : list[str] | None
    comment : str | None

    Returns
    -------
    str
    """
    constraint_lines = ""
    if constraints:
        constraint_lines = "\n" + "\n".join(f"  {c}," for c in constraints).rstrip(",")

    partition_clause = ""
    if partition_cols:
        partition_clause = f"\nPARTITIONED BY ({', '.join(partition_cols)})"

    comment_clause = f"\nCOMMENT '{comment}'" if comment else ""

    return (
        f"CREATE OR REFRESH MATERIALIZED VIEW {name}{comment_clause}"
        f"{constraint_lines}"
        f"{partition_clause}\n"
        f"AS SELECT {select_body}"
    )


def build_autoloader_source_expr(
    path: str,
    fmt: str = "json",
    schema_location: Optional[str] = None,
) -> str:
    """
    Build the cloud_files() SQL expression for an AutoLoader source.

    Parameters
    ----------
    path : str
        Cloud storage path.
    fmt : str
        File format: 'json', 'csv', 'parquet', 'avro'.
    schema_location : str | None
        Schema inference location.  When None, DLT manages it automatically.

    Returns
    -------
    str
        cloud_files(path, format[, map_of_options]) expression.
    """
    if schema_location:
        return f"cloud_files('{path}', '{fmt}', map('cloudFiles.schemaLocation', '{schema_location}'))"
    return f"cloud_files('{path}', '{fmt}')"


def build_stream_read_expr(live_table: str) -> str:
    """
    Build a STREAM(table) expression for reading a live table as a stream.

    Parameters
    ----------
    live_table : str
        Name of the LIVE or STREAMING TABLE to read incrementally.

    Returns
    -------
    str
    """
    return f"STREAM({live_table})"
