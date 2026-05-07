"""
governance/column_mask_sql.py — Unity Catalog Column Masking SQL.

Module   : governance.column_mask_sql
Concept  : Replace sensitive column values with redacted or hashed values per user
When     : Tables contain PII (SSN, email, phone), financial data, or secrets
           that must be masked for some users while remaining visible to others.

How column masks work
----------------------
1. CREATE FUNCTION that accepts the raw column value and returns a masked version.
   The function may return: NULL, a constant, a hash, a partial value, or the
   original value (for privileged users).

2. ALTER TABLE ... ALTER COLUMN ... SET MASK <function>.
   Every time the column is read, Spark passes the raw value through the function.
   The caller sees only what the function returns.

3. Like row filters, column masks are transparent to callers.

Masking strategies
------------------
Null masking         → Return NULL for non-privileged users.
Constant masking     → Return a fixed string like 'REDACTED'.
Partial masking      → Show last 4 digits: REGEXP_REPLACE(ssn, '\\d{3}-\\d{2}-', '***-**-')
Hash masking         → SHA256 of the value — preserves referential integrity for joins.
Format-preserving    → Keep structure but scramble digits (for tokenization).

When NOT to use column masks
-----------------------------
- When you need to mask entire rows (use row filters instead).
- When performance is critical — every read invokes the function.
- When the masked column is in a WHERE clause filter — masking applies AFTER filter
  evaluation, so masked values can still be used to filter (this may be unexpected).

Public API
----------
build_column_mask_function_sql(fn_name, col_param, col_type, body, catalog, schema) -> str
build_set_column_mask_sql(table, column, fn_name) -> str
build_drop_column_mask_sql(table, column) -> str
build_null_mask_sql(fn_name, col_type, privileged_group, catalog, schema) -> str
build_hash_mask_sql(fn_name, col_type, privileged_group, catalog, schema) -> str
build_partial_email_mask_sql(fn_name, privileged_group, catalog, schema) -> str
"""

from __future__ import annotations

from typing import Optional


def build_column_mask_function_sql(
    fn_name: str,
    col_param: str,
    col_type: str,
    body: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    """
    Generate CREATE FUNCTION SQL for a column mask.

    The function accepts the raw column value and returns the masked value.
    Return type must match the column type.

    Parameters
    ----------
    fn_name : str
    col_param : str
        Name of the column parameter in the function signature.
    col_type : str
        SQL data type of the column (and return type).  Example: 'STRING', 'BIGINT'.
    body : str
        RETURN expression.  Has access to col_param and Unity Catalog context functions.
    catalog : str | None
    schema : str | None

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If fn_name, col_param, col_type, or body is empty.
    """
    for field, val in [("fn_name", fn_name), ("col_param", col_param),
                       ("col_type", col_type), ("body", body)]:
        if not val:
            raise ValueError(f"{field} must not be empty")

    fq_name = fn_name
    if catalog and schema:
        fq_name = f"{catalog}.{schema}.{fn_name}"
    elif schema:
        fq_name = f"{schema}.{fn_name}"

    return (
        f"CREATE OR REPLACE FUNCTION {fq_name}({col_param} {col_type})\n"
        f"RETURN {body}"
    )


def build_set_column_mask_sql(
    table: str,
    column: str,
    fn_name: str,
) -> str:
    """
    Generate ALTER TABLE ... ALTER COLUMN ... SET MASK SQL.

    Parameters
    ----------
    table : str
        Fully-qualified table name.
    column : str
        Column to mask.
    fn_name : str
        Fully-qualified mask function name.

    Returns
    -------
    str
    """
    return f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {fn_name}"


def build_drop_column_mask_sql(table: str, column: str) -> str:
    """
    Generate ALTER TABLE ... ALTER COLUMN ... DROP MASK SQL.

    Removes the mask from the column.  The mask function is NOT dropped.

    Parameters
    ----------
    table : str
    column : str

    Returns
    -------
    str
    """
    return f"ALTER TABLE {table} ALTER COLUMN {column} DROP MASK"


def build_null_mask_sql(
    fn_name: str,
    col_type: str,
    privileged_group: str = "analysts",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    """
    Build a null-masking function: non-privileged users see NULL.

    Parameters
    ----------
    fn_name : str
    col_type : str
    privileged_group : str
        Group whose members see the real value.
    catalog : str | None
    schema : str | None

    Returns
    -------
    str
    """
    body = (
        f"CASE WHEN is_account_group_member('{privileged_group}') "
        f"THEN col ELSE NULL END"
    )
    return build_column_mask_function_sql(
        fn_name, "col", col_type, body, catalog, schema
    )


def build_hash_mask_sql(
    fn_name: str,
    col_type: str = "STRING",
    privileged_group: str = "analysts",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    """
    Build a SHA-256 hash masking function.

    Non-privileged users see the SHA-256 hex digest of the value.
    This preserves referential integrity — the same input always produces
    the same hash — so masked values can still be used in GROUP BY / JOIN.

    Parameters
    ----------
    fn_name : str
    col_type : str
    privileged_group : str
    catalog : str | None
    schema : str | None

    Returns
    -------
    str
    """
    body = (
        f"CASE WHEN is_account_group_member('{privileged_group}') "
        f"THEN col ELSE sha2(CAST(col AS STRING), 256) END"
    )
    return build_column_mask_function_sql(
        fn_name, "col", col_type, body, catalog, schema
    )


def build_partial_email_mask_sql(
    fn_name: str,
    privileged_group: str = "analysts",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    """
    Build a partial email masking function.

    Non-privileged users see only the domain: 'u***@example.com'.
    Preserves the domain for analytics while hiding the local part.

    Parameters
    ----------
    fn_name : str
    privileged_group : str
    catalog : str | None
    schema : str | None

    Returns
    -------
    str
    """
    body = (
        f"CASE WHEN is_account_group_member('{privileged_group}') "
        f"THEN col "
        f"ELSE CONCAT(LEFT(col, 1), '***@', SPLIT_PART(col, '@', 2)) END"
    )
    return build_column_mask_function_sql(
        fn_name, "col", "STRING", body, catalog, schema
    )
