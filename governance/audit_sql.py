"""
governance/audit_sql.py — Unity Catalog Audit Queries.

Module   : governance.audit_sql
Concept  : Query information_schema and system tables to audit who has what access
When     : Compliance audits, access reviews, debugging unexpected permission denials.

Information schema tables for governance
-----------------------------------------
information_schema.table_privileges   → grants on tables and views
information_schema.column_privileges  → column-level grants
information_schema.routine_privileges → grants on functions
system.access.audit                   → all Unity Catalog API calls (requires
                                        Databricks system tables enabled)

Public API
----------
build_show_table_grants_sql(catalog, schema, table) -> str
build_list_tables_for_principal_sql(catalog, principal) -> str
build_list_row_filters_sql(catalog, schema) -> str
build_list_column_masks_sql(catalog, schema, table) -> str
build_audit_log_query_sql(start_ts, end_ts, action_name) -> str
"""

from __future__ import annotations

from typing import Optional


def build_show_table_grants_sql(
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """
    Query information_schema for all grants on a specific table.

    Parameters
    ----------
    catalog : str
    schema : str
    table : str

    Returns
    -------
    str
    """
    return f"""SELECT
  grantee,
  privilege_type,
  is_grantable,
  grantor
FROM {catalog}.information_schema.table_privileges
WHERE table_schema = '{schema}'
  AND table_name   = '{table}'
ORDER BY grantee, privilege_type"""


def build_list_tables_for_principal_sql(
    catalog: str,
    principal: str,
) -> str:
    """
    List all tables a principal has at least SELECT on.

    Parameters
    ----------
    catalog : str
    principal : str
        User email or group name.

    Returns
    -------
    str
    """
    return f"""SELECT DISTINCT
  table_schema,
  table_name,
  privilege_type
FROM {catalog}.information_schema.table_privileges
WHERE grantee = '{principal}'
ORDER BY table_schema, table_name"""


def build_list_row_filters_sql(
    catalog: str,
    schema: str,
) -> str:
    """
    List all tables in a schema that have row filters applied.

    Uses information_schema.table_constraints (DLT / UC system tables).

    Parameters
    ----------
    catalog : str
    schema : str

    Returns
    -------
    str
    """
    return f"""SELECT
  table_name,
  row_filter_name,
  row_filter_columns
FROM {catalog}.information_schema.tables
WHERE table_schema = '{schema}'
  AND row_filter_name IS NOT NULL
ORDER BY table_name"""


def build_list_column_masks_sql(
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """
    List all columns in a table that have column masks applied.

    Parameters
    ----------
    catalog : str
    schema : str
    table : str

    Returns
    -------
    str
    """
    return f"""SELECT
  column_name,
  mask_name,
  mask_using_columns
FROM {catalog}.information_schema.columns
WHERE table_schema = '{schema}'
  AND table_name   = '{table}'
  AND mask_name IS NOT NULL
ORDER BY column_name"""


def build_audit_log_query_sql(
    start_ts: str,
    end_ts: str,
    action_name: Optional[str] = None,
    user_identity: Optional[str] = None,
    limit: int = 1000,
) -> str:
    """
    Query the Databricks system audit log for Unity Catalog events.

    Requires system.access.audit to be enabled in the metastore.
    Use for SOC2 / compliance evidence: who accessed what table, when.

    Parameters
    ----------
    start_ts : str
        ISO-8601 start timestamp.  Example: '2024-06-01T00:00:00'
    end_ts : str
        ISO-8601 end timestamp.
    action_name : str | None
        Filter to a specific action.  Example: 'getTable', 'createTable',
        'updatePermissions', 'execute'.
    user_identity : str | None
        Filter to a specific user.
    limit : int

    Returns
    -------
    str
    """
    filters = [
        f"event_time >= TIMESTAMP '{start_ts}'",
        f"event_time <  TIMESTAMP '{end_ts}'",
    ]
    if action_name:
        filters.append(f"action_name = '{action_name}'")
    if user_identity:
        filters.append(f"user_identity.email = '{user_identity}'")

    where_clause = "\n  AND ".join(filters)
    return f"""SELECT
  event_time,
  user_identity.email                      AS user_email,
  action_name,
  request_params.full_name_arg             AS object_name,
  response.status_code                     AS status
FROM system.access.audit
WHERE {where_clause}
ORDER BY event_time DESC
LIMIT {limit}"""
