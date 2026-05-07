"""
governance/row_filter_sql.py — Unity Catalog Row-Level Security (Row Filters).

Module   : governance.row_filter_sql
Concept  : Restrict which rows each user/group can see using SQL functions
When     : Tables contain data for multiple tenants, regions, or security classifications
           and different users should see different subsets of rows.

How row filters work in Unity Catalog
--------------------------------------
1. CREATE FUNCTION that returns BOOLEAN.
   The function receives column values from the row and the current user context.
   It returns TRUE for rows the current user is allowed to see.

2. ALTER TABLE ... SET ROW FILTER <function> ON (<columns>).
   Delta evaluates the function for every row read from the table.
   Rows where the function returns FALSE are invisible to the caller.
   The filter is transparent — callers do not know it exists.

3. The function is evaluated with the calling user's identity, not the function
   owner's identity.  Use is_account_group_member() or current_user() to make
   filter decisions.

Key Unity Catalog functions for RLS
-------------------------------------
current_user()                    → email/ID of the calling user
is_account_group_member('group')  → TRUE if calling user is in the group
is_member('group')                → TRUE if calling user is in the workspace group

Example: tenant isolation
--------------------------
    CREATE FUNCTION tenant_filter(tenant_id STRING)
    RETURN current_user() = tenant_id
        OR is_account_group_member('admins');

    ALTER TABLE orders SET ROW FILTER tenant_filter ON (tenant_id);

Public API
----------
build_row_filter_function_sql(fn_name, params, body, catalog, schema) -> str
build_set_row_filter_sql(table, fn_name, columns) -> str
build_drop_row_filter_sql(table) -> str
build_drop_row_filter_function_sql(fn_name) -> str
build_tenant_filter_sql(fn_name, tenant_col, admin_group, catalog, schema) -> str
build_classification_filter_sql(fn_name, class_col, allowed_groups, catalog, schema) -> str
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple


def build_row_filter_function_sql(
    fn_name: str,
    params: List[Tuple[str, str]],
    body: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    """
    Generate CREATE FUNCTION SQL for a row filter.

    The function must return BOOLEAN and accept the row columns it inspects.

    Parameters
    ----------
    fn_name : str
        Function name (without catalog/schema prefix if catalog/schema provided).
    params : list[tuple[str, str]]
        [(param_name, param_type), ...] — must match the columns in SET ROW FILTER.
    body : str
        SQL RETURN expression.  Example: "current_user() = tenant_id"
    catalog : str | None
        Catalog to create the function in.
    schema : str | None
        Schema to create the function in.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If params is empty or body is empty.
    """
    if not params:
        raise ValueError("params must not be empty — row filter needs at least one column parameter")
    if not body:
        raise ValueError("body must not be empty")

    fq_name = fn_name
    if catalog and schema:
        fq_name = f"{catalog}.{schema}.{fn_name}"
    elif schema:
        fq_name = f"{schema}.{fn_name}"

    param_list = ", ".join(f"{name} {dtype}" for name, dtype in params)
    return (
        f"CREATE OR REPLACE FUNCTION {fq_name}({param_list})\n"
        f"RETURN {body}"
    )


def build_set_row_filter_sql(
    table: str,
    fn_name: str,
    columns: List[str],
) -> str:
    """
    Generate ALTER TABLE ... SET ROW FILTER SQL.

    Parameters
    ----------
    table : str
        Fully-qualified table name.
    fn_name : str
        Fully-qualified function name created with build_row_filter_function_sql().
    columns : list[str]
        Column names passed to the filter function (must match function params in order).

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If columns is empty.
    """
    if not columns:
        raise ValueError("columns must not be empty")
    col_list = ", ".join(columns)
    return f"ALTER TABLE {table} SET ROW FILTER {fn_name} ON ({col_list})"


def build_drop_row_filter_sql(table: str) -> str:
    """
    Generate ALTER TABLE ... DROP ROW FILTER SQL.

    Removes the row filter from the table.  The filter function is NOT dropped.

    Parameters
    ----------
    table : str

    Returns
    -------
    str
    """
    return f"ALTER TABLE {table} DROP ROW FILTER"


def build_drop_row_filter_function_sql(fn_name: str) -> str:
    """
    Generate DROP FUNCTION SQL for a row filter function.

    Must remove the filter from all tables that use it before dropping.

    Parameters
    ----------
    fn_name : str

    Returns
    -------
    str
    """
    return f"DROP FUNCTION IF EXISTS {fn_name}"


def build_tenant_filter_sql(
    fn_name: str,
    tenant_col: str,
    admin_group: str = "admins",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    """
    Build a tenant-isolation row filter function.

    Rows are visible only when:
      - The current user's email matches the tenant_col value, OR
      - The current user is a member of admin_group (bypass for ops/admin).

    Parameters
    ----------
    fn_name : str
    tenant_col : str
        Column that holds the tenant identifier (user email or tenant ID).
    admin_group : str
        Account group whose members can see all rows.
    catalog : str | None
    schema : str | None

    Returns
    -------
    str
    """
    body = (
        f"current_user() = {tenant_col} "
        f"OR is_account_group_member('{admin_group}')"
    )
    return build_row_filter_function_sql(
        fn_name,
        params=[(tenant_col, "STRING")],
        body=body,
        catalog=catalog,
        schema=schema,
    )


def build_classification_filter_sql(
    fn_name: str,
    class_col: str,
    allowed_groups: Dict[str, List[str]],
    admin_group: str = "admins",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    """
    Build a classification-based row filter.

    Maps classification labels to the groups permitted to see them.
    Admins bypass all restrictions.

    Parameters
    ----------
    fn_name : str
    class_col : str
        Column containing the data classification label (e.g., 'PUBLIC', 'CONFIDENTIAL').
    allowed_groups : dict
        {classification_label: [group_name, ...]} mapping.
        Example: {"PUBLIC": ["all_users"], "CONFIDENTIAL": ["analysts", "engineers"]}
    admin_group : str
    catalog : str | None
    schema : str | None

    Returns
    -------
    str
    """
    conditions = []
    for label, groups in allowed_groups.items():
        group_checks = " OR ".join(
            f"is_account_group_member('{g}')" for g in groups
        )
        conditions.append(f"({class_col} = '{label}' AND ({group_checks}))")

    classification_body = " OR ".join(conditions)
    body = (
        f"is_account_group_member('{admin_group}') "
        f"OR ({classification_body})"
    )
    return build_row_filter_function_sql(
        fn_name,
        params=[(class_col, "STRING")],
        body=body,
        catalog=catalog,
        schema=schema,
    )
