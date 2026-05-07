"""
governance/grants_sql.py — Unity Catalog GRANT / REVOKE SQL.

Module   : governance.grants_sql
Concept  : Manage Unity Catalog privileges with auditable SQL statements
When     : Onboarding a new team, creating a new schema/table, or rotating access.

Unity Catalog privilege hierarchy
----------------------------------
Level         Securable         Key privileges
─────────     ──────────        ──────────────────────────────────────────
Metastore     metastore         CREATE CATALOG, CREATE EXTERNAL LOCATION
Catalog       catalog           USE CATALOG, CREATE SCHEMA
Schema        schema            USE SCHEMA, CREATE TABLE, CREATE VIEW, CREATE FUNCTION
Table/View    table / view      SELECT, MODIFY, ALL PRIVILEGES
Function      function          EXECUTE

Minimum grants for read access to a table
------------------------------------------
  GRANT USE CATALOG ON CATALOG my_catalog TO `team@example.com`;
  GRANT USE SCHEMA  ON SCHEMA  my_catalog.my_schema TO `team@example.com`;
  GRANT SELECT      ON TABLE   my_catalog.my_schema.my_table TO `team@example.com`;

Principal types in Unity Catalog
----------------------------------
- User:              `user@example.com`  (backtick-quoted in SQL)
- Group:             `my_group`
- Service principal: `12345678-...`  (UUID, backtick-quoted)
- account groups:    same syntax as group

Public API
----------
build_grant_sql(privilege, securable_type, securable, principal) -> str
build_revoke_sql(privilege, securable_type, securable, principal) -> str
build_grant_table_read_sql(catalog, schema, table, principal) -> list[str]
build_grant_schema_write_sql(catalog, schema, principal) -> list[str]
build_show_grants_sql(securable_type, securable) -> str
build_show_current_user_sql() -> str
"""

from __future__ import annotations

from typing import List

VALID_SECURABLE_TYPES = {
    "CATALOG", "SCHEMA", "TABLE", "VIEW", "FUNCTION",
    "EXTERNAL LOCATION", "STORAGE CREDENTIAL", "METASTORE",
}

TABLE_READ_PRIVILEGES = ["USE CATALOG", "USE SCHEMA", "SELECT"]
SCHEMA_WRITE_PRIVILEGES = ["USE CATALOG", "USE SCHEMA", "CREATE TABLE", "CREATE VIEW", "MODIFY"]


def _quote(principal: str) -> str:
    """Backtick-quote a principal name for Unity Catalog SQL."""
    return f"`{principal}`"


def build_grant_sql(
    privilege: str,
    securable_type: str,
    securable: str,
    principal: str,
) -> str:
    """
    Generate a GRANT privilege SQL statement.

    Parameters
    ----------
    privilege : str
        Privilege name: 'SELECT', 'MODIFY', 'USE SCHEMA', 'CREATE TABLE', etc.
    securable_type : str
        Unity Catalog securable type: 'CATALOG', 'SCHEMA', 'TABLE', 'VIEW', etc.
    securable : str
        Fully-qualified name of the securable.
        Examples: 'my_catalog', 'my_catalog.my_schema', 'my_catalog.my_schema.my_table'
    principal : str
        User email, group name, or service principal UUID.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If securable_type is not a known Unity Catalog type.
    """
    st = securable_type.upper()
    if st not in VALID_SECURABLE_TYPES:
        raise ValueError(f"securable_type '{st}' not in {VALID_SECURABLE_TYPES}")
    return f"GRANT {privilege} ON {st} {securable} TO {_quote(principal)}"


def build_revoke_sql(
    privilege: str,
    securable_type: str,
    securable: str,
    principal: str,
) -> str:
    """
    Generate a REVOKE privilege SQL statement.

    Parameters
    ----------
    privilege : str
    securable_type : str
    securable : str
    principal : str

    Returns
    -------
    str
    """
    st = securable_type.upper()
    if st not in VALID_SECURABLE_TYPES:
        raise ValueError(f"securable_type '{st}' not in {VALID_SECURABLE_TYPES}")
    return f"REVOKE {privilege} ON {st} {securable} FROM {_quote(principal)}"


def build_grant_table_read_sql(
    catalog: str,
    schema: str,
    table: str,
    principal: str,
) -> List[str]:
    """
    Generate the three GRANT statements needed for read access to a table.

    Unity Catalog enforces that a principal must have USE CATALOG on the catalog
    and USE SCHEMA on the schema before SELECT on the table is effective.

    Parameters
    ----------
    catalog : str
    schema : str
    principal : str

    Returns
    -------
    list[str]
        Three GRANT statements in order: USE CATALOG, USE SCHEMA, SELECT.
    """
    return [
        build_grant_sql("USE CATALOG", "CATALOG", catalog, principal),
        build_grant_sql("USE SCHEMA", "SCHEMA", f"{catalog}.{schema}", principal),
        build_grant_sql("SELECT", "TABLE", f"{catalog}.{schema}.{table}", principal),
    ]


def build_grant_schema_write_sql(
    catalog: str,
    schema: str,
    principal: str,
) -> List[str]:
    """
    Generate GRANT statements for full read/write access to a schema.

    Parameters
    ----------
    catalog : str
    schema : str
    principal : str

    Returns
    -------
    list[str]
    """
    fq_schema = f"{catalog}.{schema}"
    return [
        build_grant_sql("USE CATALOG", "CATALOG", catalog, principal),
        build_grant_sql("USE SCHEMA", "SCHEMA", fq_schema, principal),
        build_grant_sql("CREATE TABLE", "SCHEMA", fq_schema, principal),
        build_grant_sql("CREATE VIEW", "SCHEMA", fq_schema, principal),
        build_grant_sql("MODIFY", "SCHEMA", fq_schema, principal),
    ]


def build_grant_all_privileges_sql(
    securable_type: str,
    securable: str,
    principal: str,
) -> str:
    """Generate GRANT ALL PRIVILEGES SQL (admin use only)."""
    st = securable_type.upper()
    if st not in VALID_SECURABLE_TYPES:
        raise ValueError(f"securable_type '{st}' not in {VALID_SECURABLE_TYPES}")
    return f"GRANT ALL PRIVILEGES ON {st} {securable} TO {_quote(principal)}"


def build_show_grants_sql(
    securable_type: str,
    securable: str,
) -> str:
    """
    Generate SHOW GRANTS SQL for auditing who has access to a securable.

    Parameters
    ----------
    securable_type : str
    securable : str

    Returns
    -------
    str
    """
    st = securable_type.upper()
    return f"SHOW GRANTS ON {st} {securable}"


def build_show_current_user_sql() -> str:
    """Return SQL that reveals the current user identity."""
    return "SELECT current_user()"
