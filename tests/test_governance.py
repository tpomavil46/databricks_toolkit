"""Tests for governance.grants_sql, row_filter_sql, column_mask_sql, audit_sql."""

from __future__ import annotations

import pytest

from governance.grants_sql import (
    build_grant_sql,
    build_revoke_sql,
    build_grant_table_read_sql,
    build_grant_schema_write_sql,
    build_grant_all_privileges_sql,
    build_show_grants_sql,
    build_show_current_user_sql,
)
from governance.row_filter_sql import (
    build_row_filter_function_sql,
    build_set_row_filter_sql,
    build_drop_row_filter_sql,
    build_drop_row_filter_function_sql,
    build_tenant_filter_sql,
    build_classification_filter_sql,
)
from governance.column_mask_sql import (
    build_column_mask_function_sql,
    build_set_column_mask_sql,
    build_drop_column_mask_sql,
    build_null_mask_sql,
    build_hash_mask_sql,
    build_partial_email_mask_sql,
)
from governance.audit_sql import (
    build_show_table_grants_sql,
    build_list_tables_for_principal_sql,
    build_list_row_filters_sql,
    build_list_column_masks_sql,
    build_audit_log_query_sql,
)


# ---------------------------------------------------------------------------
# grants_sql
# ---------------------------------------------------------------------------

class TestBuildGrantSql:
    def test_grant_keyword(self):
        sql = build_grant_sql("SELECT", "TABLE", "cat.schema.t", "user@ex.com")
        assert "GRANT SELECT" in sql

    def test_on_clause(self):
        sql = build_grant_sql("SELECT", "TABLE", "cat.schema.t", "user@ex.com")
        assert "ON TABLE cat.schema.t" in sql

    def test_to_principal_quoted(self):
        sql = build_grant_sql("SELECT", "TABLE", "t", "user@ex.com")
        assert "`user@ex.com`" in sql

    def test_invalid_securable_type_raises(self):
        with pytest.raises(ValueError):
            build_grant_sql("SELECT", "DATABASE", "t", "u@e.com")

    def test_schema_type(self):
        sql = build_grant_sql("USE SCHEMA", "SCHEMA", "cat.schema", "g")
        assert "ON SCHEMA cat.schema" in sql


class TestBuildRevokeSql:
    def test_revoke_keyword(self):
        sql = build_revoke_sql("SELECT", "TABLE", "t", "u@e.com")
        assert "REVOKE SELECT" in sql

    def test_from_principal(self):
        sql = build_revoke_sql("SELECT", "TABLE", "t", "u@e.com")
        assert "FROM `u@e.com`" in sql

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError):
            build_revoke_sql("SELECT", "BUCKET", "t", "u@e.com")


class TestBuildGrantTableReadSql:
    def test_returns_three_statements(self):
        stmts = build_grant_table_read_sql("cat", "schema", "tbl", "u@e.com")
        assert len(stmts) == 3

    def test_use_catalog_first(self):
        stmts = build_grant_table_read_sql("mycat", "s", "t", "u@e.com")
        assert "USE CATALOG" in stmts[0]
        assert "mycat" in stmts[0]

    def test_use_schema_second(self):
        stmts = build_grant_table_read_sql("cat", "myschema", "t", "u@e.com")
        assert "USE SCHEMA" in stmts[1]
        assert "myschema" in stmts[1]

    def test_select_third(self):
        stmts = build_grant_table_read_sql("cat", "s", "mytable", "u@e.com")
        assert "SELECT" in stmts[2]
        assert "mytable" in stmts[2]


class TestBuildGrantSchemaWriteSql:
    def test_five_statements(self):
        stmts = build_grant_schema_write_sql("cat", "schema", "u@e.com")
        assert len(stmts) == 5

    def test_create_table_present(self):
        stmts = build_grant_schema_write_sql("cat", "schema", "u@e.com")
        combined = " ".join(stmts)
        assert "CREATE TABLE" in combined

    def test_modify_present(self):
        stmts = build_grant_schema_write_sql("cat", "schema", "u@e.com")
        combined = " ".join(stmts)
        assert "MODIFY" in combined


class TestBuildShowGrantsSql:
    def test_show_grants(self):
        sql = build_show_grants_sql("TABLE", "cat.schema.t")
        assert "SHOW GRANTS ON TABLE cat.schema.t" in sql

    def test_show_current_user(self):
        sql = build_show_current_user_sql()
        assert "current_user()" in sql.lower()


# ---------------------------------------------------------------------------
# row_filter_sql
# ---------------------------------------------------------------------------

class TestBuildRowFilterFunctionSql:
    def test_create_function(self):
        sql = build_row_filter_function_sql(
            "my_filter", [("tenant_id", "STRING")], "current_user() = tenant_id"
        )
        assert "CREATE OR REPLACE FUNCTION my_filter" in sql

    def test_param_in_signature(self):
        sql = build_row_filter_function_sql(
            "f", [("col", "STRING")], "col IS NOT NULL"
        )
        assert "col STRING" in sql

    def test_return_body(self):
        sql = build_row_filter_function_sql(
            "f", [("col", "STRING")], "col = 'PUBLIC'"
        )
        assert "RETURN col = 'PUBLIC'" in sql

    def test_fully_qualified_when_catalog_schema(self):
        sql = build_row_filter_function_sql(
            "f", [("c", "STRING")], "TRUE", catalog="cat", schema="sch"
        )
        assert "cat.sch.f" in sql

    def test_empty_params_raises(self):
        with pytest.raises(ValueError):
            build_row_filter_function_sql("f", [], "TRUE")

    def test_empty_body_raises(self):
        with pytest.raises(ValueError):
            build_row_filter_function_sql("f", [("c", "STRING")], "")


class TestBuildSetRowFilterSql:
    def test_alter_table(self):
        sql = build_set_row_filter_sql("cat.s.t", "cat.s.my_filter", ["tenant_id"])
        assert "ALTER TABLE cat.s.t" in sql
        assert "SET ROW FILTER" in sql

    def test_columns_in_sql(self):
        sql = build_set_row_filter_sql("t", "f", ["tenant_id", "region"])
        assert "tenant_id" in sql
        assert "region" in sql

    def test_empty_columns_raises(self):
        with pytest.raises(ValueError):
            build_set_row_filter_sql("t", "f", [])


class TestBuildDropRowFilterSql:
    def test_drop_row_filter(self):
        sql = build_drop_row_filter_sql("cat.schema.t")
        assert "ALTER TABLE cat.schema.t DROP ROW FILTER" in sql


class TestBuildTenantFilterSql:
    def test_current_user_in_body(self):
        sql = build_tenant_filter_sql("tenant_filter", "tenant_id")
        assert "current_user()" in sql

    def test_admin_group_in_body(self):
        sql = build_tenant_filter_sql("f", "tenant_id", admin_group="ops")
        assert "ops" in sql

    def test_create_function_keyword(self):
        sql = build_tenant_filter_sql("f", "tenant_id")
        assert "CREATE OR REPLACE FUNCTION" in sql


class TestBuildClassificationFilterSql:
    def test_classification_labels_in_sql(self):
        sql = build_classification_filter_sql(
            "class_filter", "classification",
            {"PUBLIC": ["all_users"], "CONFIDENTIAL": ["analysts"]}
        )
        assert "PUBLIC" in sql
        assert "CONFIDENTIAL" in sql

    def test_group_check_in_sql(self):
        sql = build_classification_filter_sql(
            "f", "class",
            {"INTERNAL": ["engineers"]}
        )
        assert "engineers" in sql

    def test_admin_bypass(self):
        sql = build_classification_filter_sql("f", "class", {}, admin_group="admins")
        assert "admins" in sql


# ---------------------------------------------------------------------------
# column_mask_sql
# ---------------------------------------------------------------------------

class TestBuildColumnMaskFunctionSql:
    def test_create_function(self):
        sql = build_column_mask_function_sql("my_mask", "col", "STRING", "NULL")
        assert "CREATE OR REPLACE FUNCTION my_mask" in sql

    def test_param_and_type_in_sig(self):
        sql = build_column_mask_function_sql("f", "ssn", "STRING", "NULL")
        assert "ssn STRING" in sql

    def test_return_body(self):
        sql = build_column_mask_function_sql("f", "col", "STRING", "NULL")
        assert "RETURN NULL" in sql

    def test_fq_name_when_catalog_schema(self):
        sql = build_column_mask_function_sql("f", "col", "STRING", "NULL", catalog="c", schema="s")
        assert "c.s.f" in sql

    def test_empty_fn_name_raises(self):
        with pytest.raises(ValueError):
            build_column_mask_function_sql("", "col", "STRING", "NULL")

    def test_empty_body_raises(self):
        with pytest.raises(ValueError):
            build_column_mask_function_sql("f", "col", "STRING", "")


class TestBuildSetColumnMaskSql:
    def test_alter_table_alter_column(self):
        sql = build_set_column_mask_sql("cat.s.t", "ssn", "cat.s.my_mask")
        assert "ALTER TABLE cat.s.t ALTER COLUMN ssn SET MASK cat.s.my_mask" in sql


class TestBuildDropColumnMaskSql:
    def test_drop_mask(self):
        sql = build_drop_column_mask_sql("cat.s.t", "ssn")
        assert "ALTER TABLE cat.s.t ALTER COLUMN ssn DROP MASK" in sql


class TestBuildNullMaskSql:
    def test_null_in_body(self):
        sql = build_null_mask_sql("f", "STRING")
        assert "NULL" in sql

    def test_privileged_group_in_sql(self):
        sql = build_null_mask_sql("f", "STRING", privileged_group="data_team")
        assert "data_team" in sql

    def test_is_account_group_member(self):
        sql = build_null_mask_sql("f", "STRING")
        assert "is_account_group_member" in sql


class TestBuildHashMaskSql:
    def test_sha2_in_body(self):
        sql = build_hash_mask_sql("f")
        assert "sha2" in sql.lower()

    def test_privileged_sees_original(self):
        sql = build_hash_mask_sql("f", privileged_group="analysts")
        assert "analysts" in sql
        assert "THEN col" in sql


class TestBuildPartialEmailMaskSql:
    def test_at_sign_handling(self):
        sql = build_partial_email_mask_sql("f")
        assert "@" in sql or "SPLIT_PART" in sql

    def test_concat_in_body(self):
        sql = build_partial_email_mask_sql("f")
        assert "CONCAT" in sql.upper()


# ---------------------------------------------------------------------------
# audit_sql
# ---------------------------------------------------------------------------

class TestBuildShowTableGrantsSql:
    def test_information_schema(self):
        sql = build_show_table_grants_sql("cat", "schema", "t")
        assert "information_schema.table_privileges" in sql

    def test_schema_filter(self):
        sql = build_show_table_grants_sql("cat", "myschema", "t")
        assert "myschema" in sql

    def test_table_filter(self):
        sql = build_show_table_grants_sql("cat", "s", "mytable")
        assert "mytable" in sql


class TestBuildListTablesForPrincipalSql:
    def test_principal_filter(self):
        sql = build_list_tables_for_principal_sql("cat", "user@example.com")
        assert "user@example.com" in sql

    def test_selects_table_name(self):
        sql = build_list_tables_for_principal_sql("cat", "u@e.com")
        assert "table_name" in sql


class TestBuildAuditLogQuerySql:
    def test_system_access_audit(self):
        sql = build_audit_log_query_sql("2024-01-01T00:00:00", "2024-01-02T00:00:00")
        assert "system.access.audit" in sql

    def test_time_range(self):
        sql = build_audit_log_query_sql("2024-01-01T00:00:00", "2024-02-01T00:00:00")
        assert "2024-01-01T00:00:00" in sql
        assert "2024-02-01T00:00:00" in sql

    def test_action_filter(self):
        sql = build_audit_log_query_sql("2024-01-01T00:00:00", "2024-02-01T00:00:00", action_name="getTable")
        assert "getTable" in sql

    def test_user_filter(self):
        sql = build_audit_log_query_sql(
            "2024-01-01T00:00:00", "2024-02-01T00:00:00", user_identity="user@ex.com"
        )
        assert "user@ex.com" in sql

    def test_limit_in_sql(self):
        sql = build_audit_log_query_sql("2024-01-01T00:00:00", "2024-02-01T00:00:00", limit=500)
        assert "500" in sql
