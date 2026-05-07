"""
tests/test_upserts_basic.py — Unit tests for basic INSERT/UPDATE MERGE.

Tests cover:
- UpsertConfig: validation, resolve_update_columns, from_dict.
- build_basic_merge_sql: structure, table names, key conditions.
- build_selective_update_sql: explicit SET clause with correct column list.

apply_basic_merge() (Delta MERGE) is integration-only; see tests/integration/.
"""

import pytest
from tests.fixtures.upsert_data import source_basic, target_basic
from upserts.config import UpsertConfig
from upserts.basic_merge_sql import build_basic_merge_sql, build_selective_update_sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return UpsertConfig(
        source_table="v_customers_incoming",
        target_table="catalog.gold.dim_customer",
        merge_keys=["customer_id"],
    )


@pytest.fixture
def config_selective():
    return UpsertConfig(
        source_table="v_customers_incoming",
        target_table="catalog.gold.dim_customer",
        merge_keys=["customer_id"],
        update_columns=["email", "tier"],
    )


# ---------------------------------------------------------------------------
# UpsertConfig validation
# ---------------------------------------------------------------------------

class TestUpsertConfig:

    def test_empty_source_table_raises(self):
        with pytest.raises(ValueError, match="source_table"):
            UpsertConfig(source_table="", target_table="t", merge_keys=["id"])

    def test_empty_target_table_raises(self):
        with pytest.raises(ValueError, match="target_table"):
            UpsertConfig(source_table="s", target_table="", merge_keys=["id"])

    def test_empty_merge_keys_raises(self):
        with pytest.raises(ValueError, match="merge_keys"):
            UpsertConfig(source_table="s", target_table="t", merge_keys=[])

    def test_defaults(self, config):
        assert config.delete_indicator_col == ""
        assert config.delete_indicator_value == "true"
        assert config.event_ts_col == ""
        assert config.content_hash_col == "_content_hash"
        assert config.allow_schema_evolution is False
        assert config.env == "dev"

    def test_from_dict_round_trip(self):
        d = {
            "source_table": "src",
            "target_table": "tgt",
            "merge_keys": ["id"],
            "update_columns": ["col1"],
            "delete_indicator_col": "op",
            "delete_indicator_value": "D",
            "event_ts_col": "ts",
            "content_hash_col": "_hash",
            "allow_schema_evolution": True,
            "env": "prod",
            "pipeline_name": "my_pipeline",
        }
        cfg = UpsertConfig.from_dict(d)
        assert cfg.source_table == "src"
        assert cfg.merge_keys == ["id"]
        assert cfg.delete_indicator_col == "op"
        assert cfg.allow_schema_evolution is True
        assert cfg.env == "prod"

    def test_from_dict_defaults_optional_fields(self):
        cfg = UpsertConfig.from_dict({"source_table": "s", "target_table": "t", "merge_keys": ["id"]})
        assert cfg.update_columns == []
        assert cfg.content_hash_col == "_content_hash"

    def test_resolve_update_columns_explicit(self, config_selective):
        cols = config_selective.resolve_update_columns(["customer_id", "name", "email", "tier"])
        assert cols == ["email", "tier"]

    def test_resolve_update_columns_defaults_to_non_key(self, config):
        cols = config.resolve_update_columns(["customer_id", "name", "email", "tier"])
        assert "customer_id" not in cols
        assert set(cols) == {"name", "email", "tier"}

    def test_composite_merge_keys(self):
        cfg = UpsertConfig(
            source_table="s", target_table="t", merge_keys=["order_id", "line_id"]
        )
        assert len(cfg.merge_keys) == 2


# ---------------------------------------------------------------------------
# build_basic_merge_sql
# ---------------------------------------------------------------------------

class TestBuildBasicMergeSql:

    def test_starts_with_merge_into(self, config):
        sql = build_basic_merge_sql(config)
        assert sql.upper().startswith("MERGE INTO")

    def test_contains_target_table(self, config):
        sql = build_basic_merge_sql(config)
        assert config.target_table in sql

    def test_contains_source_table(self, config):
        sql = build_basic_merge_sql(config)
        assert config.source_table in sql

    def test_merge_key_in_on_clause(self, config):
        sql = build_basic_merge_sql(config)
        assert "customer_id" in sql

    def test_update_and_insert_present(self, config):
        sql = build_basic_merge_sql(config)
        assert "UPDATE SET" in sql.upper()
        assert "INSERT" in sql.upper()

    def test_star_wildcard_used(self, config):
        sql = build_basic_merge_sql(config)
        assert "UPDATE SET *" in sql or "UPDATE SET*" in sql.replace(" ", "")
        assert "INSERT *" in sql or "INSERT*" in sql.replace(" ", "")

    def test_composite_key_joined_with_and(self):
        cfg = UpsertConfig(
            source_table="s", target_table="t", merge_keys=["order_id", "line_id"]
        )
        sql = build_basic_merge_sql(cfg)
        assert "t.order_id = s.order_id" in sql
        assert "t.line_id = s.line_id" in sql
        assert "AND" in sql.upper()

    def test_aliases_used_in_on_clause(self, config):
        sql = build_basic_merge_sql(config)
        assert "t.customer_id" in sql
        assert "s.customer_id" in sql


# ---------------------------------------------------------------------------
# build_selective_update_sql
# ---------------------------------------------------------------------------

class TestBuildSelectiveUpdateSql:

    def test_explicit_set_columns(self, config_selective):
        sql = build_selective_update_sql(config_selective, ["email", "tier"])
        assert "t.email = s.email" in sql
        assert "t.tier = s.tier" in sql

    def test_no_star_in_update(self, config_selective):
        sql = build_selective_update_sql(config_selective, ["email"])
        upper = sql.upper()
        assert "UPDATE SET *" not in upper

    def test_insert_still_uses_star(self, config_selective):
        sql = build_selective_update_sql(config_selective, ["email"])
        assert "INSERT *" in sql

    def test_merge_key_not_in_set_clause(self, config_selective):
        sql = build_selective_update_sql(config_selective, ["email", "tier"])
        lines = [l.strip() for l in sql.splitlines() if "=" in l and "ON" not in l.upper()]
        set_lines = [l for l in lines if "UPDATE" not in l.upper() and "MERGE" not in l.upper()]
        for line in set_lines:
            assert "customer_id" not in line.lower() or "t.customer_id = s.customer_id" not in line
