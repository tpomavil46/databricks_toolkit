"""
tests/test_delta_optimize.py — Unit tests for OPTIMIZE, ZORDER, Liquid Clustering.

Tests cover:
- build_optimize_sql: plain, ZORDER, WHERE, combinations.
- build_liquid_clustering_sql: column count validation, SQL structure.
- build_disable_liquid_clustering_sql: CLUSTER BY NONE.
- build_analyze_sql: with and without column list.

No Spark required — all tests run locally.
"""

import pytest
from delta_features.optimize_sql import (
    build_analyze_sql,
    build_disable_liquid_clustering_sql,
    build_liquid_clustering_sql,
    build_optimize_sql,
)

TABLE = "catalog.gold.fact_orders"


# ---------------------------------------------------------------------------
# build_optimize_sql
# ---------------------------------------------------------------------------

class TestBuildOptimizeSql:

    def test_starts_with_optimize(self):
        sql = build_optimize_sql(TABLE)
        assert sql.upper().startswith("OPTIMIZE")

    def test_contains_table_name(self):
        sql = build_optimize_sql(TABLE)
        assert TABLE in sql

    def test_plain_optimize_no_zorder(self):
        sql = build_optimize_sql(TABLE)
        assert "ZORDER" not in sql.upper()

    def test_plain_optimize_no_where(self):
        sql = build_optimize_sql(TABLE)
        assert "WHERE" not in sql.upper()

    def test_zorder_single_col(self):
        sql = build_optimize_sql(TABLE, zorder_cols=["customer_id"])
        assert "ZORDER BY" in sql.upper()
        assert "customer_id" in sql

    def test_zorder_multiple_cols(self):
        sql = build_optimize_sql(TABLE, zorder_cols=["customer_id", "order_date"])
        assert "customer_id" in sql
        assert "order_date" in sql

    def test_zorder_cols_in_parentheses(self):
        sql = build_optimize_sql(TABLE, zorder_cols=["customer_id"])
        assert "(customer_id)" in sql

    def test_where_clause_inserted(self):
        sql = build_optimize_sql(TABLE, where_clause="date = '2024-01-15'")
        assert "WHERE" in sql.upper()
        assert "date = '2024-01-15'" in sql

    def test_where_before_zorder(self):
        sql = build_optimize_sql(TABLE, zorder_cols=["id"], where_clause="date > '2024-01-01'")
        where_pos = sql.upper().index("WHERE")
        zorder_pos = sql.upper().index("ZORDER")
        assert where_pos < zorder_pos

    def test_zorder_without_where(self):
        sql = build_optimize_sql(TABLE, zorder_cols=["id"])
        assert "WHERE" not in sql.upper()
        assert "ZORDER BY" in sql.upper()

    def test_where_without_zorder(self):
        sql = build_optimize_sql(TABLE, where_clause="partition_date = '2024-01-15'")
        assert "ZORDER" not in sql.upper()
        assert "WHERE" in sql.upper()


# ---------------------------------------------------------------------------
# build_liquid_clustering_sql
# ---------------------------------------------------------------------------

class TestBuildLiquidClusteringSql:

    def test_contains_cluster_by(self):
        sql = build_liquid_clustering_sql(TABLE, ["customer_id"])
        assert "CLUSTER BY" in sql.upper()

    def test_contains_table_name(self):
        sql = build_liquid_clustering_sql(TABLE, ["customer_id"])
        assert TABLE in sql

    def test_contains_alter_table(self):
        sql = build_liquid_clustering_sql(TABLE, ["customer_id"])
        assert "ALTER TABLE" in sql.upper()

    def test_single_cluster_col(self):
        sql = build_liquid_clustering_sql(TABLE, ["customer_id"])
        assert "customer_id" in sql

    def test_multiple_cluster_cols(self):
        sql = build_liquid_clustering_sql(TABLE, ["customer_id", "order_date"])
        assert "customer_id" in sql
        assert "order_date" in sql

    def test_max_four_cols_allowed(self):
        sql = build_liquid_clustering_sql(TABLE, ["a", "b", "c", "d"])
        assert sql is not None

    def test_five_cols_raises(self):
        with pytest.raises(ValueError, match="4"):
            build_liquid_clustering_sql(TABLE, ["a", "b", "c", "d", "e"])

    def test_empty_cols_raises(self):
        with pytest.raises(ValueError):
            build_liquid_clustering_sql(TABLE, [])

    def test_cols_in_parentheses(self):
        sql = build_liquid_clustering_sql(TABLE, ["customer_id"])
        assert "(customer_id)" in sql


# ---------------------------------------------------------------------------
# build_disable_liquid_clustering_sql
# ---------------------------------------------------------------------------

class TestBuildDisableLiquidClusteringSql:

    def test_contains_cluster_by_none(self):
        sql = build_disable_liquid_clustering_sql(TABLE)
        assert "CLUSTER BY NONE" in sql.upper()

    def test_contains_table_name(self):
        sql = build_disable_liquid_clustering_sql(TABLE)
        assert TABLE in sql

    def test_contains_alter_table(self):
        sql = build_disable_liquid_clustering_sql(TABLE)
        assert "ALTER TABLE" in sql.upper()


# ---------------------------------------------------------------------------
# build_analyze_sql
# ---------------------------------------------------------------------------

class TestBuildAnalyzeSql:

    def test_contains_analyze_table(self):
        sql = build_analyze_sql(TABLE)
        assert "ANALYZE TABLE" in sql.upper()

    def test_contains_compute_statistics(self):
        sql = build_analyze_sql(TABLE)
        assert "COMPUTE STATISTICS" in sql.upper()

    def test_no_for_columns_by_default(self):
        sql = build_analyze_sql(TABLE)
        assert "FOR COLUMNS" not in sql.upper()

    def test_for_columns_when_cols_provided(self):
        sql = build_analyze_sql(TABLE, cols=["customer_id", "order_date"])
        assert "FOR COLUMNS" in sql.upper()
        assert "customer_id" in sql
        assert "order_date" in sql

    def test_contains_table_name(self):
        sql = build_analyze_sql(TABLE)
        assert TABLE in sql
