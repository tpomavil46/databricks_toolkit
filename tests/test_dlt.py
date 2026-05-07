"""Tests for dlt.expectations, dlt.pipeline_config, dlt.table_sql — no Spark required."""

from __future__ import annotations

import pytest

from dlt.expectations import (
    VALID_MODES,
    build_expect,
    build_expect_or_drop,
    build_expect_or_fail,
    build_expect_all,
    build_not_null_expectation,
    build_range_expectation,
    build_accepted_values_expectation,
    build_regex_expectation,
    expectations_to_decorator_args,
)
from dlt.pipeline_config import (
    build_pipeline_settings,
    build_pipeline_cluster,
    build_notification_config,
    build_notebook_library,
    build_file_library,
)
from dlt.table_sql import (
    build_constraint_clause,
    build_streaming_table_sql,
    build_live_view_sql,
    build_materialized_view_sql,
    build_autoloader_source_expr,
    build_stream_read_expr,
)


# ---------------------------------------------------------------------------
# dlt.expectations
# ---------------------------------------------------------------------------

class TestBuildExpect:
    def test_mode_is_expect(self):
        e = build_expect("id_not_null", "id IS NOT NULL")
        assert e["mode"] == "expect"

    def test_name_and_condition(self):
        e = build_expect("my_check", "col > 0")
        assert e["name"] == "my_check"
        assert e["condition"] == "col > 0"


class TestBuildExpectOrDrop:
    def test_mode(self):
        e = build_expect_or_drop("n", "c")
        assert e["mode"] == "expect_or_drop"


class TestBuildExpectOrFail:
    def test_mode(self):
        e = build_expect_or_fail("n", "c")
        assert e["mode"] == "expect_or_fail"


class TestBuildExpectAll:
    def test_mode(self):
        ea = build_expect_all({"id_not_null": "id IS NOT NULL"})
        assert ea["mode"] == "expect_all"

    def test_expectations_preserved(self):
        ea = build_expect_all({"a": "a IS NOT NULL", "b": "b > 0"})
        assert ea["expectations"]["a"] == "a IS NOT NULL"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            build_expect_all({})


class TestBuildNotNullExpectation:
    def test_condition(self):
        e = build_not_null_expectation("customer_id")
        assert "customer_id IS NOT NULL" in e["condition"]

    def test_name_includes_col(self):
        e = build_not_null_expectation("order_id")
        assert "order_id" in e["name"]

    def test_mode_default(self):
        e = build_not_null_expectation("col")
        assert e["mode"] == "expect_or_drop"

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            build_not_null_expectation("col", mode="invalid")


class TestBuildRangeExpectation:
    def test_min_condition(self):
        e = build_range_expectation("age", min_val=0)
        assert "age >= 0" in e["condition"]

    def test_max_condition(self):
        e = build_range_expectation("score", max_val=100)
        assert "score <= 100" in e["condition"]

    def test_both_bounds(self):
        e = build_range_expectation("age", min_val=0, max_val=120)
        assert "age >= 0" in e["condition"]
        assert "age <= 120" in e["condition"]

    def test_no_bounds_raises(self):
        with pytest.raises(ValueError):
            build_range_expectation("col")

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            build_range_expectation("col", min_val=0, mode="bad")


class TestBuildAcceptedValuesExpectation:
    def test_condition_uses_in(self):
        e = build_accepted_values_expectation("status", ["active", "inactive"])
        assert "IN" in e["condition"].upper()
        assert "'active'" in e["condition"]

    def test_empty_values_raises(self):
        with pytest.raises(ValueError):
            build_accepted_values_expectation("col", [])


class TestBuildRegexExpectation:
    def test_regexp_like_in_condition(self):
        e = build_regex_expectation("email", r".+@.+\..+")
        assert "regexp_like" in e["condition"].lower()

    def test_pattern_in_condition(self):
        e = build_regex_expectation("code", r"^\d{4}$")
        assert r"^\d{4}$" in e["condition"]


class TestExpectationsToDecoratorArgs:
    def test_returns_name_condition_map(self):
        exps = [build_expect("a", "a IS NOT NULL"), build_expect("b", "b > 0")]
        args = expectations_to_decorator_args(exps)
        assert args["a"] == "a IS NOT NULL"
        assert args["b"] == "b > 0"


# ---------------------------------------------------------------------------
# dlt.pipeline_config
# ---------------------------------------------------------------------------

class TestBuildPipelineSettings:
    def _libs(self):
        return [build_notebook_library("/Repos/me/pipeline")]

    def test_name_in_spec(self):
        spec = build_pipeline_settings("my_pipeline", self._libs())
        assert spec["name"] == "my_pipeline"

    def test_libraries_in_spec(self):
        spec = build_pipeline_settings("p", self._libs())
        assert len(spec["libraries"]) == 1

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            build_pipeline_settings("", self._libs())

    def test_invalid_channel_raises(self):
        with pytest.raises(ValueError):
            build_pipeline_settings("p", self._libs(), channel="STABLE")

    def test_both_catalog_and_target_raises(self):
        with pytest.raises(ValueError):
            build_pipeline_settings("p", self._libs(), catalog="cat", target="schema")

    def test_continuous_false_by_default(self):
        spec = build_pipeline_settings("p", self._libs())
        assert spec["continuous"] is False

    def test_serverless_in_spec(self):
        spec = build_pipeline_settings("p", self._libs(), serverless=True)
        assert spec.get("serverless") is True

    def test_photon_not_set_when_serverless(self):
        spec = build_pipeline_settings("p", self._libs(), serverless=True, photon=True)
        assert "photon" not in spec

    def test_catalog_in_spec(self):
        spec = build_pipeline_settings("p", self._libs(), catalog="my_catalog")
        assert spec["catalog"] == "my_catalog"

    def test_configuration_forwarded(self):
        spec = build_pipeline_settings("p", self._libs(), configuration={"foo": "bar"})
        assert spec["configuration"]["foo"] == "bar"


class TestBuildPipelineCluster:
    def test_fixed_workers(self):
        c = build_pipeline_cluster(num_workers=4)
        assert c["num_workers"] == 4
        assert "autoscale" not in c

    def test_autoscale(self):
        c = build_pipeline_cluster(min_workers=2, max_workers=8)
        assert "autoscale" in c
        assert c["autoscale"]["min_workers"] == 2

    def test_no_workers_raises(self):
        with pytest.raises(ValueError):
            build_pipeline_cluster()

    def test_label(self):
        c = build_pipeline_cluster(num_workers=2, label="maintenance")
        assert c["label"] == "maintenance"


class TestBuildNotificationConfig:
    def test_email_addresses_present(self):
        n = build_notification_config(["ops@example.com"])
        assert "ops@example.com" in n["email_recipients"]

    def test_empty_emails_raises(self):
        with pytest.raises(ValueError):
            build_notification_config([])

    def test_default_alerts(self):
        n = build_notification_config(["a@b.com"])
        assert "on-update-failure" in n["alerts"]

    def test_invalid_alert_raises(self):
        with pytest.raises(ValueError):
            build_notification_config(["a@b.com"], alerts=["bad-alert"])


class TestLibraryHelpers:
    def test_notebook_library(self):
        lib = build_notebook_library("/Repos/me/nb")
        assert lib["notebook"]["path"] == "/Repos/me/nb"

    def test_file_library(self):
        lib = build_file_library("/src/pipeline.py")
        assert lib["file"]["path"] == "/src/pipeline.py"


# ---------------------------------------------------------------------------
# dlt.table_sql
# ---------------------------------------------------------------------------

class TestBuildConstraintClause:
    def test_constraint_keyword(self):
        c = build_constraint_clause("id_not_null", "id IS NOT NULL")
        assert "CONSTRAINT id_not_null" in c

    def test_expect_keyword(self):
        c = build_constraint_clause("c", "x > 0")
        assert "EXPECT" in c.upper()

    def test_drop_row_violation(self):
        c = build_constraint_clause("c", "x > 0", on_violation="DROP ROW")
        assert "DROP ROW" in c

    def test_fail_violation(self):
        c = build_constraint_clause("c", "x > 0", on_violation="FAIL")
        assert "FAIL" in c

    def test_no_violation_clause_when_empty(self):
        c = build_constraint_clause("c", "x > 0", on_violation="")
        assert "ON VIOLATION" not in c

    def test_invalid_violation_raises(self):
        with pytest.raises(ValueError):
            build_constraint_clause("c", "x > 0", on_violation="WARN")


class TestBuildStreamingTableSql:
    def test_create_streaming_table(self):
        sql = build_streaming_table_sql("bronze_events", "* FROM cloud_files('/mnt/raw', 'json')")
        assert "CREATE OR REFRESH STREAMING TABLE bronze_events" in sql

    def test_as_select(self):
        sql = build_streaming_table_sql("t", "* FROM src")
        assert "AS SELECT" in sql

    def test_constraint_in_sql(self):
        c = build_constraint_clause("id_nn", "id IS NOT NULL", "DROP ROW")
        sql = build_streaming_table_sql("t", "* FROM src", constraints=[c])
        assert "id_nn" in sql

    def test_partition_clause(self):
        sql = build_streaming_table_sql("t", "* FROM src", partition_cols=["date"])
        assert "PARTITIONED BY" in sql

    def test_tblproperties(self):
        sql = build_streaming_table_sql("t", "* FROM src", tblproperties={"key": "val"})
        assert "TBLPROPERTIES" in sql

    def test_comment(self):
        sql = build_streaming_table_sql("t", "* FROM src", comment="raw events")
        assert "raw events" in sql


class TestBuildLiveViewSql:
    def test_create_live_view(self):
        sql = build_live_view_sql("silver_view", "* FROM STREAM(bronze_events)")
        assert "CREATE OR REFRESH LIVE VIEW silver_view" in sql

    def test_as_select(self):
        sql = build_live_view_sql("v", "* FROM t")
        assert "AS SELECT" in sql

    def test_comment(self):
        sql = build_live_view_sql("v", "* FROM t", comment="intermediate")
        assert "intermediate" in sql


class TestBuildMaterializedViewSql:
    def test_create_materialized_view(self):
        sql = build_materialized_view_sql("gold_agg", "date, SUM(amount) FROM silver GROUP BY date")
        assert "CREATE OR REFRESH MATERIALIZED VIEW gold_agg" in sql

    def test_constraint(self):
        c = build_constraint_clause("date_nn", "date IS NOT NULL", "FAIL")
        sql = build_materialized_view_sql("v", "* FROM t", constraints=[c])
        assert "date_nn" in sql

    def test_partition(self):
        sql = build_materialized_view_sql("v", "* FROM t", partition_cols=["report_date"])
        assert "PARTITIONED BY" in sql


class TestBuildAutoloaderSourceExpr:
    def test_cloud_files_expr(self):
        expr = build_autoloader_source_expr("/mnt/raw", "json")
        assert "cloud_files" in expr
        assert "/mnt/raw" in expr
        assert "json" in expr

    def test_schema_location_included(self):
        expr = build_autoloader_source_expr("/mnt/raw", "json", "/tmp/schema")
        assert "schemaLocation" in expr
        assert "/tmp/schema" in expr


class TestBuildStreamReadExpr:
    def test_stream_expr(self):
        expr = build_stream_read_expr("bronze_events")
        assert "STREAM(bronze_events)" == expr
