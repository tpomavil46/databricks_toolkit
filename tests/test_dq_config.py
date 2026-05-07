"""Tests for data_quality.config — DQConfig, DQResult, assert_dq_results."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from data_quality.config import (
    DQConfig,
    DQResult,
    assert_dq_results,
    VALID_LAYERS,
)


# ---------------------------------------------------------------------------
# DQConfig validation
# ---------------------------------------------------------------------------

class TestDQConfigValidation:
    def test_valid_bronze(self):
        cfg = DQConfig(table_name="catalog.bronze.t", layer="bronze")
        assert cfg.layer == "bronze"

    def test_valid_silver(self):
        cfg = DQConfig(table_name="t", layer="silver")
        assert cfg.layer == "silver"

    def test_valid_gold(self):
        cfg = DQConfig(table_name="t", layer="gold")
        assert cfg.layer == "gold"

    def test_valid_raw(self):
        cfg = DQConfig(table_name="t", layer="raw")
        assert cfg.layer == "raw"

    def test_invalid_layer_raises(self):
        with pytest.raises(ValueError, match="layer"):
            DQConfig(table_name="t", layer="platinum")

    def test_empty_table_name_raises(self):
        with pytest.raises(ValueError, match="table_name"):
            DQConfig(table_name="", layer="bronze")

    def test_defaults(self):
        cfg = DQConfig(table_name="t", layer="silver")
        assert cfg.dq_log_table == ""
        assert cfg.fail_on_error is False
        assert cfg.quarantine_table == ""
        assert cfg.pipeline_name == ""
        assert cfg.env == "dev"

    def test_custom_fields(self):
        cfg = DQConfig(
            table_name="catalog.silver.orders",
            layer="silver",
            dq_log_table="catalog.audit.dq_log",
            fail_on_error=True,
            quarantine_table="catalog.quarantine.orders",
            pipeline_name="orders_pipeline",
            env="prod",
        )
        assert cfg.dq_log_table == "catalog.audit.dq_log"
        assert cfg.fail_on_error is True
        assert cfg.quarantine_table == "catalog.quarantine.orders"
        assert cfg.pipeline_name == "orders_pipeline"
        assert cfg.env == "prod"


# ---------------------------------------------------------------------------
# DQResult properties
# ---------------------------------------------------------------------------

class TestDQResult:
    def test_passed_when_zero_failures(self):
        r = DQResult(
            check_name="not_null",
            column="id",
            passed=True,
            failing_count=0,
            total_count=100,
        )
        assert r.passed is True

    def test_failure_rate_zero(self):
        r = DQResult("not_null", "id", True, 0, 100)
        assert r.failure_rate == 0.0

    def test_failure_rate_calculation(self):
        r = DQResult("not_null", "id", False, 10, 100)
        assert r.failure_rate == pytest.approx(0.1)

    def test_failure_rate_all_failing(self):
        r = DQResult("not_null", "id", False, 50, 50)
        assert r.failure_rate == pytest.approx(1.0)

    def test_failure_rate_zero_total(self):
        r = DQResult("not_null", "id", False, 0, 0)
        assert r.failure_rate == 0.0

    def test_details_default_empty(self):
        r = DQResult("not_null", "id", True, 0, 100)
        assert r.details == ""

    def test_details_present(self):
        r = DQResult("not_null", "id", False, 5, 100, details="5 nulls")
        assert r.details == "5 nulls"


# ---------------------------------------------------------------------------
# DQResult.to_dict
# ---------------------------------------------------------------------------

class TestDQResultToDict:
    def _make_config(self):
        return DQConfig(
            table_name="catalog.silver.events",
            layer="silver",
            pipeline_name="events_pipeline",
            env="prod",
        )

    def test_to_dict_keys(self):
        cfg = self._make_config()
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        r = DQResult("not_null", "event_id", True, 0, 500)
        d = r.to_dict(cfg, ts)
        expected_keys = {
            "run_ts", "table_name", "layer", "pipeline_name",
            "check_name", "column_name", "passed",
            "failing_count", "total_count", "failure_rate",
            "details", "env",
        }
        assert set(d.keys()) == expected_keys

    def test_to_dict_values(self):
        cfg = self._make_config()
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        r = DQResult("unique", "order_id", False, 3, 100, details="3 dupes")
        d = r.to_dict(cfg, ts)
        assert d["check_name"] == "unique"
        assert d["column_name"] == "order_id"
        assert d["passed"] is False
        assert d["failing_count"] == 3
        assert d["total_count"] == 100
        assert d["failure_rate"] == pytest.approx(0.03)
        assert d["details"] == "3 dupes"
        assert d["table_name"] == "catalog.silver.events"
        assert d["layer"] == "silver"
        assert d["pipeline_name"] == "events_pipeline"
        assert d["env"] == "prod"
        assert d["run_ts"] == ts

    def test_to_dict_defaults_run_ts(self):
        cfg = self._make_config()
        r = DQResult("not_null", "id", True, 0, 10)
        d = r.to_dict(cfg)
        assert d["run_ts"] is not None

    def test_to_dict_no_config(self):
        r = DQResult("not_null", "id", True, 0, 10)
        d = r.to_dict()
        assert "table_name" not in d
        assert "layer" not in d
        assert "pipeline_name" not in d
        assert "env" not in d
        assert d["check_name"] == "not_null"


# ---------------------------------------------------------------------------
# results_to_rows
# ---------------------------------------------------------------------------

class TestResultsToRows:
    def test_returns_one_row_per_result(self):
        from data_quality.logging_pyspark import results_to_rows
        cfg = DQConfig(table_name="t", layer="bronze")
        results = [
            DQResult("not_null", "a", True, 0, 10),
            DQResult("unique", "b", False, 2, 10),
        ]
        rows = results_to_rows(results, cfg)
        assert len(rows) == 2

    def test_row_is_dict(self):
        from data_quality.logging_pyspark import results_to_rows
        cfg = DQConfig(table_name="t", layer="bronze")
        rows = results_to_rows([DQResult("not_null", "x", True, 0, 5)], cfg)
        assert isinstance(rows[0], dict)

    def test_shared_run_ts(self):
        from data_quality.logging_pyspark import results_to_rows
        cfg = DQConfig(table_name="t", layer="bronze")
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        results = [
            DQResult("not_null", "a", True, 0, 5),
            DQResult("unique", "b", True, 0, 5),
        ]
        rows = results_to_rows(results, cfg, run_ts=ts)
        assert all(r["run_ts"] == ts for r in rows)

    def test_empty_results(self):
        from data_quality.logging_pyspark import results_to_rows
        cfg = DQConfig(table_name="t", layer="bronze")
        assert results_to_rows([], cfg) == []


# ---------------------------------------------------------------------------
# assert_dq_results
# ---------------------------------------------------------------------------

class TestAssertDQResults:
    def _passing(self):
        return DQResult("not_null", "id", True, 0, 100)

    def _failing(self):
        return DQResult("not_null", "id", False, 5, 100, details="5 nulls")

    def test_no_raise_when_fail_on_error_false(self):
        cfg = DQConfig(table_name="t", layer="bronze", fail_on_error=False)
        assert_dq_results([self._failing()], cfg)

    def test_no_raise_when_all_pass(self):
        cfg = DQConfig(table_name="t", layer="bronze", fail_on_error=True)
        assert_dq_results([self._passing()], cfg)

    def test_raises_when_any_fail(self):
        cfg = DQConfig(table_name="t", layer="bronze", fail_on_error=True)
        with pytest.raises(ValueError):
            assert_dq_results([self._failing()], cfg)

    def test_raises_mentions_table_name(self):
        cfg = DQConfig(table_name="catalog.silver.orders", layer="silver", fail_on_error=True)
        with pytest.raises(ValueError, match="catalog.silver.orders"):
            assert_dq_results([self._failing()], cfg)

    def test_raises_with_mixed_results(self):
        cfg = DQConfig(table_name="t", layer="bronze", fail_on_error=True)
        with pytest.raises(ValueError):
            assert_dq_results([self._passing(), self._failing()], cfg)

    def test_empty_results_no_raise(self):
        cfg = DQConfig(table_name="t", layer="bronze", fail_on_error=True)
        assert_dq_results([], cfg)


# ---------------------------------------------------------------------------
# VALID_LAYERS constant
# ---------------------------------------------------------------------------

class TestValidLayers:
    def test_contains_expected_layers(self):
        assert "bronze" in VALID_LAYERS
        assert "silver" in VALID_LAYERS
        assert "gold" in VALID_LAYERS
        assert "raw" in VALID_LAYERS
