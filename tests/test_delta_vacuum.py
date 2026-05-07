"""
tests/test_delta_vacuum.py — Unit tests for VACUUM helpers.

Tests cover:
- validate_retention_hours: rejects below minimum, accepts None and safe values.
- build_vacuum_sql: plain, RETAIN, DRY RUN, combinations.
- build_disable_retention_check_sql: contains the config key.
- build_set_retention_property_sql: ALTER TABLE SQL with interval notation.

No Spark required — all tests run locally.
"""

import pytest
from delta_features.vacuum_sql import (
    MINIMUM_RETENTION_HOURS,
    build_disable_retention_check_sql,
    build_enable_retention_check_sql,
    build_set_retention_property_sql,
    build_vacuum_sql,
    validate_retention_hours,
)

TABLE = "catalog.gold.dim_customer"


# ---------------------------------------------------------------------------
# validate_retention_hours
# ---------------------------------------------------------------------------

class TestValidateRetentionHours:

    def test_none_is_safe(self):
        validate_retention_hours(None)

    def test_exactly_minimum_is_safe(self):
        validate_retention_hours(MINIMUM_RETENTION_HOURS)

    def test_above_minimum_is_safe(self):
        validate_retention_hours(MINIMUM_RETENTION_HOURS + 1)
        validate_retention_hours(720)

    def test_below_minimum_raises(self):
        with pytest.raises(ValueError, match=str(MINIMUM_RETENTION_HOURS)):
            validate_retention_hours(MINIMUM_RETENTION_HOURS - 1)

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            validate_retention_hours(0)

    def test_one_hour_raises(self):
        with pytest.raises(ValueError):
            validate_retention_hours(1)

    def test_error_message_contains_override_hint(self):
        with pytest.raises(ValueError, match="retentionDurationCheck"):
            validate_retention_hours(1)

    def test_minimum_retention_hours_constant(self):
        assert MINIMUM_RETENTION_HOURS == 168


# ---------------------------------------------------------------------------
# build_vacuum_sql
# ---------------------------------------------------------------------------

class TestBuildVacuumSql:

    def test_starts_with_vacuum(self):
        sql = build_vacuum_sql(TABLE)
        assert sql.upper().startswith("VACUUM")

    def test_contains_table_name(self):
        sql = build_vacuum_sql(TABLE)
        assert TABLE in sql

    def test_no_retain_by_default(self):
        sql = build_vacuum_sql(TABLE)
        assert "RETAIN" not in sql.upper()

    def test_no_dry_run_by_default(self):
        sql = build_vacuum_sql(TABLE)
        assert "DRY RUN" not in sql.upper()

    def test_retain_hours_appended(self):
        sql = build_vacuum_sql(TABLE, retain_hours=336)
        assert "RETAIN 336 HOURS" in sql

    def test_dry_run_appended(self):
        sql = build_vacuum_sql(TABLE, dry_run=True)
        assert "DRY RUN" in sql.upper()

    def test_retain_and_dry_run_combined(self):
        sql = build_vacuum_sql(TABLE, retain_hours=168, dry_run=True)
        assert "RETAIN 168 HOURS" in sql
        assert "DRY RUN" in sql.upper()

    def test_retain_before_dry_run(self):
        sql = build_vacuum_sql(TABLE, retain_hours=168, dry_run=True)
        retain_pos = sql.upper().index("RETAIN")
        dry_run_pos = sql.upper().index("DRY")
        assert retain_pos < dry_run_pos

    def test_below_minimum_raises(self):
        with pytest.raises(ValueError):
            build_vacuum_sql(TABLE, retain_hours=1)

    def test_minimum_retention_accepted(self):
        sql = build_vacuum_sql(TABLE, retain_hours=MINIMUM_RETENTION_HOURS)
        assert f"RETAIN {MINIMUM_RETENTION_HOURS} HOURS" in sql

    def test_float_retention(self):
        sql = build_vacuum_sql(TABLE, retain_hours=168.0)
        assert "168.0" in sql or "168" in sql


# ---------------------------------------------------------------------------
# Retention check SET statements
# ---------------------------------------------------------------------------

class TestRetentionCheckSql:

    def test_disable_contains_config_key(self):
        sql = build_disable_retention_check_sql()
        assert "retentionDurationCheck" in sql

    def test_disable_sets_to_false(self):
        sql = build_disable_retention_check_sql()
        assert "false" in sql.lower()

    def test_disable_starts_with_set(self):
        sql = build_disable_retention_check_sql()
        assert sql.upper().startswith("SET")

    def test_enable_sets_to_true(self):
        sql = build_enable_retention_check_sql()
        assert "true" in sql.lower()

    def test_enable_contains_config_key(self):
        sql = build_enable_retention_check_sql()
        assert "retentionDurationCheck" in sql


# ---------------------------------------------------------------------------
# build_set_retention_property_sql
# ---------------------------------------------------------------------------

class TestBuildSetRetentionPropertySql:

    def test_contains_alter_table(self):
        sql = build_set_retention_property_sql(TABLE, retain_hours=720)
        assert "ALTER TABLE" in sql.upper()

    def test_contains_table_name(self):
        sql = build_set_retention_property_sql(TABLE, retain_hours=720)
        assert TABLE in sql

    def test_contains_retention_property_key(self):
        sql = build_set_retention_property_sql(TABLE, retain_hours=720)
        assert "deletedFileRetentionDuration" in sql

    def test_contains_interval(self):
        sql = build_set_retention_property_sql(TABLE, retain_hours=720)
        assert "interval" in sql.lower()

    def test_below_minimum_raises(self):
        with pytest.raises(ValueError):
            build_set_retention_property_sql(TABLE, retain_hours=1)
