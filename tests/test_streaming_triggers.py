"""
tests/test_streaming_triggers.py — Unit tests for trigger helpers.

Tests cover:
- describe_trigger: human-readable description for each mode.
- build_trigger_kwargs: correct keyword argument dict per mode.
- validate_trigger_mode: raises on invalid mode.
- VALID_TRIGGER_MODES coverage.

No Spark required — all tests run locally.
"""

import pytest
from streaming.config import StreamingConfig, VALID_TRIGGER_MODES
from streaming.triggers import build_trigger_kwargs, describe_trigger, validate_trigger_mode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(trigger_mode, trigger_interval="30 seconds"):
    return StreamingConfig(
        source_path="s", target_table="t",
        checkpoint_base="dbfs:/cp", pipeline_name="p",
        trigger_mode=trigger_mode,
        trigger_interval=trigger_interval,
    )


# ---------------------------------------------------------------------------
# describe_trigger
# ---------------------------------------------------------------------------

class TestDescribeTrigger:

    def test_micro_batch_mentions_interval(self):
        cfg = make_config("micro_batch", "1 minute")
        desc = describe_trigger(cfg)
        assert "1 minute" in desc

    def test_available_now_mentions_stop(self):
        desc = describe_trigger(make_config("available_now"))
        assert "stop" in desc.lower() or "available" in desc.lower()

    def test_once_deprecated_mentioned(self):
        desc = describe_trigger(make_config("once"))
        assert "once" in desc.lower() or "single" in desc.lower()

    def test_continuous_mentions_interval(self):
        cfg = make_config("continuous", "1 second")
        desc = describe_trigger(cfg)
        assert "1 second" in desc

    def test_returns_string_for_all_modes(self):
        for mode in VALID_TRIGGER_MODES:
            desc = describe_trigger(make_config(mode))
            assert isinstance(desc, str)
            assert len(desc) > 0


# ---------------------------------------------------------------------------
# build_trigger_kwargs
# ---------------------------------------------------------------------------

class TestBuildTriggerKwargs:

    def test_micro_batch_returns_processing_time(self):
        kwargs = build_trigger_kwargs(make_config("micro_batch", "30 seconds"))
        assert "processingTime" in kwargs
        assert kwargs["processingTime"] == "30 seconds"

    def test_available_now_returns_available_now_true(self):
        kwargs = build_trigger_kwargs(make_config("available_now"))
        assert kwargs == {"availableNow": True}

    def test_once_returns_once_true(self):
        kwargs = build_trigger_kwargs(make_config("once"))
        assert kwargs == {"once": True}

    def test_continuous_returns_continuous_with_interval(self):
        kwargs = build_trigger_kwargs(make_config("continuous", "1 second"))
        assert "continuous" in kwargs
        assert kwargs["continuous"] == "1 second"

    def test_each_mode_returns_exactly_one_key(self):
        for mode in VALID_TRIGGER_MODES:
            kwargs = build_trigger_kwargs(make_config(mode))
            assert len(kwargs) == 1

    def test_micro_batch_interval_propagated_correctly(self):
        kwargs = build_trigger_kwargs(make_config("micro_batch", "5 minutes"))
        assert kwargs["processingTime"] == "5 minutes"

    def test_kwargs_are_dict(self):
        for mode in VALID_TRIGGER_MODES:
            kwargs = build_trigger_kwargs(make_config(mode))
            assert isinstance(kwargs, dict)


# ---------------------------------------------------------------------------
# validate_trigger_mode
# ---------------------------------------------------------------------------

class TestValidateTriggerMode:

    def test_valid_modes_do_not_raise(self):
        for mode in VALID_TRIGGER_MODES:
            validate_trigger_mode(mode)

    def test_invalid_mode_raises_value_error(self):
        with pytest.raises(ValueError, match="trigger_mode"):
            validate_trigger_mode("cron")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            validate_trigger_mode("")

    def test_case_sensitive(self):
        with pytest.raises(ValueError):
            validate_trigger_mode("MICRO_BATCH")


# ---------------------------------------------------------------------------
# VALID_TRIGGER_MODES coverage
# ---------------------------------------------------------------------------

class TestValidTriggerModes:

    def test_contains_micro_batch(self):
        assert "micro_batch" in VALID_TRIGGER_MODES

    def test_contains_available_now(self):
        assert "available_now" in VALID_TRIGGER_MODES

    def test_contains_once(self):
        assert "once" in VALID_TRIGGER_MODES

    def test_contains_continuous(self):
        assert "continuous" in VALID_TRIGGER_MODES

    def test_is_a_set(self):
        assert isinstance(VALID_TRIGGER_MODES, (set, frozenset))
