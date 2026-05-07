"""
tests/test_streaming_config.py — Unit tests for StreamingConfig.

Tests cover:
- Validation: required fields, trigger_mode enum, output_mode enum.
- checkpoint_path / schema_location properties.
- from_dict: round-trip and defaults.

No Spark required — all tests run locally.
"""

import pytest
from streaming.config import StreamingConfig, VALID_TRIGGER_MODES, VALID_OUTPUT_MODES


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return StreamingConfig(
        source_path="catalog.bronze.events",
        target_table="catalog.silver.events",
        checkpoint_base="dbfs:/checkpoints",
        pipeline_name="events_pipeline",
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestStreamingConfigValidation:

    def test_empty_source_path_raises(self):
        with pytest.raises(ValueError, match="source_path"):
            StreamingConfig(
                source_path="", target_table="t",
                checkpoint_base="dbfs:/cp", pipeline_name="p",
            )

    def test_empty_target_table_raises(self):
        with pytest.raises(ValueError, match="target_table"):
            StreamingConfig(
                source_path="s", target_table="",
                checkpoint_base="dbfs:/cp", pipeline_name="p",
            )

    def test_empty_checkpoint_base_raises(self):
        with pytest.raises(ValueError, match="checkpoint_base"):
            StreamingConfig(
                source_path="s", target_table="t",
                checkpoint_base="", pipeline_name="p",
            )

    def test_empty_pipeline_name_raises(self):
        with pytest.raises(ValueError, match="pipeline_name"):
            StreamingConfig(
                source_path="s", target_table="t",
                checkpoint_base="dbfs:/cp", pipeline_name="",
            )

    def test_invalid_trigger_mode_raises(self):
        with pytest.raises(ValueError, match="trigger_mode"):
            StreamingConfig(
                source_path="s", target_table="t",
                checkpoint_base="dbfs:/cp", pipeline_name="p",
                trigger_mode="every_5_seconds",
            )

    def test_invalid_output_mode_raises(self):
        with pytest.raises(ValueError, match="output_mode"):
            StreamingConfig(
                source_path="s", target_table="t",
                checkpoint_base="dbfs:/cp", pipeline_name="p",
                output_mode="replace",
            )

    def test_all_valid_trigger_modes_accepted(self):
        for mode in VALID_TRIGGER_MODES:
            cfg = StreamingConfig(
                source_path="s", target_table="t",
                checkpoint_base="dbfs:/cp", pipeline_name="p",
                trigger_mode=mode,
            )
            assert cfg.trigger_mode == mode

    def test_all_valid_output_modes_accepted(self):
        for mode in VALID_OUTPUT_MODES:
            cfg = StreamingConfig(
                source_path="s", target_table="t",
                checkpoint_base="dbfs:/cp", pipeline_name="p",
                output_mode=mode,
            )
            assert cfg.output_mode == mode


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

class TestStreamingConfigDefaults:

    def test_default_source_format(self, config):
        assert config.source_format == "delta"

    def test_default_trigger_mode(self, config):
        assert config.trigger_mode == "micro_batch"

    def test_default_trigger_interval(self, config):
        assert config.trigger_interval == "30 seconds"

    def test_default_output_mode(self, config):
        assert config.output_mode == "append"

    def test_default_max_files(self, config):
        assert config.max_files_per_trigger == 0

    def test_default_watermark_delay(self, config):
        assert config.watermark_delay == "10 minutes"

    def test_default_merge_keys_empty(self, config):
        assert config.merge_keys == []

    def test_default_env(self, config):
        assert config.env == "dev"


# ---------------------------------------------------------------------------
# checkpoint_path property
# ---------------------------------------------------------------------------

class TestCheckpointPath:

    def test_checkpoint_path_combines_base_and_name(self, config):
        assert config.checkpoint_path == "dbfs:/checkpoints/events_pipeline"

    def test_trailing_slash_stripped_from_base(self):
        cfg = StreamingConfig(
            source_path="s", target_table="t",
            checkpoint_base="dbfs:/checkpoints/",
            pipeline_name="my_pipeline",
        )
        assert not cfg.checkpoint_path.endswith("//my_pipeline")
        assert cfg.checkpoint_path == "dbfs:/checkpoints/my_pipeline"

    def test_checkpoint_path_includes_pipeline_name(self, config):
        assert "events_pipeline" in config.checkpoint_path

    def test_different_pipeline_names_produce_different_paths(self):
        def make(name):
            return StreamingConfig(
                source_path="s", target_table="t",
                checkpoint_base="dbfs:/cp", pipeline_name=name,
            )
        assert make("pipeline_a").checkpoint_path != make("pipeline_b").checkpoint_path


# ---------------------------------------------------------------------------
# schema_location property
# ---------------------------------------------------------------------------

class TestSchemaLocation:

    def test_defaults_to_checkpoint_subdir(self, config):
        assert config.schema_location == config.checkpoint_path + "/_schema"

    def test_explicit_schema_location_used_when_set(self):
        cfg = StreamingConfig(
            source_path="s", target_table="t",
            checkpoint_base="dbfs:/cp", pipeline_name="p",
            cloudfiles_schema_location="abfss://schemas@storage.dfs.core.windows.net/p",
        )
        assert cfg.schema_location == "abfss://schemas@storage.dfs.core.windows.net/p"


# ---------------------------------------------------------------------------
# from_dict
# ---------------------------------------------------------------------------

class TestFromDict:

    def test_round_trip(self):
        d = {
            "source_path": "catalog.bronze.events",
            "target_table": "catalog.silver.events",
            "checkpoint_base": "dbfs:/cp",
            "pipeline_name": "events",
            "source_format": "json",
            "trigger_mode": "available_now",
            "trigger_interval": "1 minute",
            "output_mode": "update",
            "max_files_per_trigger": 500,
            "cloudfiles_schema_location": "dbfs:/schemas/events",
            "watermark_col": "event_ts",
            "watermark_delay": "5 minutes",
            "merge_keys": ["event_id"],
            "env": "prod",
        }
        cfg = StreamingConfig.from_dict(d)
        assert cfg.source_path == "catalog.bronze.events"
        assert cfg.trigger_mode == "available_now"
        assert cfg.merge_keys == ["event_id"]
        assert cfg.env == "prod"

    def test_defaults_applied_from_dict(self):
        d = {
            "source_path": "s",
            "target_table": "t",
            "checkpoint_base": "dbfs:/cp",
            "pipeline_name": "p",
        }
        cfg = StreamingConfig.from_dict(d)
        assert cfg.source_format == "delta"
        assert cfg.trigger_mode == "micro_batch"
        assert cfg.output_mode == "append"
        assert cfg.merge_keys == []
