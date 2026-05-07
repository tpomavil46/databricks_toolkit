"""Tests for medallion.config — no Spark required."""

from __future__ import annotations

import pytest

from medallion.config import MedallionConfig, BRONZE_METADATA_COLS


class TestMedallionConfigValidation:
    BASE = dict(
        source_path="s3://bucket/raw",
        bronze_table="cat.bronze.events",
        silver_table="cat.silver.events",
        gold_table="cat.gold.events",
        checkpoint_base="dbfs:/checkpoints",
        pipeline_name="events_pipeline",
    )

    def _make(self, **overrides):
        return MedallionConfig(**{**self.BASE, **overrides})

    def test_valid_config(self):
        cfg = self._make()
        assert cfg.pipeline_name == "events_pipeline"

    def test_empty_source_path_raises(self):
        with pytest.raises(ValueError, match="source_path"):
            self._make(source_path="")

    def test_empty_bronze_table_raises(self):
        with pytest.raises(ValueError, match="bronze_table"):
            self._make(bronze_table="")

    def test_empty_silver_table_raises(self):
        with pytest.raises(ValueError, match="silver_table"):
            self._make(silver_table="")

    def test_empty_gold_table_raises(self):
        with pytest.raises(ValueError, match="gold_table"):
            self._make(gold_table="")

    def test_empty_checkpoint_base_raises(self):
        with pytest.raises(ValueError, match="checkpoint_base"):
            self._make(checkpoint_base="")

    def test_empty_pipeline_name_raises(self):
        with pytest.raises(ValueError, match="pipeline_name"):
            self._make(pipeline_name="")

    def test_defaults(self):
        cfg = self._make()
        assert cfg.source_format == "json"
        assert cfg.id_col == ""
        assert cfg.ts_col == ""
        assert cfg.partition_col == ""
        assert cfg.watermark_table == ""
        assert cfg.env == "dev"


class TestMedallionConfigCheckpointPaths:
    BASE = dict(
        source_path="s3://bucket/raw",
        bronze_table="b",
        silver_table="s",
        gold_table="g",
        checkpoint_base="dbfs:/checkpoints",
        pipeline_name="orders",
    )

    def _make(self, **overrides):
        return MedallionConfig(**{**self.BASE, **overrides})

    def test_bronze_checkpoint(self):
        cfg = self._make()
        assert cfg.bronze_checkpoint == "dbfs:/checkpoints/orders/bronze"

    def test_silver_checkpoint(self):
        cfg = self._make()
        assert cfg.silver_checkpoint == "dbfs:/checkpoints/orders/silver"

    def test_schema_location(self):
        cfg = self._make()
        assert cfg.schema_location == "dbfs:/checkpoints/orders/bronze/_schema"

    def test_trailing_slash_stripped(self):
        cfg = self._make(checkpoint_base="dbfs:/checkpoints/")
        assert cfg.bronze_checkpoint == "dbfs:/checkpoints/orders/bronze"

    def test_pipeline_name_in_path(self):
        cfg = self._make(pipeline_name="my_pipeline")
        assert "my_pipeline" in cfg.bronze_checkpoint
        assert "my_pipeline" in cfg.silver_checkpoint


class TestBronzeMetadataCols:
    def test_contains_ingest_ts(self):
        assert "_ingest_ts" in BRONZE_METADATA_COLS

    def test_contains_batch_id(self):
        assert "_batch_id" in BRONZE_METADATA_COLS

    def test_contains_source_file(self):
        assert "_source_file" in BRONZE_METADATA_COLS

    def test_is_list(self):
        assert isinstance(BRONZE_METADATA_COLS, list)
