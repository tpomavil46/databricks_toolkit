"""
medallion — Bronze / Silver / Gold Pipeline Templates.

Implements the Databricks Medallion (multi-hop) architecture as reusable,
config-driven building blocks.  Each layer is a separate module so teams can
adopt one layer at a time and wire them together as needed.

Modules
-------
config          MedallionConfig dataclass — all layer tables + pipeline settings.
bronze_pyspark  Raw ingest functions: metadata tagging, streaming write helpers.
silver_pyspark  Cleansing and deduplication: drop bronze metadata, keep latest row.
gold_pyspark    Aggregation and idempotent overwrite patterns.
pipeline_sql    CREATE TABLE and MERGE SQL templates for each layer.
idempotency     Watermark table helpers for re-runnable batch pipelines.
"""
