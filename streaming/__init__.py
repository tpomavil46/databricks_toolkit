"""
streaming/ — Structured Streaming with Delta Lake.

Modules
-------
config          StreamingConfig dataclass.
triggers        Trigger mode factory and descriptions.
checkpoint      Checkpoint path helpers.
readers_pyspark Delta, Auto Loader, and generic file stream readers.
writers_pyspark Delta writeStream with trigger and output mode wiring.
foreach_batch_pyspark  foreachBatch factories: upsert, SCD2, idempotent.
foreach_batch_sql      SQL-based foreachBatch factories (temp view + MERGE).
stateful_pyspark       Watermarks, dropDuplicates, window aggregations.
stream_static_join_pyspark  Enrichment joins against static dimension tables.
bronze_silver_pipeline Composable Bronze → Silver pipeline templates.
"""
