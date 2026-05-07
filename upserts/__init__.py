"""
upserts — MERGE-based upsert and CDC patterns for Delta Lake.

Covers the full spectrum of MERGE INTO variations used in production
Databricks pipelines.  Each pattern is implemented in PySpark and Spark SQL.

Patterns
--------
basic_merge         INSERT new rows, UPDATE existing rows.  The baseline.
delete_aware        INSERT / UPDATE / DELETE from a single CDC source.
idempotent          Deduplication before MERGE + content-hash change guard.
                    Makes upsert pipelines safe to re-run without side effects.
late_arriving       Recency guards and deduplication for out-of-order data.
schema_evolution    Detect and apply schema drift before MERGE so new source
                    columns are automatically added to the target table.

Entry points
------------
from upserts.basic_merge_pyspark import apply_basic_merge
from upserts.delete_aware_sql import build_delete_aware_merge_sql
from upserts.idempotent_pyspark import deduplicate_source, add_content_hash
from upserts.late_arriving_pyspark import pick_latest_per_key
from upserts.schema_evolution_pyspark import detect_schema_drift
from upserts.config import UpsertConfig
"""
