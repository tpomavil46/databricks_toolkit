# Databricks Toolkit

A reference implementation of Databricks Data Engineering patterns. Every concept is implemented in two ways: **PySpark DataFrame API** (`_pyspark.py`) and **Spark SQL string generation** (`_sql.py`). All modules are fully tested and config-driven with no hardcoded table or path names.

---

## Modules

| # | Package | What it covers |
|---|---------|---------------|
| 1 | `scd/` | Slowly Changing Dimensions Types 0–4 and 6 |
| 2 | `upserts/` | Delta MERGE patterns: basic, delete-aware, idempotent, late-arriving, schema evolution |
| 3 | `streaming/` | Structured Streaming readers/writers, triggers, foreachBatch, stream-static joins, stateful aggregations |
| 4 | `delta_features/` | CDF, time travel, OPTIMIZE/ZORDER, VACUUM, deletion vectors, table properties |
| 5 | `data_quality/` | DQ checks (not-null, unique, range, regex, referential integrity, freshness), quarantine pattern, DQResult dataclass |
| 6 | `medallion/` | Bronze/Silver/Gold layer transforms, watermark idempotency, replaceWhere Gold writes |
| 7 | `optimization/` | Partitioning (Hive/Liquid/ZORDER), small-file management, bloom filters, join hints, skew handling |
| 8 | `orchestration/` | Databricks Jobs API spec builders, DABs (Databricks Asset Bundles) YAML generation |
| 9 | `dlt/` | Delta Live Tables expectations, pipeline config, streaming table / live view / materialized view SQL |
| 10 | `governance/` | Unity Catalog GRANT/REVOKE, row filters, column masks, audit log queries |
| 11 | `performance/` | AQE tuning SQL, per-workload Spark config bundles, DataFrame caching utilities |

---

## Project Structure

```
databricks_toolkit/
├── scd/                        # Module 1 — SCD Types 0-4, 6
│   ├── type{0,1,2,3,4,6}_pyspark.py
│   ├── type{0,1,2,3,4,6}_sql.py
│   ├── surrogate_key.py
│   └── config.py
├── upserts/                    # Module 2 — Delta MERGE patterns
│   ├── basic_merge_{pyspark,sql}.py
│   ├── delete_aware_{pyspark,sql}.py
│   ├── idempotent_{pyspark,sql}.py
│   ├── late_arriving_{pyspark,sql}.py
│   ├── schema_evolution_{pyspark,sql}.py
│   └── config.py
├── streaming/                  # Module 3 — Structured Streaming
│   ├── readers_pyspark.py
│   ├── writers_pyspark.py
│   ├── triggers.py
│   ├── foreach_batch_{pyspark,sql}.py
│   ├── stream_static_join_pyspark.py
│   ├── stateful_pyspark.py
│   ├── checkpoint.py
│   └── config.py
├── delta_features/             # Module 4 — Delta Lake features
│   ├── cdf_{pyspark,sql}.py
│   ├── time_travel_{pyspark,sql}.py
│   ├── optimize_sql.py
│   ├── vacuum_sql.py
│   ├── deletion_vectors.py
│   └── table_properties.py
├── data_quality/               # Module 5 — DQ checks and quarantine
│   ├── checks_{pyspark,sql}.py
│   ├── quarantine_{pyspark,sql}.py
│   ├── logging_pyspark.py
│   └── config.py
├── medallion/                  # Module 6 — Medallion architecture
│   ├── bronze_pyspark.py
│   ├── silver_pyspark.py
│   ├── gold_pyspark.py
│   ├── pipeline_sql.py
│   ├── idempotency.py
│   └── config.py
├── optimization/               # Module 7 — Query and storage optimization
│   ├── partitioning_sql.py
│   ├── small_files_sql.py
│   ├── bloom_filters_sql.py
│   ├── join_sql.py
│   └── skew_pyspark.py
├── orchestration/              # Module 8 — Jobs and DABs
│   ├── job_spec.py
│   └── dabs_yaml.py
├── dlt/                        # Module 9 — Delta Live Tables
│   ├── expectations.py
│   ├── pipeline_config.py
│   └── table_sql.py
├── governance/                 # Module 10 — Unity Catalog governance
│   ├── grants_sql.py
│   ├── row_filter_sql.py
│   ├── column_mask_sql.py
│   └── audit_sql.py
├── performance/                # Module 11 — Performance tuning
│   ├── aqe_sql.py
│   ├── config.py
│   └── cache_pyspark.py
├── great_expectations/suites/  # GE expectation stubs (one per layer)
├── tests/                      # Pytest suite — 821 passing locally
└── ingestion/                  # Bronze ingestion library (singleplex + multiplex)
```

---

## Running Tests

```bash
# All tests — Spark-dependent tests skip gracefully when not on a cluster
PYTHONPATH=. pytest tests/ -q

# Only no-Spark tests (fast, ~1s)
PYTHONPATH=. pytest tests/ -q -k "not spark"

# Specific module
PYTHONPATH=. pytest tests/test_scd_type2.py -v
```

**Test split:**

- **No-Spark tests** — pure Python / SQL string generation, run always, ~1 s total
- **Spark tests** — `pytest.importorskip("pyspark")` at top of file + `spark` fixture from `conftest.py`; skip locally when `databricks-connect` is not authenticated; run on a cluster via integration test runner

---

## Design Patterns

### Dual PySpark / SQL

Each concept ships two implementations:

```python
# PySpark — DataFrame API
from scd.type2_pyspark import apply_scd2

updated = apply_scd2(spark, source_df, config)
```

```python
# SQL — returns executable SQL string
from scd.type2_sql import build_scd2_merge_sql

sql = build_scd2_merge_sql(config)
spark.sql(sql)
```

### Config-driven

No hardcoded table or path names. Every module has a `Config` dataclass:

```python
from scd.config import SCDConfig

cfg = SCDConfig(
    source_table="catalog.bronze.orders",
    target_table="catalog.silver.orders_scd2",
    business_key=["order_id"],
    tracked_cols=["status", "amount"],
)
```

### foreachBatch factory

Streaming transforms use a factory pattern so config is pre-bound:

```python
from medallion.bronze_pyspark import make_bronze_batch_fn

bronze_fn = make_bronze_batch_fn(config)
stream.writeStream.foreachBatch(bronze_fn).start()
```

### DQ quarantine

Rows are tagged in-place; good and bad are then split:

```python
from data_quality.quarantine_pyspark import tag_with_all_failures, split_good_bad

tagged = tag_with_all_failures(df, checks)
good_df, bad_df = split_good_bad(tagged)
```

---

## Great Expectations Suites

Stub suites in `great_expectations/suites/` cover every layer:

| Suite | Layer |
|-------|-------|
| `bronze_suite.json` | Raw ingestion |
| `silver_suite.json` | Cleansed / conformed |
| `gold_suite.json` | Aggregated / serving |
| `medallion_suite.json` | Medallion pipeline |
| `scd_type2_suite.json` | SCD Type 2 |
| `scd_type4_suite.json` | SCD Type 4 |
| `upserts_suite.json` | MERGE operations |
| `streaming_suite.json` | Streaming output |
| `delta_features_suite.json` | Delta table health |
| `optimization_suite.json` | File size / layout |

---

## Requirements

```
databricks-connect
pyspark
delta-spark
great-expectations
pytest
```

Install: `pip install -r requirements.txt`
