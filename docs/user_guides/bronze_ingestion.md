# 🥉 Bronze Ingestion Layer

## ✅ Purpose
The Bronze layer ingests raw datasets from external sources (e.g., DBFS, cloud buckets), applies a **common schema**, **adds metadata** (e.g., source label), and optionally **normalizes column names** using configurable mappings.

---

## 📂 Files Involved

| File                             | Purpose                                                                 |
|----------------------------------|-------------------------------------------------------------------------|
| `jobs/bronze/ingest.py`          | Main ingestion logic — reads files, applies schema, optionally normalizes |
| `utils/structs.py`               | Contains dataset-specific `get_column_mapping()`                         |
| `utils/session.py`               | Initializes Spark session via `DatabricksSession`                        |
| `utils/io.py`                    | Handles writing output to path or Delta table                            |
| `utils/schema_normalizer.py`    | Implements `auto_normalize_columns()` for schema unification             |
| `tests/test_ingest.py`          | Pytest-based validation of ingest output                                 |

---

## ⚙️ Function Signature

```
def ingest_data(
    spark: SparkSession,
    dataset: str,
    input_paths: dict,
    bronze_output: str,
    column_mapping: dict,
    format: str = "delta",
    normalize: bool = True,
) -> DataFrame:
```

### Arguments:
- `spark`: Active Spark session (from `utils.session`).
- `dataset`: High-level dataset name (e.g., `"nyctaxi"`).
- `input_paths`: Dictionary mapping logical names (e.g., `"yellow"`) to file paths.
- `bronze_output`: Table name or path to write.
- `column_mapping`: Dict of raw → clean column names for each dataset.
- `format`: Storage format. Default is `"delta"`.
- `normalize`: Whether to auto-rename columns to unified schema.

---

## ✅ Usage Examples

### 📥 Ingest and Normalize:
```python
from utils.structs import get_column_mapping
from jobs.bronze.ingest import ingest_data

column_mapping = {
    "yellow": get_column_mapping("yellow"),
    "green": get_column_mapping("green")
}

df = ingest_data(
    spark=spark,
    dataset="nyctaxi",
    input_paths={
        "yellow": "dbfs:/.../yellow_tripdata.csv.gz",
        "green": "dbfs:/.../green_tripdata.csv.gz"
    },
    bronze_output="main.default.nytaxi_bronze",
    column_mapping=column_mapping,
    format="delta",
    normalize=True
)
```

### 🪪 Ingest Only (no renaming):
```python
df = ingest_data(
    spark,
    dataset="nyctaxi",
    input_paths=...,
    bronze_output=...,
    column_mapping=column_mapping,
    normalize=False
)
```

---

## 🧪 Test Coverage

Test file: `tests/test_ingest.py`

- Validates ingestion for NYC Taxi (yellow + green)
- Verifies column presence and row count
- Logs SparkSession startup and ingested file info
- Uses real DBFS paths

---

## ✅ Status

- ✅ Fully working with `pytest`
- ✅ Compatible with Databricks Connect
- ✅ Modular and dataset-agnostic
- ✅ Prepares normalized and raw bronze layers
