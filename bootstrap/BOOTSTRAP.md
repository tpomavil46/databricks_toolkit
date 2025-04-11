# Databricks Toolkit: Bootstrap Modules

This document outlines the foundational utilities in the `bootstrap/` directory that support core functionality in the Databricks Toolkit.

---

## Modules

### `dbfs_explorer.py`

**Purpose**:  
Recursively explore and list contents of DBFS using the Databricks SDK.

**Function**:
```python
explore_dbfs_path(
    path: str = "dbfs:/databricks-datasets/",
    recursive: bool = False,
    files_only: bool = False
) -> List[str]
```

Arguments:
    file_path: Path to the file in DBFS.
	file_format: File format to read with (e.g., csv, parquet, json).
	limit: Number of rows to return from the file.

Returns:
	A Spark DataFrame with the previewed content.

Notes:
	CSV files are read with .option("header", True) if format is csv.

---

preview_cli.py

Description:
Preview a file from DBFS using Spark SQL with format-specific logic.

Usage:
```python
python cli/preview_cli.py --file /databricks-datasets/path/file.csv --format csv --limit 10
```

Options:
	--file: File path to read (absolute DBFS path).
	--format: Format of the file (csv, parquet, delta, etc.).
	--limit: Number of rows to preview (default: 10).

Examples:
```python
python cli/preview_cli.py --file /databricks-datasets/wine-quality/winequality-red.csv --format csv --limit 5
```