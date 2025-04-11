
---

### 📄 `cli/COMMANDS.md`

```markdown
# Databricks Toolkit CLI Commands

This document outlines the CLI tools available in the `cli/` directory for the Databricks Toolkit.

---

## CLI Entry Points

### `dbfs_cli.py`

**Description**:  
Command-line wrapper around `explore_dbfs_path()` to inspect DBFS directories.

**Usage**:
```bash
python cli/dbfs_cli.py --path dbfs:/some/path --files-only --no-recursive
```

### `query_file.py`

**Description**:
Preview a file from DBFS using Spark SQL with format-specific logic.

**Usage**:
```python
python cli/query_file.py --file /databricks-datasets/path/file.csv --format csv --limit 10
```

**Options**:
	--file: File path to read (absolute DBFS path).
	--format: Format of the file (csv, parquet, delta, etc.).
	--limit: Number of rows to preview (default: 10).

**Examples**:
```python
python cli/query_file.py --file /databricks-datasets/wine-quality/winequality-red.csv --format csv --limit 5
```