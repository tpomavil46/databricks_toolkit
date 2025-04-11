
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

Options:
	--path: DBFS path to explore.
	--files-only: Only return file paths.
	--no-recursive: Skip recursive traversal of subdirectories.