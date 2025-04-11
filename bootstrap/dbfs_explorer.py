import os
import sys

# Add project root to sys.path so imports work when running this file directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from typing import List
from databricks.sdk import WorkspaceClient
from utils.logger import log_function_call


@log_function_call
def explore_dbfs_path(path: str = "dbfs:/databricks-datasets/", recursive: bool = False, files_only: bool = False) -> List[str]:
    """
    Lists immediate or recursive DBFS contents using the Databricks SDK.

    Args:
        path (str): Starting DBFS path.
        recursive (bool): If True, descend into subdirectories.
        files_only (bool): If True, only return files (not directories).

    Returns:
        List[str]: List of file or directory paths.
    """
    w = WorkspaceClient()
    discovered = []

    def list_dir(current_path: str, depth: int = 0):
        try:
            entries = w.dbfs.list(path=current_path)
            for entry in entries:
                if files_only and entry.is_dir:
                    continue

                discovered.append(entry.path)

                if recursive and entry.is_dir:
                    list_dir(entry.path, depth + 1)

        except Exception as e:
            print(f"⚠️ Error accessing {current_path}: {e}")

    list_dir(path)
    return discovered


def main():
    parser = argparse.ArgumentParser(description="Explore DBFS paths recursively.")
    parser.add_argument(
        "--path",
        type=str,
        default="dbfs:/databricks-datasets/",
        help="Root DBFS path to explore (default: dbfs:/databricks-datasets/)"
    )
    parser.add_argument(
        "--files-only",
        action="store_true",
        help="Only list files, exclude folders"
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Disable recursive traversal"
    )

    args = parser.parse_args()

    paths = explore_dbfs_path(
        path=args.path,
        recursive=not args.no_recursive,
        files_only=args.files_only,
    )

    print(f"\n✅ {len(paths)} paths discovered.")
    for p in paths:
        print(p)


if __name__ == "__main__":
    main()
