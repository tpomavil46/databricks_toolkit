import os
import sys

# Add project root to sys.path so imports work when running this file directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from typing import List
from databricks.sdk import WorkspaceClient
from utils.logger import log_function_call


@log_function_call
def explore_dbfs_path(
    path: str = "dbfs:/databricks-datasets/",
    recursive: bool = False,
    files_only: bool = False,
    limit: int = None,
    offset: int = 0,
) -> List[str]:
    """
    Lists immediate or recursive DBFS contents using the Databricks SDK.

    Args:
        path (str): Starting DBFS path.
        recursive (bool): If True, descend into subdirectories.
        files_only (bool): If True, only return files (not directories).
        limit (int): Maximum number of paths to return (None for all).
        offset (int): Number of paths to skip (for pagination).

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

    # Apply pagination
    if offset > 0:
        discovered = discovered[offset:]

    if limit is not None:
        discovered = discovered[:limit]

    return discovered


def main():
    parser = argparse.ArgumentParser(description="Explore DBFS paths recursively.")
    parser.add_argument(
        "--path",
        type=str,
        default="dbfs:/databricks-datasets/",
        help="Root DBFS path to explore (default: dbfs:/databricks-datasets/)",
    )
    parser.add_argument(
        "--files-only", action="store_true", help="Only list files, exclude folders"
    )
    parser.add_argument(
        "--no-recursive", action="store_true", help="Disable recursive traversal"
    )
    parser.add_argument("--limit", type=int, help="Maximum number of paths to return")
    parser.add_argument(
        "--offset", type=int, default=0, help="Number of paths to skip (for pagination)"
    )

    args = parser.parse_args()

    paths = explore_dbfs_path(
        path=args.path,
        recursive=not args.no_recursive,
        files_only=args.files_only,
        limit=args.limit,
        offset=args.offset,
    )

    total_found = len(paths)
    limit_info = f" (showing {total_found}"
    if args.limit:
        limit_info += f" of max {args.limit}"
    if args.offset:
        limit_info += f", offset {args.offset}"
    limit_info += ")"

    print(f"\n✅ {total_found} paths discovered{limit_info}:")
    for p in paths:
        print(p)


if __name__ == "__main__":
    main()
