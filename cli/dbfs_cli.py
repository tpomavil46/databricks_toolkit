#!/usr/bin/env python

import os
import sys
import argparse

# Add project root to sys.path so imports work
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from bootstrap.dbfs_explorer import explore_dbfs_path


def main():
    """CLI entrypoint for recursively exploring a DBFS path."""
    parser = argparse.ArgumentParser(description="Explore DBFS paths recursively.")
    parser.add_argument(
        "--path",
        type=str,
        default="dbfs:/databricks-datasets/",
        help="Root DBFS path to explore (default: dbfs:/databricks-datasets/)",
    )
    parser.add_argument(
        "--files-only",
        action="store_true",
        help="Only list files, exclude folders.",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Disable recursive traversal.",
    )

    args = parser.parse_args()

    if not args.path.startswith("dbfs:/"):
        print("❌ Error: DBFS path must start with 'dbfs:/'")
        sys.exit(1)

    try:
        paths = explore_dbfs_path(
            path=args.path,
            recursive=not args.no_recursive,
            files_only=args.files_only,
        )
    except Exception as e:
        print(f"❌ Failed to explore DBFS path: {e}")
        sys.exit(1)

    if paths:
        print(f"\n✅ {len(paths)} paths discovered:\n")
        for p in paths:
            print(p)
    else:
        print(f"\n⚠️ No paths found under: {args.path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
