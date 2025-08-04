#!/usr/bin/env python3
"""
Drop Table CLI - Drop tables to avoid schema conflicts.
"""

import os
import sys
import argparse

# Add project root to sys.path so imports work
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks.connect import DatabricksSession
from utils.logger import log_function_call


@log_function_call
def drop_table(table_name: str):
    """
    Drop a table if it exists.
    
    Args:
        table_name: Name of the table to drop
    """
    print(f"🗑️  Dropping table: {table_name}")
    
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")
        .getOrCreate()
    )
    
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✅ Successfully dropped table: {table_name}")
    except Exception as e:
        print(f"❌ Error dropping table: {str(e)}")


def main():
    """CLI entry point for dropping tables."""
    parser = argparse.ArgumentParser(description="Drop a table if it exists.")
    parser.add_argument(
        "--table-name",
        type=str,
        required=True,
        help="Name of the table to drop"
    )
    
    args = parser.parse_args()
    
    drop_table(args.table_name)


if __name__ == "__main__":
    main() 