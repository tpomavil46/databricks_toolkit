"""
SQL-Driven Main Entry Point

This demonstrates how to use SQL files as arguments to make Python code
reusable and generalized while maintaining loose coupling between SQL and Python.

Usage:
    python main_sql_driven.py --input_table /path/to/data --bronze_path bronze_table --silver_path silver_table --gold_path gold_table
"""

import argparse
import os
from databricks.connect import DatabricksSession
from pipelines.sql_driven_pipeline import run_sql_pipeline
from dotenv import load_dotenv
from utils.logger import log_function_call
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env for DATABRICKS_PROFILE and DATABRICKS_CLUSTER_ID
load_dotenv()


@log_function_call
def main():
    """
    Main entry point for SQL-driven pipeline.
    
    This demonstrates how to:
    - Accept SQL files as arguments to Python code
    - Keep SQL and Python separated
    - Make Python code reusable and generalized
    - Maintain loose coupling between components
    """
    parser = argparse.ArgumentParser(
        description="Run SQL-driven Medallion pipeline with loose coupling"
    )

    # Pipeline configuration
    parser.add_argument(
        "--input_table",
        type=str,
        required=True,
        help="Input path or table (CSV, JSON, etc.)",
    )
    parser.add_argument(
        "--bronze_path", 
        type=str, 
        required=True, 
        help="Target Bronze table name"
    )
    parser.add_argument(
        "--silver_path", 
        type=str, 
        required=True, 
        help="Target Silver table name"
    )
    parser.add_argument(
        "--gold_path", 
        type=str, 
        required=True, 
        help="Target Gold table name"
    )
    
    # Optional parameters
    parser.add_argument(
        "--format", 
        type=str, 
        default="delta", 
        help="Storage format (default: delta)"
    )
    parser.add_argument(
        "--vendor_filter",
        type=int,
        default=None,
        help="Optional vendor ID filter for data processing",
    )
    parser.add_argument(
        "--sql_base_path",
        type=str,
        default="sql",
        help="Base path for SQL files (default: sql)",
    )

    args = parser.parse_args()

    # Create Spark session via Databricks Connect
    # Using the same pattern as your etl_example.py
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")  # Your current cluster ID from etl_example.py
        .getOrCreate()
    )

    print("🔧 SQL-Driven Pipeline Configuration:")
    print(f"   Input: {args.input_table}")
    print(f"   Bronze: {args.bronze_path}")
    print(f"   Silver: {args.silver_path}")
    print(f"   Gold: {args.gold_path}")
    print(f"   Format: {args.format}")
    print(f"   Vendor Filter: {args.vendor_filter}")
    print(f"   SQL Base Path: {args.sql_base_path}")
    print()

    # Execute SQL-driven pipeline
    # This demonstrates how SQL files are passed as arguments to make Python code reusable
    results = run_sql_pipeline(
        spark=spark,
        input_table=args.input_table,
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        gold_path=args.gold_path,
        format=args.format,
        vendor_filter=args.vendor_filter
    )

    # Report results
    if results['success']:
        print(f"✅ Pipeline completed successfully!")
        print(f"   Steps completed: {', '.join(results['steps_completed'])}")
    else:
        print(f"❌ Pipeline failed!")
        for error in results['errors']:
            print(f"   Error: {error}")


if __name__ == "__main__":
    main() 