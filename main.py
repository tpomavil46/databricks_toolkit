#!/usr/bin/env python3
"""
Databricks Toolkit - Main Entry Point

This is the unified entry point for the Databricks Toolkit.
It provides access to both SQL-driven and PySpark ETL workflows.
"""

import sys
import argparse
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def run_sql_workflow(project: str, environment: str = "dev"):
    """Run the SQL-driven workflow."""
    from workflows.sql_driven.run import run_sql_pipeline

    run_sql_pipeline(project, environment)


def run_pyspark_workflow(pipeline: str, environment: str = "dev"):
    """Run the PySpark ETL workflow."""
    from workflows.pyspark_etl.run import run_pyspark_pipeline

    run_pyspark_pipeline(pipeline, environment)


def list_workflows():
    """List available workflows and their components."""
    print("=" * 60)
    print("🔧 DATABRICKS TOOLKIT - AVAILABLE WORKFLOWS")
    print("=" * 60)

    print("\n📊 SQL-DRIVEN WORKFLOW")
    print("   Purpose: SQL-first data processing")
    print("   Usage: python main.py sql <project> [--environment]")
    print("   Examples:")
    print("     python main.py sql retail")
    print("     python main.py sql ecommerce --environment prod")

    print("\n🔧 PYSPARK ETL WORKFLOW")
    print("   Purpose: Python-first ETL processing")
    print("   Usage: python main.py pyspark <pipeline> [--environment]")
    print("   Examples:")
    print("     python main.py pyspark data_ingestion")
    print("     python main.py pyspark transformation --environment staging")

    print("\n📚 SHARED COMPONENTS")
    print("   CLI Tools: python -m shared.cli")
    print("   Admin Tools: python -m shared.admin")
    print("   SQL Library: python -m shared.sql_library")


def main():
    """Main entry point with workflow selection."""
    parser = argparse.ArgumentParser(
        description="Databricks Toolkit - Unified Entry Point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py sql retail                    # Run SQL workflow for retail project
  python main.py pyspark data_ingestion       # Run PySpark ETL workflow
  python main.py list                         # List available workflows
        """,
    )

    parser.add_argument(
        "workflow",
        nargs="?",
        choices=["sql", "pyspark", "list"],
        help="Workflow to run (sql, pyspark, or list)",
    )
    parser.add_argument("target", nargs="?", help="Project or pipeline name")
    parser.add_argument(
        "--environment",
        "-e",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment to run in",
    )

    args = parser.parse_args()

    if args.workflow == "list" or not args.workflow:
        list_workflows()
        return

    if not args.target:
        print("❌ Error: Please specify a project/pipeline name")
        print("   Example: python main.py sql retail")
        sys.exit(1)

    print("=" * 60)
    print("🚀 DATABRICKS TOOLKIT")
    print("=" * 60)

    try:
        if args.workflow == "sql":
            run_sql_workflow(args.target, args.environment)
        elif args.workflow == "pyspark":
            run_pyspark_workflow(args.target, args.environment)
        else:
            print(f"❌ Unknown workflow: {args.workflow}")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error running workflow: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
