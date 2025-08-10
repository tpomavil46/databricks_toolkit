#!/usr/bin/env python3
"""
SQL-Driven Workflow Runner

This is the main entry point for the SQL-driven workflow.
It provides a clean interface for running SQL-driven pipelines.
"""

import sys
import argparse
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from workflows.sql_driven.pipelines.sql_driven_pipeline import SQLDrivenPipeline
from workflows.sql_driven.pipelines.dlt_pipeline import DLTPipelineRunner
from shared.utils.logger import log_function_call


@log_function_call
def run_sql_pipeline(project_name: str, environment: str = "dev"):
    """
    Run a SQL-driven pipeline for the specified project.

    Args:
        project_name (str): Name of the project to run
        environment (str): Environment to run in (dev, staging, prod)
    """
    print(f"🚀 Starting SQL-driven pipeline for project: {project_name}")
    print(f"📋 Environment: {environment}")

    try:
        # Import here to avoid circular imports
        from databricks.connect import DatabricksSession

        # Create Spark session
        spark = DatabricksSession.builder.profile("databricks").getOrCreate()

        # Initialize the SQL-driven pipeline
        pipeline = SQLDrivenPipeline(
            spark=spark,
            sql_base_path="workflows/sql_driven/sql",
            project=project_name,
        )

        # Run the pipeline
        pipeline.run_full_pipeline(
            input_path="/tmp/test_data",
            bronze_table=f"{project_name}_bronze",
            silver_table=f"{project_name}_silver",
            gold_table=f"{project_name}_gold",
        )

        print(f"✅ SQL-driven pipeline completed successfully for {project_name}")

    except Exception as e:
        print(f"❌ Error running SQL-driven pipeline: {e}")
        sys.exit(1)


@log_function_call
def run_dlt_pipeline(project_name: str, source_path: str, environment: str = "dev"):
    """
    Run a DLT pipeline for the specified project.

    Args:
        project_name (str): Name of the project to run
        source_path (str): Source data path
        environment (str): Environment to run in (dev, staging, prod)
    """
    print(f"🚀 Starting DLT pipeline for project: {project_name}")
    print(f"📁 Source path: {source_path}")
    print(f"📋 Environment: {environment}")

    try:
        # Import here to avoid circular imports
        from databricks.connect import DatabricksSession

        # Create Spark session
        spark = DatabricksSession.builder.profile("databricks").getOrCreate()

        # Configuration
        config = {
            "sql_base_path": "workflows/sql_driven/sql",
            "environment": environment,
            "project": project_name,
        }

        # Create and run DLT pipeline
        runner = DLTPipelineRunner(spark, config)
        runner.run_complete_dlt_pipeline(project_name, source_path, environment)

        print(f"✅ DLT pipeline completed successfully for {project_name}")

    except Exception as e:
        print(f"❌ Error running DLT pipeline: {e}")
        sys.exit(1)


def main():
    """Main entry point for SQL-driven workflow."""
    parser = argparse.ArgumentParser(description="SQL-Driven Workflow Runner")
    parser.add_argument("project", help="Project name to run")
    parser.add_argument(
        "--environment",
        "-e",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment to run in",
    )
    parser.add_argument(
        "--pipeline-type",
        "-t",
        default="sql",
        choices=["sql", "dlt"],
        help="Pipeline type to run (sql or dlt)",
    )
    parser.add_argument(
        "--source-path", "-s", help="Source data path (required for DLT pipelines)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("🔧 SQL-DRIVEN WORKFLOW")
    print("=" * 60)

    if args.pipeline_type == "dlt":
        if not args.source_path:
            print("❌ Error: --source-path is required for DLT pipelines")
            sys.exit(1)
        run_dlt_pipeline(args.project, args.source_path, args.environment)
    else:
        run_sql_pipeline(args.project, args.environment)


if __name__ == "__main__":
    main()
