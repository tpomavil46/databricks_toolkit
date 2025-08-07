#!/usr/bin/env python3
"""
PySpark ETL Workflow Runner

This is the main entry point for the PySpark ETL workflow.
It provides a clean interface for running PySpark ETL pipelines.
"""

import sys
import argparse
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from workflows.pyspark_etl.pipelines.pipeline import Pipeline
from shared.utils.logger import log_function_call


@log_function_call
def run_pyspark_pipeline(pipeline_name: str, environment: str = "dev"):
    """
    Run a PySpark ETL pipeline for the specified pipeline.

    Args:
        pipeline_name (str): Name of the pipeline to run
        environment (str): Environment to run in (dev, staging, prod)
    """
    print(f"🚀 Starting PySpark ETL pipeline: {pipeline_name}")
    print(f"📋 Environment: {environment}")

    try:
        # Initialize the PySpark ETL pipeline
        pipeline = Pipeline(name=pipeline_name, environment=environment)

        # Run the pipeline
        pipeline.run()

        print(f"✅ PySpark ETL pipeline completed successfully: {pipeline_name}")

    except Exception as e:
        print(f"❌ Error running PySpark ETL pipeline: {e}")
        sys.exit(1)


def main():
    """Main entry point for PySpark ETL workflow."""
    parser = argparse.ArgumentParser(description="PySpark ETL Workflow Runner")
    parser.add_argument("pipeline", help="Pipeline name to run")
    parser.add_argument(
        "--environment",
        "-e",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment to run in",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("🔧 PYSPARK ETL WORKFLOW")
    print("=" * 60)

    run_pyspark_pipeline(args.pipeline, args.environment)


if __name__ == "__main__":
    main()
