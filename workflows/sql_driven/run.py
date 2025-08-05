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
        # Initialize the SQL-driven pipeline
        pipeline = SQLDrivenPipeline(
            spark=None,  # Will be initialized by the pipeline
            sql_base_path="workflows/sql_driven/sql",
            project=project_name
        )
        
        # Run the pipeline
        pipeline.run()
        
        print(f"✅ SQL-driven pipeline completed successfully for {project_name}")
        
    except Exception as e:
        print(f"❌ Error running SQL-driven pipeline: {e}")
        sys.exit(1)


def main():
    """Main entry point for SQL-driven workflow."""
    parser = argparse.ArgumentParser(description="SQL-Driven Workflow Runner")
    parser.add_argument("project", help="Project name to run")
    parser.add_argument("--environment", "-e", default="dev", 
                       choices=["dev", "staging", "prod"],
                       help="Environment to run in")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔧 SQL-DRIVEN WORKFLOW")
    print("=" * 60)
    
    run_sql_pipeline(args.project, args.environment)


if __name__ == "__main__":
    main() 