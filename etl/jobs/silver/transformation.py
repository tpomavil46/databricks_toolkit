"""
Standardized Silver Transformation Job

This module provides a standardized silver transformation job that can be
parameterized and reused across different projects.
"""

from typing import Dict, Any, Optional
from utils.logger import log_function_call
from etl.core.etl_pipeline import StandardETLPipeline
from etl.core.config import PipelineConfig


class SilverTransformationJob:
    """
    Standardized silver transformation job with parameterized configuration.
    """

    def __init__(self, project_name: str, environment: str = "dev"):
        """
        Initialize the silver transformation job.

        Args:
            project_name: Name of the project
            environment: Environment (dev, staging, prod)
        """
        self.config = PipelineConfig(project_name=project_name)
        self.config.table_config.environment = environment
        self.pipeline = StandardETLPipeline(self.config)

    @log_function_call
    def run(
        self,
        source_table: str,
        table_name: str = None,
        transformations: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Run silver transformation job.

        Args:
            source_table: Source table name (bronze table)
            table_name: Name for the silver table (if None, auto-generate)
            transformations: Transformation configuration

        Returns:
            Dictionary containing transformation results
        """
        print(f"⚙️  Silver Transformation Job")
        print(f"📁 Project: {self.config.project_name}")
        print(f"🌍 Environment: {self.config.table_config.environment}")
        print("=" * 60)

        try:
            results = self.pipeline.run_silver_transformation(
                source_table, table_name, transformations
            )
            print(f"✅ Silver transformation job completed successfully")
            return results
        except Exception as e:
            print(f"❌ Silver transformation job failed: {str(e)}")
            raise

    @log_function_call
    def run_with_validation(
        self,
        source_table: str,
        table_name: str = None,
        transformations: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Run silver transformation job with enhanced validation.

        Args:
            source_table: Source table name (bronze table)
            table_name: Name for the silver table (if None, auto-generate)
            transformations: Transformation configuration

        Returns:
            Dictionary containing transformation results with validation
        """
        results = self.run(source_table, table_name, transformations)

        # Add validation summary
        validation_summary = self.pipeline.validator.get_validation_summary()
        results["validation_summary"] = validation_summary

        return results


def run_silver_transformation_job(
    project_name: str,
    source_table: str,
    environment: str = "dev",
    table_name: str = None,
    transformations: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Convenience function to run silver transformation job.

    Args:
        project_name: Name of the project
        source_table: Source table name (bronze table)
        environment: Environment (dev, staging, prod)
        table_name: Name for the silver table (if None, auto-generate)
        transformations: Transformation configuration

    Returns:
        Dictionary containing transformation results
    """
    job = SilverTransformationJob(project_name, environment)
    return job.run(source_table, table_name, transformations)
