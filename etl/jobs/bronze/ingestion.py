"""
Standardized Bronze Ingestion Job

This module provides a standardized bronze ingestion job that can be
parameterized and reused across different projects.
"""

from typing import Dict, Any, Optional
from utils.logger import log_function_call
from etl.core.etl_pipeline import StandardETLPipeline
from etl.core.config import PipelineConfig


class BronzeIngestionJob:
    """
    Standardized bronze ingestion job with parameterized configuration.
    """

    def __init__(self, project_name: str, environment: str = "dev"):
        """
        Initialize the bronze ingestion job.

        Args:
            project_name: Name of the project
            environment: Environment (dev, staging, prod)
        """
        self.config = PipelineConfig(project_name=project_name)
        self.config.table_config.environment = environment
        self.pipeline = StandardETLPipeline(self.config)

    @log_function_call
    def run(
        self, source_path: str, table_name: str = None, auto_drop: bool = False
    ) -> Dict[str, Any]:
        """
        Run bronze ingestion job.

        Args:
            source_path: Path to source data
            table_name: Name for the bronze table (if None, auto-generate)
            auto_drop: Whether to automatically drop existing tables

        Returns:
            Dictionary containing ingestion results
        """
        print(f"🛢️  Bronze Ingestion Job")
        print(f"📁 Project: {self.config.project_name}")
        print(f"🌍 Environment: {self.config.table_config.environment}")
        print("=" * 60)

        try:
            results = self.pipeline.run_bronze_ingestion(
                source_path, table_name, auto_drop
            )
            print(f"✅ Bronze ingestion job completed successfully")
            return results
        except Exception as e:
            print(f"❌ Bronze ingestion job failed: {str(e)}")
            raise

    @log_function_call
    def run_with_validation(
        self, source_path: str, table_name: str = None, auto_drop: bool = False
    ) -> Dict[str, Any]:
        """
        Run bronze ingestion job with enhanced validation.

        Args:
            source_path: Path to source data
            table_name: Name for the bronze table (if None, auto-generate)
            auto_drop: Whether to automatically drop existing tables

        Returns:
            Dictionary containing ingestion results with validation
        """
        results = self.run(source_path, table_name, auto_drop)

        # Add validation summary
        validation_summary = self.pipeline.validator.get_validation_summary()
        results["validation_summary"] = validation_summary

        return results


def run_bronze_ingestion_job(
    project_name: str,
    source_path: str,
    environment: str = "dev",
    table_name: str = None,
    auto_drop: bool = False,
) -> Dict[str, Any]:
    """
    Convenience function to run bronze ingestion job.

    Args:
        project_name: Name of the project
        source_path: Path to source data
        environment: Environment (dev, staging, prod)
        table_name: Name for the bronze table (if None, auto-generate)
        auto_drop: Whether to automatically drop existing tables

    Returns:
        Dictionary containing ingestion results
    """
    job = BronzeIngestionJob(project_name, environment)
    return job.run(source_path, table_name, auto_drop)
