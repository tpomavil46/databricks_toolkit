"""
Standardized Gold Aggregation Job

This module provides a standardized gold aggregation job that can be
parameterized and reused across different projects.
"""

from typing import Dict, Any, Optional
from utils.logger import log_function_call
from etl.core.etl_pipeline import StandardETLPipeline
from etl.core.config import PipelineConfig


class GoldAggregationJob:
    """
    Standardized gold aggregation job with parameterized configuration.
    """

    def __init__(self, project_name: str, environment: str = "dev"):
        """
        Initialize the gold aggregation job.

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
        aggregations: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Run gold aggregation job.

        Args:
            source_table: Source table name (silver table)
            table_name: Name for the gold table (if None, auto-generate)
            aggregations: Aggregation configuration

        Returns:
            Dictionary containing aggregation results
        """
        print(f"🏆 Gold Aggregation Job")
        print(f"📁 Project: {self.config.project_name}")
        print(f"🌍 Environment: {self.config.table_config.environment}")
        print("=" * 60)

        try:
            results = self.pipeline.run_gold_aggregation(
                source_table, table_name, aggregations
            )
            print(f"✅ Gold aggregation job completed successfully")
            return results
        except Exception as e:
            print(f"❌ Gold aggregation job failed: {str(e)}")
            raise

    @log_function_call
    def run_with_validation(
        self,
        source_table: str,
        table_name: str = None,
        aggregations: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Run gold aggregation job with enhanced validation.

        Args:
            source_table: Source table name (silver table)
            table_name: Name for the gold table (if None, auto-generate)
            aggregations: Aggregation configuration

        Returns:
            Dictionary containing aggregation results with validation
        """
        results = self.run(source_table, table_name, aggregations)

        # Add validation summary
        validation_summary = self.pipeline.validator.get_validation_summary()
        results["validation_summary"] = validation_summary

        return results


def run_gold_aggregation_job(
    project_name: str,
    source_table: str,
    environment: str = "dev",
    table_name: str = None,
    aggregations: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Convenience function to run gold aggregation job.

    Args:
        project_name: Name of the project
        source_table: Source table name (silver table)
        environment: Environment (dev, staging, prod)
        table_name: Name for the gold table (if None, auto-generate)
        aggregations: Aggregation configuration

    Returns:
        Dictionary containing aggregation results
    """
    job = GoldAggregationJob(project_name, environment)
    return job.run(source_table, table_name, aggregations)
