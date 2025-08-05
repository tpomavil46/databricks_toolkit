"""
Standardized ETL Pipeline Framework

This module provides a standardized ETL pipeline class that orchestrates
all ETL operations using the Medallion Architecture pattern.
"""

from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from databricks.connect import DatabricksSession
from utils.logger import log_function_call

from .config import PipelineConfig
from .transformations import DataTransformation
from .validators import DataValidator


class StandardETLPipeline:
    """
    Standardized ETL pipeline framework following Medallion Architecture.
    """

    def __init__(self, config: PipelineConfig):
        """
        Initialize the ETL pipeline.

        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.spark = self._create_spark_session()
        self.transformer = DataTransformation(self.spark)
        self.validator = DataValidator(self.spark, config)
        self.pipeline_results = {}

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session based on configuration."""
        return (
            DatabricksSession.builder.profile(self.config.cluster_config.profile)
            .clusterId(self.config.cluster_config.cluster_id)
            .getOrCreate()
        )

    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise
        """
        try:
            # Try to get table metadata
            self.spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    def _get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about an existing table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table information
        """
        try:
            # Get table metadata
            table_info = self.spark.sql(f"DESCRIBE TABLE {table_name}")
            row_count = self.spark.table(table_name).count()

            return {
                "exists": True,
                "row_count": row_count,
                "schema": table_info.collect(),
            }
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def _handle_existing_table(self, table_name: str, auto_drop: bool = False) -> bool:
        """
        Handle existing table by checking and optionally dropping it.

        Args:
            table_name: Name of the table
            auto_drop: Whether to automatically drop existing tables

        Returns:
            True if table should be written, False if user cancelled
        """
        if not self._check_table_exists(table_name):
            return True

        # Get table information
        table_info = self._get_table_info(table_name)

        print(f"⚠️  Table '{table_name}' already exists!")
        print(f"📊 Current row count: {table_info.get('row_count', 'unknown'):,}")

        if auto_drop:
            print(f"🗑️  Auto-dropping existing table '{table_name}'...")
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                print(f"✅ Successfully dropped table '{table_name}'")
                return True
            except Exception as e:
                print(f"❌ Failed to drop table: {str(e)}")
                return False
        else:
            # Ask user what to do
            print(f"\n🔄 Options:")
            print(f"  1. Drop existing table and continue")
            print(f"  2. Cancel operation")
            print(f"  3. Use different table name")

            # For automated testing, default to dropping
            response = input("\nEnter choice (1-3, default=1): ").strip()

            if response == "1" or response == "":
                print(f"🗑️  Dropping existing table '{table_name}'...")
                try:
                    self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                    print(f"✅ Successfully dropped table '{table_name}'")
                    return True
                except Exception as e:
                    print(f"❌ Failed to drop table: {str(e)}")
                    return False
            elif response == "2":
                print("❌ Operation cancelled by user")
                return False
            elif response == "3":
                new_name = input("Enter new table name: ").strip()
                if new_name:
                    print(f"🔄 Using new table name: '{new_name}'")
                    return self._handle_existing_table(new_name, auto_drop)
                else:
                    print("❌ Invalid table name, cancelling operation")
                    return False
            else:
                print("❌ Invalid choice, cancelling operation")
                return False

    @log_function_call
    def run_bronze_ingestion(
        self, source_path: str, table_name: str = None, auto_drop: bool = False
    ) -> Dict[str, Any]:
        """
        Run bronze layer ingestion.

        Args:
            source_path: Path to source data
            table_name: Name for the bronze table (if None, auto-generate)
            auto_drop: Whether to automatically drop existing tables

        Returns:
            Dictionary containing ingestion results
        """
        if table_name is None:
            table_name = self.config.table_config.get_table_name("bronze", "data")

        print(f"🛢️  Starting Bronze Ingestion")
        print(f"📁 Source: {source_path}")
        print(f"🏷️  Table: {table_name}")
        print("=" * 60)

        try:
            # Check for existing table
            if not self._handle_existing_table(table_name, auto_drop):
                raise ValueError("Operation cancelled by user")

            # Load source data
            df = self._load_source_data(source_path)
            if df is None:
                raise ValueError(f"Failed to load data from {source_path}")

            # Create bronze table
            bronze_df = self.transformer.create_bronze_table(
                df, table_name, source_path
            )

            # Validate bronze data
            validation_results = self.validator.validate_bronze_data(
                bronze_df, table_name
            )

            # Write to bronze table
            self._write_table(bronze_df, table_name, "bronze")

            # Store results
            self.pipeline_results["bronze"] = {
                "table_name": table_name,
                "source_path": source_path,
                "row_count": bronze_df.count(),
                "validation_results": validation_results,
                "status": "success",
            }

            print(f"✅ Bronze ingestion completed successfully")
            self.validator.print_validation_report("bronze")

            return self.pipeline_results["bronze"]

        except Exception as e:
            error_msg = f"Bronze ingestion failed: {str(e)}"
            print(f"❌ {error_msg}")
            self.pipeline_results["bronze"] = {
                "table_name": table_name,
                "source_path": source_path,
                "status": "failed",
                "error": error_msg,
            }
            raise

    @log_function_call
    def run_silver_transformation(
        self,
        source_table: str,
        table_name: str = None,
        transformations: Dict[str, Any] = None,
        auto_drop: bool = False,
    ) -> Dict[str, Any]:
        """
        Run silver layer transformation.

        Args:
            source_table: Source table name (bronze table)
            table_name: Name for the silver table (if None, auto-generate)
            transformations: Transformation configuration
            auto_drop: Whether to automatically drop existing tables

        Returns:
            Dictionary containing transformation results
        """
        if table_name is None:
            table_name = self.config.table_config.get_table_name("silver", "data")

        print(f"⚙️  Starting Silver Transformation")
        print(f"📁 Source: {source_table}")
        print(f"🏷️  Table: {table_name}")
        print("=" * 60)

        try:
            # Check for existing table
            if not self._handle_existing_table(table_name, auto_drop):
                raise ValueError("Operation cancelled by user")

            # Load source data
            df = self.spark.table(source_table)

            # Apply silver transformations
            silver_df = self.transformer.create_silver_table(
                df, table_name, transformations
            )

            # Validate silver data
            validation_results = self.validator.validate_silver_data(
                silver_df, table_name
            )

            # Write to silver table
            self._write_table(silver_df, table_name, "silver")

            # Store results
            self.pipeline_results["silver"] = {
                "table_name": table_name,
                "source_table": source_table,
                "row_count": silver_df.count(),
                "validation_results": validation_results,
                "transformations_applied": (
                    list(transformations.keys()) if transformations else []
                ),
                "status": "success",
            }

            print(f"✅ Silver transformation completed successfully")
            self.validator.print_validation_report("silver")

            return self.pipeline_results["silver"]

        except Exception as e:
            error_msg = f"Silver transformation failed: {str(e)}"
            print(f"❌ {error_msg}")
            self.pipeline_results["silver"] = {
                "table_name": table_name,
                "source_table": source_table,
                "status": "failed",
                "error": error_msg,
            }
            raise

    @log_function_call
    def run_gold_aggregation(
        self,
        source_table: str,
        table_name: str = None,
        aggregations: Dict[str, Any] = None,
        auto_drop: bool = False,
    ) -> Dict[str, Any]:
        """
        Run gold layer aggregation.

        Args:
            source_table: Source table name (silver table)
            table_name: Name for the gold table (if None, auto-generate)
            aggregations: Aggregation configuration
            auto_drop: Whether to automatically drop existing tables

        Returns:
            Dictionary containing aggregation results
        """
        if table_name is None:
            table_name = self.config.table_config.get_table_name("gold", "data")

        print(f"🏆 Starting Gold Aggregation")
        print(f"📁 Source: {source_table}")
        print(f"🏷️  Table: {table_name}")
        print("=" * 60)

        try:
            # Check for existing table
            if not self._handle_existing_table(table_name, auto_drop):
                raise ValueError("Operation cancelled by user")

            # Load source data
            df = self.spark.table(source_table)

            # Apply gold aggregations
            gold_df = self.transformer.create_gold_table(df, table_name, aggregations)

            # Validate gold data
            validation_results = self.validator.validate_gold_data(gold_df, table_name)

            # Write to gold table
            self._write_table(gold_df, table_name, "gold")

            # Store results
            self.pipeline_results["gold"] = {
                "table_name": table_name,
                "source_table": source_table,
                "row_count": gold_df.count(),
                "validation_results": validation_results,
                "aggregations_applied": (
                    list(aggregations.keys()) if aggregations else []
                ),
                "status": "success",
            }

            print(f"✅ Gold aggregation completed successfully")
            self.validator.print_validation_report("gold")

            return self.pipeline_results["gold"]

        except Exception as e:
            error_msg = f"Gold aggregation failed: {str(e)}"
            print(f"❌ {error_msg}")
            self.pipeline_results["gold"] = {
                "table_name": table_name,
                "source_table": source_table,
                "status": "failed",
                "error": error_msg,
            }
            raise

    @log_function_call
    def run_full_pipeline(
        self,
        source_path: str,
        transformations: Dict[str, Any] = None,
        aggregations: Dict[str, Any] = None,
        auto_drop: bool = False,
    ) -> Dict[str, Any]:
        """
        Run complete ETL pipeline (bronze -> silver -> gold).

        Args:
            source_path: Path to source data
            transformations: Silver layer transformations
            aggregations: Gold layer aggregations
            auto_drop: Whether to automatically drop existing tables

        Returns:
            Dictionary containing complete pipeline results
        """
        print(f"🚀 Starting Full ETL Pipeline")
        print(f"📁 Source: {source_path}")
        print(f"🏷️  Project: {self.config.project_name}")
        print("=" * 60)

        try:
            # Run bronze ingestion
            bronze_results = self.run_bronze_ingestion(source_path, auto_drop=auto_drop)
            bronze_table = bronze_results["table_name"]

            # Run silver transformation
            silver_results = self.run_silver_transformation(
                bronze_table, transformations=transformations, auto_drop=auto_drop
            )
            silver_table = silver_results["table_name"]

            # Run gold aggregation
            gold_results = self.run_gold_aggregation(
                silver_table, aggregations=aggregations, auto_drop=auto_drop
            )

            # Compile full results
            full_results = {
                "project_name": self.config.project_name,
                "source_path": source_path,
                "bronze": bronze_results,
                "silver": silver_results,
                "gold": gold_results,
                "overall_status": "success",
                "total_rows_processed": (
                    bronze_results.get("row_count", 0)
                    + silver_results.get("row_count", 0)
                    + gold_results.get("row_count", 0)
                ),
            }

            print(f"✅ Full ETL pipeline completed successfully")
            print(f"📊 Total rows processed: {full_results['total_rows_processed']:,}")

            return full_results

        except Exception as e:
            error_msg = f"Full ETL pipeline failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "project_name": self.config.project_name,
                "source_path": source_path,
                "overall_status": "failed",
                "error": error_msg,
            }

    def _load_source_data(self, source_path: str) -> Optional[DataFrame]:
        """
        Load source data from various formats.

        Args:
            source_path: Path to source data

        Returns:
            DataFrame or None if failed
        """
        try:
            # Try to load as table first
            if not source_path.startswith("dbfs:/"):
                df = self.spark.table(source_path)
                print(f"✅ Loaded table: {source_path}")
                return df

            # Try different formats for DBFS paths
            formats_to_try = ["csv", "parquet", "delta", "json"]

            for fmt in formats_to_try:
                try:
                    if fmt == "csv":
                        df = (
                            self.spark.read.format(fmt)
                            .option("header", "true")
                            .load(source_path + "*")
                        )
                    else:
                        df = self.spark.read.format(fmt).load(source_path + "*")

                    print(f"✅ Loaded as {fmt.upper()}: {source_path}")
                    return df
                except Exception as e:
                    continue

            print(f"❌ Failed to load source data: {source_path}")
            return None

        except Exception as e:
            print(f"❌ Error loading source data: {str(e)}")
            return None

    def _write_table(self, df: DataFrame, table_name: str, layer: str) -> None:
        """
        Write DataFrame to table.

        Args:
            df: DataFrame to write
            table_name: Name for the table
            layer: Data layer (bronze, silver, gold)
        """
        try:
            # Use overwrite mode for idempotent operations
            df.write.mode("overwrite").saveAsTable(table_name)
            print(f"✅ Successfully wrote {layer} table: {table_name}")
        except Exception as e:
            print(f"❌ Error writing {layer} table: {str(e)}")
            raise

    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get a summary of all pipeline results."""
        summary = {
            "project_name": self.config.project_name,
            "layers_completed": list(self.pipeline_results.keys()),
            "total_layers": len(self.pipeline_results),
            "successful_layers": len(
                [
                    r
                    for r in self.pipeline_results.values()
                    if r.get("status") == "success"
                ]
            ),
            "failed_layers": len(
                [
                    r
                    for r in self.pipeline_results.values()
                    if r.get("status") == "failed"
                ]
            ),
            "total_rows_processed": sum(
                r.get("row_count", 0) for r in self.pipeline_results.values()
            ),
            "validation_summary": self.validator.get_validation_summary(),
        }

        return summary

    def print_pipeline_summary(self) -> None:
        """Print a formatted pipeline summary."""
        summary = self.get_pipeline_summary()

        print(f"\n📊 ETL Pipeline Summary")
        print("=" * 60)
        print(f"Project: {summary['project_name']}")
        print(
            f"Layers Completed: {summary['successful_layers']}/{summary['total_layers']}"
        )
        print(f"Total Rows Processed: {summary['total_rows_processed']:,}")
        print(
            f"Overall Quality Score: {summary['validation_summary']['overall_quality_score']:.1f}/100"
        )

        if summary["failed_layers"] > 0:
            print(f"⚠️  Failed Layers: {summary['failed_layers']}")

        print("=" * 60)
