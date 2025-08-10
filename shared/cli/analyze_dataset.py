#!/usr/bin/env python3
"""
Dataset Analysis CLI - Comprehensive analysis of datasets with data quality metrics.
"""

import os
import sys
import argparse
import pandas as pd

# Add project root to sys.path so imports work
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks.connect import DatabricksSession
from pyspark.sql.functions import col, count, isnan, when, isnull, desc, asc
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)
from utils.logger import log_function_call


class DatasetAnalyzer:
    """
    Comprehensive dataset analysis tool.
    """

    def __init__(self, spark):
        """
        Initialize the dataset analyzer.

        Args:
            spark: Spark session
        """
        self.spark = spark

    def analyze_dataset(
        self, dataset_path: str, dataset_name: str = None, max_rows: int = 10000
    ):
        """
        Comprehensive analysis of a dataset.

        Args:
            dataset_path: Path to the dataset (DBFS or table name)
            dataset_name: Optional name for the dataset
            max_rows: Maximum rows to analyze (for performance)
        """
        if dataset_name is None:
            dataset_name = (
                dataset_path.split("/")[-1] if "/" in dataset_path else dataset_path
            )

        print(f"🔍 Analyzing Dataset: {dataset_name}")
        print(f"📁 Path: {dataset_path}")
        print("=" * 80)

        # Load the dataset
        df = self._load_dataset(dataset_path)
        if df is None:
            return None

        # Basic information
        self._show_basic_info(df, dataset_name, max_rows)

        # Schema analysis
        self._show_schema_info(df)

        # Data quality analysis
        self._show_data_quality(df, max_rows)

        # Sample data
        self._show_sample_data(df)

        # Statistical summary
        self._show_statistical_summary(df)

        # Column analysis
        self._show_column_analysis(df, max_rows)

        return df

    def _load_dataset(self, dataset_path: str):
        """
        Load dataset from various sources.

        Args:
            dataset_path: Path to the dataset

        Returns:
            DataFrame or None if failed
        """
        try:
            # Try to load as table first (for table names)
            if not dataset_path.startswith("dbfs:/") and not "/" in dataset_path:
                df = self.spark.table(dataset_path)
                print(f"✅ Loaded table: {dataset_path}")
                return df

            # Try different formats for file paths (both local and DBFS)
            formats_to_try = ["csv", "parquet", "delta", "json"]

            for fmt in formats_to_try:
                try:
                    if fmt == "csv":
                        df = (
                            self.spark.read.format(fmt)
                            .option("header", "true")
                            .load(dataset_path)
                        )
                    else:
                        df = self.spark.read.format(fmt).load(dataset_path)

                    print(f"✅ Loaded as {fmt.upper()}: {dataset_path}")
                    return df
                except Exception as e:
                    continue

            print(f"❌ Failed to load dataset: {dataset_path}")
            return None

        except Exception as e:
            print(f"❌ Error loading dataset: {str(e)}")
            return None

    def _show_basic_info(self, df, dataset_name: str, max_rows: int):
        """Show basic dataset information."""
        print("\n📊 BASIC INFORMATION")
        print("-" * 40)

        # Get row count (limit for performance)
        sample_df = df.limit(max_rows)
        row_count = df.count()
        col_count = len(df.columns)

        print(f"Dataset Name: {dataset_name}")
        print(f"Total Rows: {row_count:,}")
        print(f"Total Columns: {col_count}")
        print(f"Columns: {', '.join(df.columns)}")

        # Show data types
        print(f"\nData Types:")
        for field in df.schema.fields:
            print(f"  {field.name}: {field.dataType}")

    def _show_schema_info(self, df):
        """Show detailed schema information."""
        print("\n📋 SCHEMA ANALYSIS")
        print("-" * 40)

        schema_info = []
        for field in df.schema.fields:
            info = {
                "column": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable,
            }
            schema_info.append(info)

        # Convert to pandas for better display
        schema_df = pd.DataFrame(schema_info)
        print(schema_df.to_string(index=False))

    def _show_data_quality(self, df, max_rows: int):
        """Show data quality metrics."""
        print("\n🔍 DATA QUALITY ANALYSIS")
        print("-" * 40)

        quality_metrics = []

        # Use sample for performance
        sample_df = df.limit(max_rows)

        for field in df.schema.fields:
            col_name = field.name

            # Count nulls
            null_count = sample_df.filter(col(col_name).isNull()).count()
            total_count = sample_df.count()
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0

            # Count distinct values
            distinct_count = sample_df.select(col_name).distinct().count()

            # For string columns, check for empty strings
            empty_string_count = 0
            if isinstance(field.dataType, StringType):
                empty_string_count = sample_df.filter(col(col_name) == "").count()

            quality_metrics.append(
                {
                    "column": col_name,
                    "null_count": null_count,
                    "null_percentage": f"{null_percentage:.2f}%",
                    "distinct_values": distinct_count,
                    "empty_strings": (
                        empty_string_count
                        if isinstance(field.dataType, StringType)
                        else "N/A"
                    ),
                }
            )

        quality_df = pd.DataFrame(quality_metrics)
        print(quality_df.to_string(index=False))

    def _show_sample_data(self, df):
        """Show sample data."""
        print("\n📄 SAMPLE DATA")
        print("-" * 40)

        print("First 5 rows:")
        df.show(5, truncate=False)

        print("\nLast 5 rows:")
        # Use a valid column for ordering (first column)
        first_col = df.columns[0]
        df.orderBy(desc(first_col)).show(5, truncate=False)

    def _show_statistical_summary(self, df):
        """Show statistical summary for numeric columns."""
        print("\n📈 STATISTICAL SUMMARY")
        print("-" * 40)

        numeric_columns = []
        for field in df.schema.fields:
            if isinstance(field.dataType, (IntegerType, DoubleType)):
                numeric_columns.append(field.name)

        if numeric_columns:
            print("Numeric columns found:")
            for col_name in numeric_columns:
                print(f"\n{col_name}:")
                stats = df.select(col_name).summary(
                    "count", "min", "25%", "50%", "75%", "max"
                )
                stats.show(truncate=False)
        else:
            print("No numeric columns found for statistical analysis.")

    def _show_column_analysis(self, df, max_rows: int):
        """Show detailed analysis for each column."""
        print("\n🔬 COLUMN ANALYSIS")
        print("-" * 40)

        # Use sample for performance
        sample_df = df.limit(max_rows)

        for field in df.schema.fields:
            col_name = field.name
            print(f"\n📊 Column: {col_name}")
            print(f"   Type: {field.dataType}")
            print(f"   Nullable: {field.nullable}")

            # Show top values for string columns
            if isinstance(field.dataType, StringType):
                print("   Top 5 values:")
                top_values = (
                    sample_df.groupBy(col_name).count().orderBy(desc("count")).limit(5)
                )
                top_values.show(truncate=False)

            # Show range for numeric columns
            elif isinstance(field.dataType, (IntegerType, DoubleType)):
                print("   Value range:")
                range_stats = sample_df.select(col_name).summary("min", "max")
                range_stats.show(truncate=False)


@log_function_call
def analyze_dataset(path: str, name: str = None, max_rows: int = 10000):
    """
    Analyze a dataset with comprehensive metrics.

    Args:
        path: Path to the dataset
        name: Optional name for the dataset
        max_rows: Maximum rows to analyze
    """
    # Create Spark session
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")
        .getOrCreate()
    )

    analyzer = DatasetAnalyzer(spark)
    return analyzer.analyze_dataset(path, name, max_rows)


def main():
    """CLI entrypoint for dataset analysis."""
    parser = argparse.ArgumentParser(
        description="Analyze datasets with comprehensive metrics."
    )
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="Path to the dataset (DBFS path or table name)",
    )
    parser.add_argument("--name", type=str, help="Optional name for the dataset")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=10000,
        help="Maximum rows to analyze (default: 10000)",
    )

    args = parser.parse_args()

    try:
        analyze_dataset(args.path, args.name, args.max_rows)
    except Exception as e:
        print(f"❌ Failed to analyze dataset: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
