#!/usr/bin/env python3
"""
Bronze Ingestion CLI - Create bronze tables from raw data sources.
"""

import os
import sys
import argparse

# Add project root to sys.path so imports work
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from databricks.connect import DatabricksSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from utils.logger import log_function_call
import json
from datetime import datetime


class BronzeIngestion:
    """
    Bronze layer ingestion tool.
    """
    
    def __init__(self, spark):
        """
        Initialize the bronze ingestion tool.
        
        Args:
            spark: Spark session
        """
        self.spark = spark
    
    def ingest_to_bronze(self, source_path: str, bronze_table_name: str, project_name: str = "default"):
        """
        Ingest raw data into bronze table.
        
        Args:
            source_path: Path to source data (DBFS or table name)
            bronze_table_name: Name for the bronze table
            project_name: Project name for organization
            
        Returns:
            DataFrame of the bronze table
        """
        print(f"🛢️  Ingesting to Bronze Layer")
        print(f"📁 Source: {source_path}")
        print(f"🏷️  Bronze Table: {bronze_table_name}")
        print(f"📂 Project: {project_name}")
        print("=" * 80)
        
        # Load source data
        df = self._load_source_data(source_path)
        if df is None:
            return None
        
        # Add bronze layer metadata
        bronze_df = self._add_bronze_metadata(df, source_path)
        
        # Write to bronze table
        self._write_bronze_table(bronze_df, bronze_table_name)
        
        # Show bronze table info
        self._show_bronze_info(bronze_df, bronze_table_name)
        
        return bronze_df
    
    def _load_source_data(self, source_path: str):
        """
        Load source data from various formats.
        
        Args:
            source_path: Path to source data
            
        Returns:
            DataFrame or None if failed
        """
        try:
            # Try to load as table first
            if not source_path.startswith('dbfs:/'):
                df = self.spark.table(source_path)
                print(f"✅ Loaded table: {source_path}")
                return df
            
            # Try different formats for DBFS paths
            formats_to_try = ["csv", "parquet", "delta", "json"]
            
            for fmt in formats_to_try:
                try:
                    if fmt == "csv":
                        df = self.spark.read.format(fmt).option("header", "true").load(source_path + "*")
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
    
    def _add_bronze_metadata(self, df, source_path: str):
        """
        Add bronze layer metadata columns.
        
        Args:
            df: Source DataFrame
            source_path: Original source path
            
        Returns:
            DataFrame with bronze metadata
        """
        bronze_df = df.withColumn("_bronze_ingestion_timestamp", current_timestamp()) \
                     .withColumn("_bronze_source_path", lit(source_path)) \
                     .withColumn("_bronze_layer", lit("bronze"))
        
        print(f"✅ Added bronze metadata columns")
        return bronze_df
    
    def _write_bronze_table(self, df, bronze_table_name: str):
        """
        Write DataFrame to bronze table.
        
        Args:
            df: DataFrame to write
            bronze_table_name: Name for the bronze table
        """
        try:
            df.write.mode("overwrite").saveAsTable(bronze_table_name)
            print(f"✅ Successfully wrote bronze table: {bronze_table_name}")
        except Exception as e:
            print(f"❌ Error writing bronze table: {str(e)}")
    
    def _show_bronze_info(self, df, bronze_table_name: str):
        """
        Show information about the created bronze table.
        
        Args:
            df: Bronze DataFrame
            bronze_table_name: Name of the bronze table
        """
        print(f"\n📊 Bronze Table Information:")
        print(f"   Table Name: {bronze_table_name}")
        print(f"   Row Count: {df.count():,}")
        print(f"   Column Count: {len(df.columns)}")
        print(f"   Schema: {', '.join(df.columns)}")


@log_function_call
def bronze_ingestion(source_path: str, bronze_table_name: str, project_name: str = "default"):
    """
    CLI function for bronze ingestion.
    
    Args:
        source_path: Path to source data
        bronze_table_name: Name for the bronze table
        project_name: Project name for organization
    """
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")
        .getOrCreate()
    )
    
    ingestion = BronzeIngestion(spark)
    return ingestion.ingest_to_bronze(source_path, bronze_table_name, project_name)


def main():
    """CLI entry point for bronze ingestion."""
    parser = argparse.ArgumentParser(description="Ingest data into bronze layer.")
    parser.add_argument(
        "--source-path",
        type=str,
        required=True,
        help="Path to source data (DBFS path or table name)"
    )
    parser.add_argument(
        "--bronze-table-name",
        type=str,
        required=True,
        help="Name for the bronze table"
    )
    parser.add_argument(
        "--project-name",
        type=str,
        default="default",
        help="Project name for organization (default: default)"
    )
    
    args = parser.parse_args()
    
    bronze_ingestion(args.source_path, args.bronze_table_name, args.project_name)


if __name__ == "__main__":
    main() 