"""
DLT Pipeline Runner

This module provides DLT-specific pipeline execution with support for:
- Auto Loader integration
- Data quality constraints
- Streaming tables and materialized views
- CDC processing
"""

import os
import re
from typing import Dict, Any, List, Optional
from pathlib import Path
from databricks.connect import DatabricksSession
from shared.utils.logger import log_function_call


class DLTPipelineRunner:
    """
    DLT Pipeline Runner for executing DLT-specific SQL with Auto Loader support.
    
    This class handles DLT syntax, data quality constraints, and Auto Loader
    integration for comprehensive data pipeline execution.
    """
    
    def __init__(self, spark_session: DatabricksSession, config: Dict[str, Any]):
        """
        Initialize DLT Pipeline Runner.
        
        Args:
            spark_session: Active Databricks Spark session
            config: Pipeline configuration
        """
        self.spark = spark_session
        self.config = config
        self.sql_base_path = config.get('sql_base_path', 'workflows/sql_driven/sql')
        
    @log_function_call
    def load_dlt_template(self, template_name: str, parameters: Dict[str, Any]) -> str:
        """
        Load and parameterize a DLT template.
        
        Args:
            template_name: Name of the template file
            parameters: Parameters for template substitution
            
        Returns:
            Parameterized SQL string
        """
        template_path = Path(self.sql_base_path) / 'templates' / f'{template_name}.sql'
        
        if not template_path.exists():
            raise FileNotFoundError(f"DLT template not found: {template_path}")
            
        with open(template_path, 'r') as f:
            sql_template = f.read()
            
        # Parameter substitution
        for key, value in parameters.items():
            placeholder = f"${{{key}}}"
            sql_template = sql_template.replace(placeholder, str(value))
            
        return sql_template
    
    @log_function_call
    def execute_dlt_bronze_ingestion(self, source_path: str, table_name: str, 
                                   file_format: str = "json", options: str = "") -> None:
        """
        Execute DLT bronze ingestion with Auto Loader.
        
        Args:
            source_path: Source data path
            table_name: Target table name
            file_format: File format (json, csv, parquet, etc.)
            options: Auto Loader options
        """
        print(f"🔄 Executing Bronze Ingestion: {table_name}")
        
        parameters = {
            'source_path': source_path,
            'table_name': table_name,
            'file_format': file_format,
            'options': options or '"cloudFiles.inferColumnTypes", "true"'
        }
        
        # Use simple template for regular Spark SQL execution
        sql = self.load_dlt_template('simple_bronze_ingestion', parameters)
            
        self._execute_dlt_sql(sql, f"Bronze ingestion for {table_name}")
        
    @log_function_call
    def execute_dlt_silver_transformation(self, bronze_table: str, silver_table: str) -> None:
        """
        Execute DLT silver transformation with data quality constraints.
        
        Args:
            bronze_table: Source bronze table
            silver_table: Target silver table
        """
        print(f"🔄 Executing Silver Transformation: {bronze_table} → {silver_table}")
        
        parameters = {
            'bronze_table': bronze_table,
            'silver_table': silver_table
        }
        
        # Use simple template for regular Spark SQL execution
        sql = self.load_dlt_template('simple_silver_transformation', parameters)
            
        self._execute_dlt_sql(sql, f"Silver transformation for {silver_table}")
        
    @log_function_call
    def execute_dlt_gold_aggregation(self, silver_table: str, gold_table: str) -> None:
        """
        Execute DLT gold aggregation with materialized views.
        
        Args:
            silver_table: Source silver table
            gold_table: Target gold table
        """
        print(f"🔄 Executing Gold Aggregation: {silver_table} → {gold_table}")
        
        parameters = {
            'silver_table': silver_table,
            'gold_table': gold_table
        }
        
        # Use simple template for regular Spark SQL execution
        sql = self.load_dlt_template('simple_gold_aggregation', parameters)
            
        self._execute_dlt_sql(sql, f"Gold aggregation for {gold_table}")
        
    @log_function_call
    def execute_dlt_cdc_processing(self, bronze_table: str, silver_table: str, 
                                 key_column: str) -> None:
        """
        Execute DLT CDC processing with APPLY CHANGES.
        
        Args:
            bronze_table: Source bronze table
            silver_table: Target silver table
            key_column: Primary key column for CDC
        """
        print(f"🔄 Executing DLT CDC Processing: {bronze_table} → {silver_table}")
        
        parameters = {
            'bronze_table': bronze_table,
            'silver_table': silver_table,
            'key_column': key_column
        }
        
        sql = self.load_dlt_template('dlt_cdc_processing', parameters)
        self._execute_dlt_sql(sql, f"CDC processing for {silver_table}")
        
    def _execute_dlt_sql(self, sql: str, description: str) -> None:
        """
        Execute DLT SQL with proper error handling.
        
        Args:
            sql: SQL to execute
            description: Description for logging
        """
        try:
            print(f"📝 Executing: {description}")
            print(f"🔍 SQL Preview: {sql[:200]}...")
            
            # Execute SQL
            result = self.spark.sql(sql)
            
            # For DLT operations, we might not get immediate results
            # but we can check if the operation was successful
            print(f"✅ Successfully executed: {description}")
            
        except Exception as e:
            print(f"❌ Error executing {description}: {str(e)}")
            raise
    
    @log_function_call
    def run_complete_dlt_pipeline(self, project: str, source_path: str, 
                                 environment: str = "dev") -> None:
        """
        Run a complete DLT pipeline with bronze, silver, and gold layers.
        
        Args:
            project: Project name (e.g., 'retail', 'ecommerce')
            source_path: Source data path
            environment: Environment (dev, staging, prod)
        """
        print(f"🚀 Starting Complete DLT Pipeline for {project}")
        print(f"📁 Source: {source_path}")
        print(f"🌍 Environment: {environment}")
        
        # Bronze Layer
        bronze_table = f"{project}_bronze"
        self.execute_dlt_bronze_ingestion(
            source_path=source_path,
            table_name=bronze_table,
            file_format="json"
        )
        
        # Silver Layer
        silver_table = f"{project}_silver"
        self.execute_dlt_silver_transformation(
            bronze_table=bronze_table,
            silver_table=silver_table
        )
        
        # Gold Layer
        gold_table = f"{project}_analytics"
        self.execute_dlt_gold_aggregation(
            silver_table=silver_table,
            gold_table=gold_table
        )
        
        print(f"✅ Complete DLT Pipeline finished for {project}")
        
    def validate_dlt_syntax(self, sql: str) -> bool:
        """
        Validate DLT-specific syntax.
        
        Args:
            sql: SQL to validate
            
        Returns:
            True if valid DLT syntax
        """
        # Check for DLT-specific keywords
        dlt_keywords = [
            'CREATE OR REFRESH STREAMING TABLE',
            'CREATE OR REFRESH MATERIALIZED VIEW',
            'CONSTRAINT',
            'EXPECT',
            'ON VIOLATION',
            'TBLPROPERTIES',
            'cloud_files',
            'STREAM',
            'LIVE',
            'APPLY CHANGES'
        ]
        
        sql_upper = sql.upper()
        found_keywords = [keyword for keyword in dlt_keywords if keyword in sql_upper]
        
        if not found_keywords:
            print("⚠️ Warning: No DLT-specific keywords found in SQL")
            return False
            
        print(f"✅ DLT syntax validation passed. Found keywords: {found_keywords}")
        return True


def run_dlt_pipeline(project: str, source_path: str, environment: str = "dev") -> None:
    """
    Main entry point for DLT pipeline execution.
    
    Args:
        project: Project name
        source_path: Source data path
        environment: Environment
    """
    # Create Spark session
    spark = DatabricksSession.builder.profile("databricks").getOrCreate()
    
    # Configuration
    config = {
        'sql_base_path': 'workflows/sql_driven/sql',
        'environment': environment,
        'project': project
    }
    
    # Create and run DLT pipeline
    runner = DLTPipelineRunner(spark, config)
    runner.run_complete_dlt_pipeline(project, source_path, environment)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="DLT Pipeline Runner")
    parser.add_argument("project", help="Project name")
    parser.add_argument("source_path", help="Source data path")
    parser.add_argument("--environment", default="dev", help="Environment")
    
    args = parser.parse_args()
    
    run_dlt_pipeline(args.project, args.source_path, args.environment) 