"""
SQL-Driven Pipeline

A pipeline implementation that uses SQL files as arguments to maintain
loose coupling between SQL logic and Python orchestration.

This demonstrates how to:
- Pass SQL files as arguments to Python code
- Keep SQL and Python separated
- Make Python code reusable and generalized
- Maintain the "ilities" principles
"""

import os
from typing import Dict, Any, Optional
from pathlib import Path

from core.sql_pipeline_executor import SQLPipelineExecutor, SQLPipelineBuilder
from utils.logger import log_function_call


class SQLDrivenPipeline:
    """
    A SQL-driven pipeline that maintains loose coupling between SQL and Python.
    
    This class demonstrates how to:
    - Accept SQL files as arguments
    - Execute SQL with parameter substitution
    - Maintain separation of concerns
    - Make Python code reusable and generalized
    """
    
    def __init__(self, spark, sql_base_path: str = "sql", project: str = "retail"):
        """
        Initialize the SQL-driven pipeline.
        
        Args:
            spark: Spark session
            sql_base_path: Base path for SQL files
            project: Project name (e.g., 'retail', 'ecommerce', 'healthcare')
        """
        self.spark = spark
        self.sql_base_path = Path(sql_base_path)
        self.project = project
        self.executor = SQLPipelineExecutor(spark)
        
        # Define SQL file names based on project
        self.sql_files = {
            'retail': {
                'bronze': 'ingest_customers.sql',
                'silver': 'transform_customers.sql', 
                'gold': 'customer_kpis.sql'
            },
            'ecommerce': {
                'bronze': 'ingest_orders.sql',
                'silver': 'transform_orders.sql',
                'gold': 'sales_kpis.sql'
            },
            'healthcare': {
                'bronze': 'ingest_patients.sql',
                'silver': 'transform_patients.sql',
                'gold': 'patient_kpis.sql'
            }
        }
    
    @log_function_call
    def run_bronze(self, input_path: str, bronze_table: str, file_format: str = "delta") -> None:
        """
        Execute bronze layer ingestion using SQL file.
        
        Args:
            input_path: Path to input data
            bronze_table: Name of bronze table to create
            file_format: Format of input data
        """
        sql_file = self.sql_base_path / "bronze" / self.project / self.sql_files[self.project]['bronze']
        
        config = (SQLPipelineBuilder()
                 .from_file(str(sql_file))
                 .with_parameters({
                     'input_path': input_path,
                     'bronze_table_name': bronze_table,
                     'file_format': file_format
                 })
                 .write_to_table(bronze_table, format="delta", mode="overwrite")
                 .build())
        
        self.executor.execute_sql(config)
        print(f"✅ Bronze layer completed: {bronze_table}")
    
    @log_function_call
    def run_silver(self, bronze_table: str, silver_table: str, vendor_filter: Optional[int] = None) -> None:
        """
        Execute silver layer transformation using SQL file.
        
        Args:
            bronze_table: Name of bronze table to read from
            silver_table: Name of silver table to create
            vendor_filter: Optional vendor ID filter
        """
        sql_file = self.sql_base_path / "silver" / self.project / self.sql_files[self.project]['silver']
        
        # Build vendor filter condition
        vendor_filter_condition = ""
        if vendor_filter is not None:
            vendor_filter_condition = f"WHERE vendor_id = {vendor_filter}"
        
        config = (SQLPipelineBuilder()
                 .from_file(str(sql_file))
                 .with_parameters({
                     'bronze_table_name': bronze_table,
                     'silver_table_name': silver_table,
                     'vendor_filter_condition': vendor_filter_condition
                 })
                 .write_to_table(silver_table, format="delta", mode="overwrite")
                 .build())
        
        self.executor.execute_sql(config)
        print(f"✅ Silver layer completed: {silver_table}")
    
    @log_function_call
    def run_gold(self, silver_table: str, gold_table: str, vendor_filter: Optional[int] = None) -> None:
        """
        Execute gold layer KPI generation using SQL file.
        
        Args:
            silver_table: Name of silver table to read from
            gold_table: Name of gold table to create
            vendor_filter: Optional vendor ID filter
        """
        sql_file = self.sql_base_path / "gold" / self.project / self.sql_files[self.project]['gold']
        
        # Build vendor filter condition
        vendor_filter_condition = ""
        if vendor_filter is not None:
            vendor_filter_condition = f"WHERE vendor_id = {vendor_filter}"
        
        config = (SQLPipelineBuilder()
                 .from_file(str(sql_file))
                 .with_parameters({
                     'silver_table_name': silver_table,
                     'gold_table_name': gold_table,
                     'vendor_filter_condition': vendor_filter_condition
                 })
                 .write_to_table(gold_table, format="delta", mode="overwrite")
                 .build())
        
        self.executor.execute_sql(config)
        print(f"✅ Gold layer completed: {gold_table}")
    
    @log_function_call
    def run_full_pipeline(self, 
                         input_path: str,
                         bronze_table: str,
                         silver_table: str,
                         gold_table: str,
                         file_format: str = "delta",
                         vendor_filter: Optional[int] = None) -> Dict[str, Any]:
        """
        Run the complete Medallion pipeline using SQL files.
        
        Args:
            input_path: Path to input data
            bronze_table: Name of bronze table
            silver_table: Name of silver table
            gold_table: Name of gold table
            file_format: Format of input data
            vendor_filter: Optional vendor ID filter
            
        Returns:
            Dictionary with pipeline execution results
        """
        print("🚀 Starting SQL-Driven Medallion Pipeline")
        
        results = {
            'success': True,
            'steps_completed': [],
            'errors': []
        }
        
        try:
            # Bronze Layer
            self.run_bronze(input_path, bronze_table, file_format)
            results['steps_completed'].append('bronze')
            
            # Silver Layer
            self.run_silver(bronze_table, silver_table, vendor_filter)
            results['steps_completed'].append('silver')
            
            # Gold Layer
            self.run_gold(silver_table, gold_table, vendor_filter)
            results['steps_completed'].append('gold')
            
            print("✅ SQL-Driven Pipeline completed successfully")
            
        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            print(f"❌ {error_msg}")
            results['success'] = False
            results['errors'].append(error_msg)
        
        return results

    @log_function_call
    def run_silver_gold_pipeline(self, 
                                bronze_table: str,
                                silver_table: str,
                                gold_table: str,
                                vendor_filter: Optional[int] = None) -> Dict[str, Any]:
        """
        Run only silver and gold layers (skip bronze ingestion).
        
        Args:
            bronze_table: Name of existing bronze table
            silver_table: Name of silver table
            gold_table: Name of gold table
            vendor_filter: Optional vendor ID filter
            
        Returns:
            Dictionary with pipeline execution results
        """
        print("🚀 Starting Silver-Gold Pipeline")
        
        results = {
            'success': True,
            'steps_completed': [],
            'errors': []
        }
        
        try:
            # Silver Layer
            self.run_silver(bronze_table, silver_table, vendor_filter)
            results['steps_completed'].append('silver')
            
            # Gold Layer
            self.run_gold(silver_table, gold_table, vendor_filter)
            results['steps_completed'].append('gold')
            
            print("✅ Silver-Gold Pipeline completed successfully")
            
        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            print(f"❌ {error_msg}")
            results['success'] = False
            results['errors'].append(error_msg)
        
        return results


def run_sql_pipeline(spark, **kwargs):
    """
    Execute SQL-driven pipeline with the given parameters.
    
    This function demonstrates how to make Python code reusable by accepting
    SQL files as arguments and maintaining loose coupling.
    
    Args:
        spark: Spark session
        **kwargs: Pipeline parameters including:
            - input_table: Raw input file or table name
            - bronze_path: Target Bronze table name
            - silver_path: Target Silver table name
            - gold_path: Target Gold table name
            - format: Storage format (default: delta)
            - vendor_filter: Optional vendor ID filter
    """
    pipeline = SQLDrivenPipeline(spark)
    
    return pipeline.run_full_pipeline(
        input_path=kwargs.get("input_table"),
        bronze_table=kwargs.get("bronze_path"),
        silver_table=kwargs.get("silver_path"),
        gold_table=kwargs.get("gold_path"),
        file_format=kwargs.get("format", "delta"),
        vendor_filter=kwargs.get("vendor_filter")
    ) 