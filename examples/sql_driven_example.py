"""
SQL-Driven Pipeline Example

This example demonstrates how to use the SQL-driven pipeline framework
to maintain loose coupling between SQL and Python code.

Key Benefits:
- SQL files can be modified without touching Python code
- Python code is reusable and generalized
- Easy to add new pipeline steps by creating new SQL files
- Maintains separation of concerns
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
sys.path.append(str(Path(__file__).parent.parent))

from core.sql_pipeline_executor import SQLPipelineExecutor, SQLPipelineBuilder
from pipelines.sql_driven_pipeline import SQLDrivenPipeline


def example_basic_usage():
    """Demonstrate basic usage of the SQL-driven pipeline."""
    print("🔧 Example 1: Basic SQL-Driven Pipeline Usage")
    print("=" * 50)
    
    # This would normally come from your Spark session
    # spark = get_spark_session()
    
    # Create pipeline instance
    pipeline = SQLDrivenPipeline(spark=None, sql_base_path="sql")
    
    # Example parameters
    params = {
        'input_path': '/path/to/raw/data',
        'bronze_table': 'my_bronze_table',
        'silver_table': 'my_silver_table', 
        'gold_table': 'my_gold_table',
        'file_format': 'delta',
        'vendor_filter': 123
    }
    
    print("Pipeline would execute with these parameters:")
    for key, value in params.items():
        print(f"  {key}: {value}")
    
    print("\nSQL files would be loaded from:")
    print("  Bronze: sql/bronze/ingest_data.sql")
    print("  Silver: sql/silver/transform_data.sql") 
    print("  Gold: sql/gold/generate_kpis.sql")
    
    print("\n✅ This demonstrates loose coupling - SQL files can be modified")
    print("   without touching the Python code!")


def example_sql_executor_usage():
    """Demonstrate direct usage of SQLPipelineExecutor."""
    print("\n🔧 Example 2: Direct SQL Executor Usage")
    print("=" * 50)
    
    # Create executor
    executor = SQLPipelineExecutor(spark=None)
    
    # Example 1: Execute SQL from file with parameters
    config1 = (SQLPipelineBuilder()
               .from_file("sql/bronze/ingest_data.sql")
               .with_parameters({
                   'input_path': '/data/raw/orders.csv',
                   'bronze_table_name': 'orders_bronze',
                   'file_format': 'csv'
               })
               .write_to_table('orders_bronze', format='delta')
               .build())
    
    print("Configuration 1: Load from file with parameters")
    print(f"  SQL File: {config1.sql_file_path}")
    print(f"  Parameters: {config1.sql_parameters}")
    print(f"  Write to table: {config1.table_name}")
    
    # Example 2: Execute SQL from string
    config2 = (SQLPipelineBuilder()
               .from_string("SELECT * FROM orders_bronze WHERE vendor_id = ${vendor_id}")
               .with_parameters({'vendor_id': 123})
               .create_temp_view('filtered_orders')
               .build())
    
    print("\nConfiguration 2: Load from string with temp view")
    print(f"  SQL String: {config2.sql_string}")
    print(f"  Parameters: {config2.sql_parameters}")
    print(f"  Create temp view: {config2.temp_view_name}")


def example_custom_sql_pipeline():
    """Demonstrate creating a custom SQL pipeline."""
    print("\n🔧 Example 3: Custom SQL Pipeline")
    print("=" * 50)
    
    # Define custom SQL files for different use cases
    custom_sql_files = {
        'data_quality_check': 'sql/quality/check_data.sql',
        'data_cleaning': 'sql/cleaning/clean_data.sql',
        'feature_engineering': 'sql/features/create_features.sql',
        'model_scoring': 'sql/ml/score_model.sql'
    }
    
    print("Custom pipeline with specialized SQL files:")
    for step, sql_file in custom_sql_files.items():
        print(f"  {step}: {sql_file}")
    
    print("\nEach SQL file can be parameterized:")
    print("  - Different input tables")
    print("  - Different output locations") 
    print("  - Different business rules")
    print("  - Different data quality thresholds")
    
    print("\n✅ Python code remains the same - just change SQL files!")


def example_parameter_substitution():
    """Demonstrate SQL parameter substitution."""
    print("\n🔧 Example 4: SQL Parameter Substitution")
    print("=" * 50)
    
    # Example SQL with parameters
    sql_template = """
    SELECT 
        customer_id,
        SUM(total_amount) as total_spend,
        COUNT(*) as order_count
    FROM ${input_table}
    WHERE vendor_id = ${vendor_id}
    AND order_date >= '${start_date}'
    GROUP BY customer_id
    HAVING total_spend > ${min_spend}
    """
    
    # Parameters to substitute
    parameters = {
        'input_table': 'orders_silver',
        'vendor_id': 123,
        'start_date': '2024-01-01',
        'min_spend': 1000
    }
    
    print("SQL Template:")
    print(sql_template.strip())
    
    print("\nParameters:")
    for key, value in parameters.items():
        print(f"  ${key}: {value}")
    
    print("\nSubstituted SQL would be:")
    substituted_sql = sql_template
    for param_name, param_value in parameters.items():
        placeholder = f"${{{param_name}}}"
        if isinstance(param_value, str):
            substituted_sql = substituted_sql.replace(placeholder, f"'{param_value}'")
        else:
            substituted_sql = substituted_sql.replace(placeholder, str(param_value))
    
    print(substituted_sql.strip())


def example_pipeline_variations():
    """Demonstrate different pipeline configurations."""
    print("\n🔧 Example 5: Pipeline Variations")
    print("=" * 50)
    
    # Different pipeline configurations
    pipelines = {
        'orders_pipeline': {
            'input_path': '/data/raw/orders.csv',
            'bronze_table': 'orders_bronze',
            'silver_table': 'orders_silver', 
            'gold_table': 'orders_gold',
            'vendor_filter': 123
        },
        'customers_pipeline': {
            'input_path': '/data/raw/customers.json',
            'bronze_table': 'customers_bronze',
            'silver_table': 'customers_silver',
            'gold_table': 'customers_gold',
            'vendor_filter': None
        },
        'products_pipeline': {
            'input_path': '/data/raw/products.parquet',
            'bronze_table': 'products_bronze',
            'silver_table': 'products_silver',
            'gold_table': 'products_gold',
            'vendor_filter': 456
        }
    }
    
    print("Different pipeline configurations using the same Python code:")
    for name, config in pipelines.items():
        print(f"\n{name}:")
        for key, value in config.items():
            print(f"  {key}: {value}")
    
    print("\n✅ Same Python code, different SQL files and parameters!")


def main():
    """Run all examples."""
    print("🚀 SQL-Driven Pipeline Examples")
    print("=" * 60)
    print("This demonstrates how to pass SQL files as arguments to make")
    print("Python code reusable and generalized while maintaining loose coupling.")
    print()
    
    example_basic_usage()
    example_sql_executor_usage()
    example_custom_sql_pipeline()
    example_parameter_substitution()
    example_pipeline_variations()
    
    print("\n" + "=" * 60)
    print("🎯 Key Benefits Achieved:")
    print("  ✅ Loose coupling between SQL and Python")
    print("  ✅ SQL files can be modified without touching Python code")
    print("  ✅ Python code is reusable and generalized")
    print("  ✅ Easy to add new pipeline steps")
    print("  ✅ Maintains separation of concerns")
    print("  ✅ Follows SOLID principles")
    print("  ✅ Supports all the 'ilities'")


if __name__ == "__main__":
    main() 