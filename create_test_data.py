#!/usr/bin/env python3
"""
Create test data for SQL-driven pipeline demonstration
"""

from databricks.connect import DatabricksSession
from utils.logger import log_function_call


@log_function_call
def run(spark, **kwargs):
    """
    Create test data for the SQL-driven pipeline.
    
    Args:
        spark: Spark session
        **kwargs: Additional arguments
    """
    print("🔧 Creating test data for SQL-driven pipeline...")
    
    # Create a simple test table with sample data
    test_data = [
        (1, "Product A", 100.0, "Vendor 1"),
        (2, "Product B", 200.0, "Vendor 2"),
        (3, "Product C", 150.0, "Vendor 1"),
        (4, "Product D", 300.0, "Vendor 3"),
        (5, "Product E", 250.0, "Vendor 2"),
    ]
    
    # Create DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("vendor", StringType(), False)
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Write to table
    table_name = kwargs.get('table_name', 'test_orders')
    df.write.mode("overwrite").saveAsTable(table_name)
    
    print(f"✅ Created test table: {table_name}")
    print(f"   Records: {df.count()}")
    print(f"   Schema: {df.schema}")
    
    # Show sample data
    print(f"\n📊 Sample data from {table_name}:")
    df.show()
    
    return df


def main():
    """Main entry point for standalone execution."""
    # Create Spark session using your existing pattern
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")
        .getOrCreate()
    )
    
    run(spark, table_name="test_orders")


if __name__ == "__main__":
    main() 