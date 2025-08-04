#!/usr/bin/env python3
"""
Check available tables in the default schema
"""

from databricks.connect import DatabricksSession
from utils.logger import log_function_call


@log_function_call
def run(spark, **kwargs):
    """
    Check what tables are available in the default schema.
    
    Args:
        spark: Spark session
        **kwargs: Additional arguments
    """
    print("🔍 Checking available tables in default schema...")
    
    # Check current schema
    current_schema = spark.sql("SELECT current_schema()").collect()[0][0]
    print(f"Current schema: {current_schema}")
    
    # List tables in current schema
    tables = spark.sql("SHOW TABLES").collect()
    print(f"\n📋 Available tables in {current_schema}:")
    for table in tables:
        print(f"  - {table.tableName}")
    
    # Also check if there are any sample datasets available
    print(f"\n🔍 Checking for sample datasets...")
    try:
        sample_data = spark.sql("SELECT * FROM sample_data LIMIT 1")
        print("✅ sample_data table exists")
    except:
        print("❌ sample_data table not found")
    
    try:
        nyc_taxi = spark.sql("SELECT * FROM nyc_taxi LIMIT 1")
        print("✅ nyc_taxi table exists")
    except:
        print("❌ nyc_taxi table not found")
    
    try:
        retail_data = spark.sql("SELECT * FROM retail_data LIMIT 1")
        print("✅ retail_data table exists")
    except:
        print("❌ retail_data table not found")


def main():
    """Main entry point for standalone execution."""
    # Create Spark session using your existing pattern
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")
        .getOrCreate()
    )
    
    run(spark)


if __name__ == "__main__":
    main() 