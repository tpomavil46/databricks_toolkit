#!/usr/bin/env python3
"""
Simple script to find available datasets using databricks-connect
"""

from databricks.connect import DatabricksSession

def run(spark, **kwargs):
    """
    Find available datasets in DBFS using databricks-connect.
    
    Args:
        spark: Spark session
        **kwargs: Additional arguments
    """
    print("🔍 Searching for available datasets...")
    
    # Try to list some common dataset paths
    common_paths = [
        "dbfs:/databricks-datasets/",
        "dbfs:/databricks-datasets/nyctaxi/",
        "dbfs:/databricks-datasets/retail-org/",
        "dbfs:/databricks-datasets/ecommerce/",
        "dbfs:/databricks-datasets/iot-stream/",
        "dbfs:/databricks-datasets/airlines/",
        "dbfs:/databricks-datasets/weather/",
        "dbfs:/databricks-datasets/clickstream/",
        "dbfs:/databricks-datasets/events/",
        "dbfs:/databricks-datasets/sales/",
        "dbfs:/databricks-datasets/customers/",
        "dbfs:/databricks-datasets/products/"
    ]
    
    available_datasets = []
    
    for path in common_paths:
        try:
            print(f"Checking {path}...")
            files = spark.read.format("text").load(path).limit(1)
            print(f"✅ {path} - AVAILABLE")
            available_datasets.append(path)
        except Exception as e:
            print(f"❌ {path} - Not available")
    
    print(f"\n🎯 Found {len(available_datasets)} available datasets:")
    for dataset in available_datasets:
        print(f"  {dataset}")
    
    if available_datasets:
        print(f"\n💡 Try using one of these paths with your SQL-driven pipeline:")
        for dataset in available_datasets[:3]:  # Show first 3
            print(f"make run JOB=main_sql_driven --input_table {dataset} --bronze_path test_bronze --silver_path test_silver --gold_path test_gold")

def main():
    from databricks.connect import DatabricksSession
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")
        .getOrCreate()
    )
    run(spark)

if __name__ == "__main__":
    main() 