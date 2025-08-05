#!/usr/bin/env python3
"""
Ecommerce Pipeline - Dedicated pipeline for ecommerce sales analytics
"""

from databricks.connect import DatabricksSession
from pipelines.sql_driven_pipeline import SQLDrivenPipeline
from utils.logger import log_function_call


@log_function_call
def run(spark, **kwargs):
    """
    Run the complete ecommerce pipeline for sales analytics.

    Args:
        spark: Spark session
        **kwargs: Additional arguments
    """
    print("🚀 Starting Ecommerce Sales Analytics Pipeline")

    # Use default parameters if none provided
    if not kwargs:
        kwargs = {
            "bronze_table": "ecommerce_orders_bronze",
            "silver_table": "ecommerce_orders_silver",
            "gold_table": "ecommerce_orders_gold",
            "vendor_filter": None,
        }

    print(f"📊 Parameters: {kwargs}")

    # Create ecommerce-specific pipeline
    pipeline = SQLDrivenPipeline(spark, project="ecommerce")

    # Run the silver-gold pipeline (bronze data already exists)
    results = pipeline.run_silver_gold_pipeline(**kwargs)

    if results["success"]:
        print("✅ Ecommerce Pipeline completed successfully!")
        print(f"   Steps completed: {', '.join(results['steps_completed'])}")
    else:
        print("❌ Ecommerce Pipeline failed!")
        for error in results["errors"]:
            print(f"   Error: {error}")

    return results


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
