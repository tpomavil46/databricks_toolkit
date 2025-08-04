#!/usr/bin/env python3
"""
Retail Pipeline - Dedicated pipeline for retail customer analytics
"""

from databricks.connect import DatabricksSession
from pipelines.sql_driven_pipeline import SQLDrivenPipeline
from utils.logger import log_function_call


@log_function_call
def run(spark, **kwargs):
    """
    Run the complete retail pipeline for customer analytics.
    
    Args:
        spark: Spark session
        **kwargs: Additional arguments
    """
    print("🚀 Starting Retail Customer Analytics Pipeline")
    
    # Use default parameters if none provided
    if not kwargs:
        kwargs = {
            'bronze_table': 'retail_customers_bronze',
            'silver_table': 'retail_customers_silver', 
            'gold_table': 'retail_customers_gold',
            'vendor_filter': None
        }
    
    print(f"📊 Parameters: {kwargs}")
    
    # Create retail-specific pipeline
    pipeline = SQLDrivenPipeline(spark, project="retail")
    
    # Run the silver-gold pipeline (bronze data already exists)
    results = pipeline.run_silver_gold_pipeline(**kwargs)
    
    if results['success']:
        print("✅ Retail Pipeline completed successfully!")
        print(f"   Steps completed: {', '.join(results['steps_completed'])}")
    else:
        print("❌ Retail Pipeline failed!")
        for error in results['errors']:
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