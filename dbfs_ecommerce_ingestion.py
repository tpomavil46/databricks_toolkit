#!/usr/bin/env python3
"""
DBFS Ecommerce Ingestion - Transform retail data into ecommerce orders format.
"""

from databricks.connect import DatabricksSession
from pyspark.sql.functions import col, current_timestamp, lit, rand, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import random


def ingest_ecommerce_from_retail(spark):
    """
    Ingest retail data and transform it into ecommerce orders format.
    
    Args:
        spark: Spark session
    """
    print("🛒 Ingesting retail data and transforming to ecommerce orders format...")
    
    try:
        # Read retail customers data
        retail_path = "dbfs:/databricks-datasets/retail-org/customers/"
        print(f"📊 Reading from {retail_path}")
        
        df = spark.read.format("csv").option("header", "true").load(retail_path + "*")
        print(f"✅ Loaded {df.count()} retail customer records")
        print(f"📋 Schema: {df.schema}")
        df.show(5)
        
        # Transform retail data into ecommerce orders format
        print("\n🔄 Transforming to ecommerce orders format...")
        
        # Create ecommerce orders from retail data
        ecommerce_orders = df.select(
            # Generate order_id based on customer_id
            (col("customer_id").cast("string")).alias("order_id"),
            col("customer_id"),
            # Generate product_id based on state/city
            (lit("PROD-") + col("state").substr(1, 2) + col("city").substr(1, 2)).alias("product_id"),
            # Use units_purchased as quantity
            col("units_purchased").cast("int").alias("quantity"),
            # Generate unit_price based on loyalty_segment
            (col("loyalty_segment") * 50.0 + rand() * 100).alias("unit_price"),
            # Calculate total_amount
            (col("units_purchased") * (col("loyalty_segment") * 50.0 + rand() * 100)).alias("total_amount"),
            # Use current date as order_date
            current_timestamp().cast("date").alias("order_date"),
            # Generate status based on loyalty_segment
            when(col("loyalty_segment") >= 3, "completed")
            .when(col("loyalty_segment") >= 2, "pending")
            .otherwise("cancelled").alias("status")
        )
        
        # Add some data quality and business logic
        ecommerce_orders = ecommerce_orders.select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("quantity"),
            col("unit_price"),
            col("total_amount"),
            col("order_date"),
            col("status"),
            # Add calculated fields
            (col("quantity") * col("unit_price")).alias("calculated_total"),
            # Add data quality flags
            when(col("order_id").isNotNull() & col("customer_id").isNotNull(), "valid")
            .otherwise("invalid").alias("data_quality_flag"),
            # Add business logic
            when(col("total_amount") > 1000, "high_value")
            .when(col("total_amount") > 100, "medium_value")
            .otherwise("low_value").alias("order_category")
        )
        
        print(f"✅ Transformed to {ecommerce_orders.count()} ecommerce orders")
        print("📊 Sample ecommerce orders:")
        ecommerce_orders.show(5, truncate=False)
        
        # Write to bronze table
        ecommerce_orders.write.mode("overwrite").saveAsTable("ecommerce_orders_bronze")
        print("✅ Created ecommerce_orders_bronze table")
        
        return ecommerce_orders
        
    except Exception as e:
        print(f"❌ Error ingesting ecommerce data: {str(e)}")
        return None


def run(spark, **kwargs):
    """
    Run the ecommerce ingestion from retail data.
    
    Args:
        spark: Spark session
        **kwargs: Additional arguments
    """
    return ingest_ecommerce_from_retail(spark)


def main():
    """Main entry point."""
    # Create Spark session
    spark = (
        DatabricksSession.builder.profile("databricks")
        .clusterId("5802-005055-h7vtizbe")
        .getOrCreate()
    )
    
    ingest_ecommerce_from_retail(spark)


if __name__ == "__main__":
    main() 