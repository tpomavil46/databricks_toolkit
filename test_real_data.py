#!/usr/bin/env python3
"""
Test script to verify we can query the actual Databricks tables and see real data.
"""

import os
import sys
from datetime import datetime

def test_real_data():
    """Test querying real data from your Databricks tables."""
    
    print("🔍 Testing real data queries from your Databricks tables...")
    
    try:
        from databricks.connect import DatabricksSession
        from pyspark.sql.functions import col, count, sum as spark_sum
        
        # Initialize Databricks Connect session
        spark = DatabricksSession.builder \
            .remote() \
            .getOrCreate()
        
        print("✅ Databricks Connect session created successfully")
        
        # Test retail_analytics table
        try:
            print("\n📊 Testing retail_analytics table:")
            analytics_df = spark.sql("SELECT * FROM retail_analytics LIMIT 5")
            analytics_df.show(truncate=False)
            
            # Get row count
            count_df = spark.sql("SELECT COUNT(*) as total_rows FROM retail_analytics")
            total_rows = count_df.collect()[0]['total_rows']
            print(f"✅ retail_analytics has {total_rows} rows")
            
        except Exception as e:
            print(f"❌ Error querying retail_analytics: {e}")
        
        # Test orders_gold table
        try:
            print("\n📊 Testing orders_gold table:")
            orders_df = spark.sql("SELECT * FROM orders_gold LIMIT 5")
            orders_df.show(truncate=False)
            
            # Get row count
            count_df = spark.sql("SELECT COUNT(*) as total_rows FROM orders_gold")
            total_rows = count_df.collect()[0]['total_rows']
            print(f"✅ orders_gold has {total_rows} rows")
            
        except Exception as e:
            print(f"❌ Error querying orders_gold: {e}")
        
        # Test orders_silver table
        try:
            print("\n📊 Testing orders_silver table:")
            silver_df = spark.sql("SELECT * FROM orders_silver LIMIT 5")
            silver_df.show(truncate=False)
            
            # Get row count
            count_df = spark.sql("SELECT COUNT(*) as total_rows FROM orders_silver")
            total_rows = count_df.collect()[0]['total_rows']
            print(f"✅ orders_silver has {total_rows} rows")
            
        except Exception as e:
            print(f"❌ Error querying orders_silver: {e}")
        
        # Test retail_customers_gold table
        try:
            print("\n📊 Testing retail_customers_gold table:")
            customers_df = spark.sql("SELECT * FROM retail_customers_gold LIMIT 5")
            customers_df.show(truncate=False)
            
            # Get row count
            count_df = spark.sql("SELECT COUNT(*) as total_rows FROM retail_customers_gold")
            total_rows = count_df.collect()[0]['total_rows']
            print(f"✅ retail_customers_gold has {total_rows} rows")
            
        except Exception as e:
            print(f"❌ Error querying retail_customers_gold: {e}")
        
        # Test the actual query that the dashboard will use
        try:
            print("\n📊 Testing dashboard query on retail_analytics:")
            dashboard_df = spark.sql("""
                SELECT 
                    order_date,
                    product_category,
                    category,
                    SUM(total_revenue) as total_revenue,
                    AVG(avg_price) as avg_price,
                    COUNT(*) as total_products
                FROM retail_analytics 
                GROUP BY order_date, product_category, category
                ORDER BY order_date DESC
                LIMIT 10
            """)
            dashboard_df.show(truncate=False)
            
        except Exception as e:
            print(f"❌ Error in dashboard query: {e}")
        
        spark.stop()
        print("\n✅ Real data test completed!")
        
    except Exception as e:
        print(f"❌ Error connecting to Databricks: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_real_data()
    sys.exit(0 if success else 1) 