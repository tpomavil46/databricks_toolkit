#!/usr/bin/env python3
"""
Test script to verify Databricks Connect connection and query real data.
"""

import os
import sys
from datetime import datetime

def test_databricks_connection():
    """Test Databricks Connect connection and query real data."""
    
    print("🔍 Testing Databricks Connect connection...")
    
    try:
        from databricks.connect import DatabricksSession
        from pyspark.sql.functions import col, count
        
        # Initialize Databricks Connect session
        spark = DatabricksSession.builder \
            .remote() \
            .getOrCreate()
        
        print("✅ Databricks Connect session created successfully")
        
        # Test querying tables
        tables_to_test = ['bronze_orders', 'silver_orders', 'gold_analytics', 'customers', 'products']
        
        for table_name in tables_to_test:
            try:
                print(f"🔍 Testing table: {table_name}")
                
                # Try to query the table
                df = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
                result = df.collect()
                row_count = result[0]['row_count']
                
                print(f"✅ Table {table_name}: {row_count} rows")
                
                # If it's a data table, show some sample data
                if table_name in ['bronze_orders', 'silver_orders', 'gold_analytics']:
                    sample_df = spark.sql(f"SELECT * FROM {table_name} LIMIT 3")
                    print(f"📊 Sample data from {table_name}:")
                    sample_df.show()
                
            except Exception as e:
                print(f"❌ Error accessing table {table_name}: {e}")
        
        # Test a more complex query
        try:
            print("\n🔍 Testing complex query on gold_analytics...")
            analytics_df = spark.sql("""
                SELECT 
                    product_category,
                    COUNT(*) as product_count,
                    SUM(total_revenue) as total_revenue
                FROM gold_analytics 
                GROUP BY product_category
                ORDER BY total_revenue DESC
            """)
            
            print("📊 Analytics summary:")
            analytics_df.show()
            
        except Exception as e:
            print(f"❌ Error in complex query: {e}")
        
        spark.stop()
        print("\n✅ Databricks Connect test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error connecting to Databricks: {e}")
        print("💡 Make sure you have configured Databricks Connect properly")
        return False
    
    return True

if __name__ == "__main__":
    success = test_databricks_connection()
    sys.exit(0 if success else 1) 