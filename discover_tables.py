#!/usr/bin/env python3
"""
Script to discover what tables exist in the Databricks workspace.
"""

import os
import sys
from datetime import datetime

def discover_tables():
    """Discover what tables exist in the Databricks workspace."""
    
    print("🔍 Discovering tables in your Databricks workspace...")
    
    try:
        from databricks.connect import DatabricksSession
        
        # Initialize Databricks Connect session
        spark = DatabricksSession.builder \
            .remote() \
            .getOrCreate()
        
        print("✅ Databricks Connect session created successfully")
        
        # Get current catalog and schema
        try:
            current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
            current_schema = spark.sql("SELECT current_schema()").collect()[0][0]
            print(f"📂 Current catalog: {current_catalog}")
            print(f"📂 Current schema: {current_schema}")
        except Exception as e:
            print(f"⚠️ Could not get current catalog/schema: {e}")
            current_catalog = "hive_metastore"
            current_schema = "default"
        
        # List all catalogs
        try:
            print("\n📚 Available catalogs:")
            catalogs_df = spark.sql("SHOW CATALOGS")
            catalogs_df.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not list catalogs: {e}")
        
        # List all schemas in current catalog
        try:
            print(f"\n📁 Available schemas in {current_catalog}:")
            schemas_df = spark.sql(f"SHOW SCHEMAS IN {current_catalog}")
            schemas_df.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not list schemas: {e}")
        
        # List all tables in current schema
        try:
            print(f"\n📋 Available tables in {current_catalog}.{current_schema}:")
            tables_df = spark.sql(f"SHOW TABLES IN {current_catalog}.{current_schema}")
            tables_df.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not list tables: {e}")
        
        # Try to find any tables with 'order' in the name
        try:
            print(f"\n🔍 Searching for tables with 'order' in the name:")
            order_tables = spark.sql(f"""
                SELECT table_name 
                FROM {current_catalog}.information_schema.tables 
                WHERE table_name LIKE '%order%' 
                AND table_schema = '{current_schema}'
            """)
            order_tables.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not search for order tables: {e}")
        
        # Try to find any tables with 'customer' in the name
        try:
            print(f"\n🔍 Searching for tables with 'customer' in the name:")
            customer_tables = spark.sql(f"""
                SELECT table_name 
                FROM {current_catalog}.information_schema.tables 
                WHERE table_name LIKE '%customer%' 
                AND table_schema = '{current_schema}'
            """)
            customer_tables.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not search for customer tables: {e}")
        
        # Try to find any tables with 'product' in the name
        try:
            print(f"\n🔍 Searching for tables with 'product' in the name:")
            product_tables = spark.sql(f"""
                SELECT table_name 
                FROM {current_catalog}.information_schema.tables 
                WHERE table_name LIKE '%product%' 
                AND table_schema = '{current_schema}'
            """)
            product_tables.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not search for product tables: {e}")
        
        # List all tables in information_schema
        try:
            print(f"\n📊 All tables in information_schema:")
            all_tables = spark.sql(f"""
                SELECT table_schema, table_name, table_type
                FROM {current_catalog}.information_schema.tables 
                WHERE table_schema = '{current_schema}'
                ORDER BY table_name
            """)
            all_tables.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Could not list all tables: {e}")
        
        spark.stop()
        print("\n✅ Table discovery completed!")
        
    except Exception as e:
        print(f"❌ Error connecting to Databricks: {e}")
        print("💡 Make sure you have configured Databricks Connect properly")
        return False
    
    return True

if __name__ == "__main__":
    success = discover_tables()
    sys.exit(0 if success else 1) 