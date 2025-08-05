#!/usr/bin/env python3
"""
Debug Dashboard Data
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def debug_dashboard_data():
    """Debug what data the dashboard is actually receiving."""
    
    # Set the project ID
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    
    try:
        from dashboard.cloud_integrations import GoogleCloudIntegration, CostMonitor, DataSourceManager
        from dashboard.config import config
        
        print("🔍 DEBUGGING DASHBOARD DATA")
        print("=" * 50)
        
        # Test what the dashboard actually receives
        print("\n📊 Testing DataSourceManager...")
        data_manager = DataSourceManager()
        
        # Test retail data source
        retail_data = data_manager.get_data_for_source("retail")
        print(f"✅ Retail data shape: {retail_data.shape}")
        print(f"💰 Total revenue: ${retail_data['total_revenue'].sum():.2f}")
        print(f"💸 Total cost: ${retail_data['total_cost'].sum():.2f}")
        
        # Check if this is real or sample data
        if 'cloud_provider' in retail_data.columns:
            gcp_data = retail_data[retail_data['cloud_provider'] == 'gcp']
            print(f"📊 GCP data rows: {len(gcp_data)}")
            if not gcp_data.empty:
                print(f"💰 GCP total cost: ${gcp_data['total_cost'].sum():.2f}")
        
        print("\n💰 Testing CostMonitor...")
        cost_monitor = CostMonitor()
        cost_breakdown = cost_monitor.get_total_cost_breakdown()
        print(f"✅ Cost breakdown shape: {cost_breakdown.shape}")
        
        if not cost_breakdown.empty:
            gcp_costs = cost_breakdown[cost_breakdown['provider'] == 'gcp']
            if not gcp_costs.empty:
                print(f"🎯 GCP costs found: {len(gcp_costs)}")
                print(f"💰 GCP total cost: ${gcp_costs['cost'].sum():.2f}")
                if 'is_real_data' in gcp_costs.columns:
                    real_data = gcp_costs['is_real_data'].any()
                    print(f"🎯 Real data: {real_data}")
                else:
                    print("⚠️ No 'is_real_data' column")
        
        print("\n🔍 Testing real data indicator logic...")
        real_gcp_data = False
        try:
            if not cost_breakdown.empty:
                gcp_costs = cost_breakdown[cost_breakdown['provider'] == 'gcp']
                if not gcp_costs.empty and 'is_real_data' in gcp_costs.columns and gcp_costs['is_real_data'].any():
                    real_gcp_data = True
        except Exception as e:
            print(f"⚠️ Error checking real data: {e}")
            real_gcp_data = False
        
        print(f"🎯 Dashboard real data indicator: {real_gcp_data}")
        
        # Test the actual GCP integration
        print("\n☁️ Testing GCP Integration...")
        gcp = GoogleCloudIntegration()
        
        if gcp.is_configured():
            print("✅ GCP is configured")
            
            # Test BigQuery
            bigquery_data = gcp.get_bigquery_usage()
            print(f"📊 BigQuery data shape: {bigquery_data.shape}")
            print(f"💰 BigQuery total cost: ${bigquery_data['cost'].sum():.2f}")
            
            if 'is_real_data' in bigquery_data.columns:
                real_bq_data = bigquery_data['is_real_data'].sum()
                print(f"🎯 Real BigQuery data rows: {real_bq_data}")
            
            # Test Storage
            storage_data = gcp.get_storage_usage()
            print(f"🗄️ Storage data shape: {storage_data.shape}")
            print(f"💾 Storage total cost: ${storage_data['cost_per_month'].sum():.2f}")
            
        else:
            print("❌ GCP not configured")
            
    except Exception as e:
        print(f"❌ Error debugging dashboard: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_dashboard_data() 