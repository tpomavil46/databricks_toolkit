#!/usr/bin/env python3
"""
Debug Real Data Access
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def debug_real_data():
    """Debug why real data isn't being retrieved."""
    
    # Set the project ID
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    
    try:
        from dashboard.cloud_integrations import GoogleCloudIntegration, CostMonitor
        
        print("🔍 DEBUGGING REAL DATA ACCESS")
        print("=" * 50)
        
        # Test Google Cloud integration
        gcp = GoogleCloudIntegration()
        
        print(f"✅ GCP configured: {gcp.is_configured()}")
        print(f"📋 Project ID: {gcp.project_id}")
        
        if gcp.is_configured():
            print("\n📊 Testing BigQuery access...")
            bigquery_data = gcp.get_bigquery_usage()
            print(f"✅ BigQuery data shape: {bigquery_data.shape}")
            print(f"💰 Total BigQuery cost: ${bigquery_data['cost'].sum():.2f}")
            
            # Check if we have real data indicator
            if 'is_real_data' in bigquery_data.columns:
                real_data_count = bigquery_data['is_real_data'].sum()
                print(f"🎯 Real data rows: {real_data_count}")
                print(f"📊 Sample data rows: {len(bigquery_data) - real_data_count}")
            else:
                print("⚠️ No 'is_real_data' column found")
            
            print("\n🗄️ Testing Storage access...")
            storage_data = gcp.get_storage_usage()
            print(f"✅ Storage data shape: {storage_data.shape}")
            if not storage_data.empty:
                print(f"📦 Total buckets: {len(storage_data)}")
                print(f"💾 Total storage cost: ${storage_data['cost_per_month'].sum():.2f}")
            
            print("\n💰 Testing cost monitoring...")
            cost_monitor = CostMonitor()
            cost_breakdown = cost_monitor.get_total_cost_breakdown()
            print(f"✅ Cost breakdown shape: {cost_breakdown.shape}")
            
            if not cost_breakdown.empty:
                gcp_costs = cost_breakdown[cost_breakdown['provider'] == 'gcp']
                if not gcp_costs.empty:
                    if 'is_real_data' in gcp_costs.columns:
                        real_data = gcp_costs['is_real_data'].any()
                        print(f"🎯 Real GCP cost data: {real_data}")
                        if real_data:
                            print(f"💰 Your actual GCP cost: ${gcp_costs['cost'].sum():.2f}")
                        else:
                            print("📊 Using sample GCP cost data")
                    else:
                        print("⚠️ No 'is_real_data' column in cost breakdown")
                else:
                    print("📊 No GCP costs found")
            
        else:
            print("❌ GCP not configured properly")
            
    except Exception as e:
        print(f"❌ Error debugging real data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_real_data() 