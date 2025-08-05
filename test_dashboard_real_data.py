#!/usr/bin/env python3
"""
Test Dashboard Real Data
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_dashboard_real_data():
    """Test if dashboard is showing real data."""
    
    # Set the project ID
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    
    try:
        from dashboard.cloud_integrations import DataSourceManager, CostMonitor
        
        print("🔍 TESTING DASHBOARD REAL DATA")
        print("=" * 50)
        
        # Test DataSourceManager
        print("\n📊 Testing DataSourceManager...")
        data_manager = DataSourceManager()
        retail_data = data_manager.get_data_for_source("retail")
        
        print(f"✅ Retail data shape: {retail_data.shape}")
        
        # Check for real data
        if 'is_real_data' in retail_data.columns:
            real_data_count = retail_data['is_real_data'].sum()
            print(f"🎯 Real data rows: {real_data_count}")
            print(f"📊 Sample data rows: {len(retail_data) - real_data_count}")
            
            if real_data_count > 0:
                real_gcp_data = retail_data[retail_data['is_real_data'] == True]
                print(f"💰 Real GCP total cost: ${real_gcp_data['total_cost'].sum():.2f}")
                print(f"📦 Real GCP data processed: {real_gcp_data['data_processed_gb'].sum():.2f} GB")
        else:
            print("⚠️ No 'is_real_data' column found")
        
        # Test CostMonitor
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
                    print(f"🎯 Real GCP cost data: {real_data}")
                else:
                    print("⚠️ No 'is_real_data' column in cost breakdown")
        
        # Test real data indicator logic
        print("\n🔍 Testing real data indicator...")
        real_gcp_data = False
        try:
            if not cost_breakdown.empty:
                gcp_costs = cost_breakdown[cost_breakdown['provider'] == 'gcp']
                if not gcp_costs.empty and 'is_real_data' in gcp_costs.columns and gcp_costs['is_real_data'].any():
                    real_gcp_data = True
        except Exception as e:
            print(f"⚠️ Error checking real data: {e}")
        
        print(f"🎯 Dashboard real data indicator: {real_gcp_data}")
        
        if real_gcp_data:
            print("🎉 SUCCESS! Dashboard is using real data!")
        else:
            print("❌ Dashboard is still using sample data")
            
    except Exception as e:
        print(f"❌ Error testing dashboard: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_dashboard_real_data() 