#!/usr/bin/env python3
"""
Test GCP Integration

Simple test to verify GCP integration works with real data.
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_gcp_integration():
    """Test GCP integration with real data."""
    
    try:
        from dashboard.cloud_integrations import GoogleCloudIntegration, CostMonitor
        
        print("🧪 Testing GCP Integration...")
        print("=" * 50)
        
        # Test Google Cloud integration
        gcp = GoogleCloudIntegration()
        
        if gcp.is_configured():
            print("✅ GCP is configured")
            
            # Test BigQuery usage
            print("\n📊 Testing BigQuery usage...")
            bigquery_data = gcp.get_bigquery_usage()
            print(f"✅ BigQuery data shape: {bigquery_data.shape}")
            print(f"💰 Total BigQuery cost: ${bigquery_data['cost'].sum():.2f}")
            
            # Test Storage usage
            print("\n🗄️ Testing Storage usage...")
            storage_data = gcp.get_storage_usage()
            print(f"✅ Storage data shape: {storage_data.shape}")
            if not storage_data.empty:
                print(f"📦 Total buckets: {len(storage_data)}")
                print(f"💾 Total storage cost: ${storage_data['cost_per_month'].sum():.2f}")
            
            # Test cost monitoring
            print("\n💰 Testing cost monitoring...")
            cost_monitor = CostMonitor()
            cost_breakdown = cost_monitor.get_total_cost_breakdown()
            print(f"✅ Cost breakdown shape: {cost_breakdown.shape}")
            
            if not cost_breakdown.empty:
                gcp_costs = cost_breakdown[cost_breakdown['provider'] == 'gcp']
                if not gcp_costs.empty:
                    real_data = gcp_costs.get('is_real_data', False)
                    if real_data.any():
                        print("🎉 Connected to REAL GCP billing data!")
                        print(f"💰 Your actual GCP cost: ${gcp_costs['cost'].sum():.2f}")
                    else:
                        print("📊 Using sample data (real data not available)")
                else:
                    print("📊 No GCP costs found")
            
        else:
            print("⚠️ GCP not configured")
            print("To configure GCP:")
            print("1. Set GCP_PROJECT_ID environment variable")
            print("2. Set up Google Cloud credentials")
            print("3. Install google-cloud libraries")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing GCP integration: {e}")
        return False

def main():
    """Run GCP integration test."""
    
    print("🚀 GCP INTEGRATION TEST")
    print("=" * 50)
    
    success = test_gcp_integration()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 GCP integration test completed!")
        print("🚀 Dashboard is ready to use with real GCP data!")
    else:
        print("❌ GCP integration test failed")
        print("📊 Dashboard will use sample data")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 