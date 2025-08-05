#!/usr/bin/env python3
"""
Test Real GCP Billing Data
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_real_billing():
    """Test real GCP billing data."""
    
    # Set the project ID
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    
    try:
        from dashboard.cloud_integrations import GoogleCloudIntegration
        
        print("🔍 TESTING REAL GCP BILLING DATA")
        print("=" * 50)
        
        # Test GCP integration
        gcp = GoogleCloudIntegration()
        
        if gcp.is_configured():
            print("✅ GCP is configured")
            
            # Get real billing data
            bigquery_data = gcp.get_bigquery_usage()
            storage_data = gcp.get_storage_usage()
            
            # Calculate real costs
            real_bq_cost = bigquery_data['cost'].sum() if not bigquery_data.empty else 0
            real_storage_cost = storage_data['cost_per_month'].sum() if not storage_data.empty else 0
            total_real_cost = real_bq_cost + real_storage_cost
            
            print(f"\n💰 REAL GCP BILLING DATA:")
            print(f"📊 BigQuery cost: ${real_bq_cost:.2f}")
            print(f"🗄️ Storage cost: ${real_storage_cost:.2f}")
            print(f"💳 Total cost: ${total_real_cost:.2f}")
            
            print(f"\n🔍 COMPARE TO YOUR GCP CONSOLE:")
            print(f"Your actual total cost should be: ${total_real_cost:.2f}")
            
            if total_real_cost == 0:
                print("🎉 Your GCP costs are $0.00 - this is correct!")
            else:
                print(f"💡 Your GCP costs are ${total_real_cost:.2f}")
                
        else:
            print("❌ GCP not configured")
            
    except Exception as e:
        print(f"❌ Error testing billing: {e}")

if __name__ == "__main__":
    test_real_billing() 