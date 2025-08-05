#!/usr/bin/env python3
"""
Test My GCP Connection
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_my_gcp():
    """Test GCP connection with mydatabrickssandbox project."""
    
    # Set the project ID
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    print(f"🔧 Setting GCP_PROJECT_ID to: {os.environ.get('GCP_PROJECT_ID')}")
    
    try:
        from dashboard.cloud_integrations import GoogleCloudIntegration
        
        print("🧪 Testing My GCP Connection...")
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
            
            print("\n🗄️ Testing Storage access...")
            storage_data = gcp.get_storage_usage()
            print(f"✅ Storage data shape: {storage_data.shape}")
            if not storage_data.empty:
                print(f"📦 Total buckets: {len(storage_data)}")
                print(f"💾 Total storage cost: ${storage_data['cost_per_month'].sum():.2f}")
            
            print("\n🎉 SUCCESS! Your real GCP data is accessible!")
            return True
        else:
            print("❌ GCP not configured properly")
            return False
            
    except Exception as e:
        print(f"❌ Error testing GCP connection: {e}")
        return False

if __name__ == "__main__":
    success = test_my_gcp()
    sys.exit(0 if success else 1) 