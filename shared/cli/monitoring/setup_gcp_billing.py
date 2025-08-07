#!/usr/bin/env python3
"""
GCP Billing Setup Script

This script helps set up GCP billing export to BigQuery so we can get real cost data.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Any

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def setup_billing_export(project_id: str, dataset_id: str = "billing_export") -> Dict[str, Any]:
    """
    Set up GCP billing export to BigQuery.
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID for billing export
        
    Returns:
        Dictionary containing setup results
    """
    setup_results = {
        'project_id': project_id,
        'dataset_id': dataset_id,
        'status': 'unknown',
        'steps_completed': [],
        'errors': []
    }
    
    try:
        print(f"🔧 Setting up GCP billing export for project: {project_id}")
        
        # Step 1: Enable required APIs
        print("📋 Step 1: Enabling required APIs...")
        try:
            os.system(f"gcloud services enable billingbudgets.googleapis.com --project={project_id}")
            os.system(f"gcloud services enable bigquery.googleapis.com --project={project_id}")
            os.system(f"gcloud services enable cloudbilling.googleapis.com --project={project_id}")
            setup_results['steps_completed'].append("Enabled required APIs")
        except Exception as e:
            setup_results['errors'].append(f"Failed to enable APIs: {e}")
        
        # Step 2: Create BigQuery dataset
        print("📋 Step 2: Creating BigQuery dataset...")
        try:
            os.system(f"bq mk --project_id={project_id} {project_id}:{dataset_id}")
            setup_results['steps_completed'].append("Created BigQuery dataset")
        except Exception as e:
            setup_results['errors'].append(f"Failed to create dataset: {e}")
        
        # Step 3: Set up billing export
        print("📋 Step 3: Setting up billing export...")
        try:
            # Get billing account
            from google.cloud import billing_v1
            from google.auth import default
            
            credentials, project = default()
            billing_client = billing_v1.CloudBillingClient(credentials=credentials)
            
            billing_accounts = billing_client.list_billing_accounts()
            billing_account = None
            for account in billing_accounts:
                billing_account = account
                break
            
            if billing_account:
                print(f"✅ Found billing account: {billing_account.name}")
                setup_results['steps_completed'].append(f"Found billing account: {billing_account.name}")
                
                # Note: Setting up billing export requires specific permissions
                print("ℹ️ To complete billing export setup, run these commands manually:")
                print(f"gcloud billing accounts list")
                print(f"gcloud billing export create --billing-account={billing_account.name} --dataset={dataset_id} --project={project_id}")
                
            else:
                setup_results['errors'].append("No billing accounts found")
                
        except Exception as e:
            setup_results['errors'].append(f"Failed to set up billing export: {e}")
        
        # Step 4: Grant necessary permissions
        print("📋 Step 4: Setting up permissions...")
        try:
            # Get current user
            current_user = os.popen("gcloud auth list --filter=status:ACTIVE --format='value(account)'").read().strip()
            
            if current_user:
                print(f"✅ Current user: {current_user}")
                setup_results['steps_completed'].append(f"Identified current user: {current_user}")
                
                # Grant BigQuery Admin role
                os.system(f"gcloud projects add-iam-policy-binding {project_id} --member=user:{current_user} --role=roles/bigquery.admin")
                setup_results['steps_completed'].append("Granted BigQuery Admin role")
                
                # Grant Billing Account User role
                if billing_account:
                    os.system(f"gcloud billing accounts add-iam-policy-binding {billing_account.name} --member=user:{current_user} --role=roles/billing.user")
                    setup_results['steps_completed'].append("Granted Billing Account User role")
                
            else:
                setup_results['errors'].append("Could not identify current user")
                
        except Exception as e:
            setup_results['errors'].append(f"Failed to set up permissions: {e}")
        
        if not setup_results['errors']:
            setup_results['status'] = 'success'
            print("✅ Billing export setup completed successfully!")
        else:
            setup_results['status'] = 'partial'
            print("⚠️ Billing export setup completed with some errors")
            
        return setup_results
        
    except Exception as e:
        setup_results['status'] = 'failed'
        setup_results['errors'].append(f"Setup failed: {e}")
        return setup_results


def check_billing_setup(project_id: str) -> Dict[str, Any]:
    """
    Check if billing export is properly set up.
    
    Args:
        project_id: GCP project ID
        
    Returns:
        Dictionary containing check results
    """
    check_results = {
        'project_id': project_id,
        'billing_export_available': False,
        'dataset_exists': False,
        'permissions_ok': False,
        'details': {}
    }
    
    try:
        print(f"🔍 Checking billing setup for project: {project_id}")
        
        # Check if billing export dataset exists
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=project_id)
            
            # Try to access billing export dataset
            dataset_ref = client.dataset("billing_export", project=project_id)
            dataset = client.get_dataset(dataset_ref)
            
            if dataset:
                check_results['dataset_exists'] = True
                check_results['details']['dataset'] = f"✅ Dataset exists: {dataset.dataset_id}"
                print("✅ Billing export dataset exists")
            else:
                check_results['details']['dataset'] = "❌ Dataset does not exist"
                print("❌ Billing export dataset does not exist")
                
        except Exception as e:
            check_results['details']['dataset'] = f"❌ Error checking dataset: {e}"
            print(f"❌ Error checking dataset: {e}")
        
        # Check billing account access
        try:
            from google.cloud import billing_v1
            from google.auth import default
            
            credentials, project = default()
            billing_client = billing_v1.CloudBillingClient(credentials=credentials)
            
            billing_accounts = billing_client.list_billing_accounts()
            if billing_accounts:
                check_results['details']['billing'] = f"✅ Billing accounts accessible: {len(list(billing_accounts))} accounts"
                print("✅ Billing accounts accessible")
            else:
                check_results['details']['billing'] = "❌ No billing accounts found"
                print("❌ No billing accounts found")
                
        except Exception as e:
            check_results['details']['billing'] = f"❌ Error checking billing: {e}"
            print(f"❌ Error checking billing: {e}")
        
        # Check BigQuery access
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=project_id)
            
            # Try to list datasets
            datasets = list(client.list_datasets(max_results=5))
            check_results['details']['bigquery'] = f"✅ BigQuery accessible: {len(datasets)} datasets found"
            print("✅ BigQuery accessible")
            check_results['permissions_ok'] = True
            
        except Exception as e:
            check_results['details']['bigquery'] = f"❌ Error accessing BigQuery: {e}"
            print(f"❌ Error accessing BigQuery: {e}")
        
        # Overall assessment
        if check_results['dataset_exists'] and check_results['permissions_ok']:
            check_results['billing_export_available'] = True
            print("✅ Billing export is properly set up!")
        else:
            print("⚠️ Billing export needs setup")
        
        return check_results
        
    except Exception as e:
        check_results['details']['error'] = f"Check failed: {e}"
        return check_results


def print_setup_instructions(project_id: str):
    """Print manual setup instructions."""
    print("=" * 60)
    print("📋 MANUAL SETUP INSTRUCTIONS")
    print("=" * 60)
    print(f"Project: {project_id}")
    print()
    
    print("1. Enable required APIs:")
    print(f"   gcloud services enable billingbudgets.googleapis.com --project={project_id}")
    print(f"   gcloud services enable bigquery.googleapis.com --project={project_id}")
    print(f"   gcloud services enable cloudbilling.googleapis.com --project={project_id}")
    print()
    
    print("2. Create BigQuery dataset:")
    print(f"   bq mk --project_id={project_id} {project_id}:billing_export")
    print()
    
    print("3. List billing accounts:")
    print("   gcloud billing accounts list")
    print()
    
    print("4. Set up billing export (replace BILLING_ACCOUNT_ID):")
    print("   gcloud billing export create --billing-account=BILLING_ACCOUNT_ID --dataset=billing_export --project={project_id}")
    print()
    
    print("5. Grant necessary permissions:")
    print(f"   gcloud projects add-iam-policy-binding {project_id} --member=user:$(gcloud auth list --filter=status:ACTIVE --format='value(account)') --role=roles/bigquery.admin")
    print()
    
    print("6. Test the setup:")
    print(f"   python shared/cli/monitoring/gcp_cost_cli.py --project {project_id} --costs")
    print()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GCP Billing Setup Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_gcp_billing.py --project myproject --setup
  python setup_gcp_billing.py --project myproject --check
  python setup_gcp_billing.py --project myproject --instructions
        """
    )
    
    parser.add_argument(
        '--project', 
        required=True,
        help='GCP project ID'
    )
    
    parser.add_argument(
        '--setup',
        action='store_true',
        help='Set up billing export'
    )
    
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check billing setup status'
    )
    
    parser.add_argument(
        '--instructions',
        action='store_true',
        help='Print manual setup instructions'
    )
    
    args = parser.parse_args()
    
    # Check if GCP is configured
    try:
        from google.auth import default
        credentials, project = default()
        if not credentials:
            print("❌ GCP not configured. Please run:")
            print("   gcloud auth application-default login")
            print("   gcloud config set project YOUR_PROJECT_ID")
            return
    except Exception:
        print("❌ GCP not configured. Please run:")
        print("   gcloud auth application-default login")
        print("   gcloud config set project YOUR_PROJECT_ID")
        return
    
    if args.setup:
        print("🔧 Setting up GCP billing export...")
        results = setup_billing_export(args.project)
        
        print("\n📊 Setup Results:")
        print(f"Status: {results['status']}")
        print(f"Steps completed: {len(results['steps_completed'])}")
        print(f"Errors: {len(results['errors'])}")
        
        if results['steps_completed']:
            print("\n✅ Completed steps:")
            for step in results['steps_completed']:
                print(f"  - {step}")
        
        if results['errors']:
            print("\n❌ Errors:")
            for error in results['errors']:
                print(f"  - {error}")
    
    elif args.check:
        print("🔍 Checking GCP billing setup...")
        results = check_billing_setup(args.project)
        
        print("\n📊 Check Results:")
        print(f"Billing export available: {results['billing_export_available']}")
        print(f"Dataset exists: {results['dataset_exists']}")
        print(f"Permissions OK: {results['permissions_ok']}")
        
        print("\n📋 Details:")
        for key, detail in results['details'].items():
            print(f"  {key}: {detail}")
    
    elif args.instructions:
        print_setup_instructions(args.project)
    
    else:
        print("❌ Please specify an action: --setup, --check, or --instructions")


if __name__ == "__main__":
    main()
