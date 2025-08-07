#!/usr/bin/env python3
"""
GCP Cost Monitoring CLI Tool

This CLI provides real GCP cost monitoring capabilities using BigQuery billing data export.
Based on Google Cloud documentation: https://cloud.google.com/billing/docs/how-to/bq-examples
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, date
import json

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from utils.logger import log_function_call


@log_function_call
def get_gcp_billing_data(project_id: str, days: int = 30) -> Dict[str, Any]:
    """
    Get real GCP billing data for the specified project using BigQuery billing export.
    
    Args:
        project_id: GCP project ID
        days: Number of days to analyze (default: 30)
        
    Returns:
        Dictionary containing GCP billing analysis
    """
    billing_data = {
        'timestamp': _get_current_timestamp(),
        'project_id': project_id,
        'period_days': days,
        'services': {},
        'total_cost': 0,
        'is_real_data': True,
        'recommendations': []
    }
    
    try:
        from google.cloud import bigquery
        from google.auth import default
        
        print(f"🔍 Fetching real GCP billing data for project: {project_id}")
        
        # Get credentials
        credentials, project = default()
        
        # Initialize BigQuery client
        bq_client = bigquery.Client(credentials=credentials, project=project)
        
        # Get billing account ID
        billing_account_id = _get_billing_account_id(bq_client, project_id)
        
        if not billing_account_id:
            billing_data['error'] = "No billing account found or billing export not enabled"
            return billing_data
        
        # Query Standard usage cost data
        standard_costs = _get_standard_usage_costs(bq_client, project_id, billing_account_id, days)
        
        # Query Detailed usage cost data (if available)
        detailed_costs = _get_detailed_usage_costs(bq_client, project_id, billing_account_id, days)
        
        # Combine costs
        all_costs = {**standard_costs, **detailed_costs}
        
        # Add costs to billing data
        for service, cost_info in all_costs.items():
            if cost_info['cost'] > 0:
                billing_data['services'][service] = {
                    'cost': cost_info['cost'],
                    'description': cost_info['description'],
                    'usage': cost_info.get('usage', 0),
                    'sku': cost_info.get('sku', 'Unknown')
                }
                billing_data['total_cost'] += cost_info['cost']
        
        # Generate cost optimization recommendations
        if billing_data['total_cost'] > 100:
            billing_data['recommendations'].append('Consider reviewing BigQuery query optimization')
        
        storage_cost = all_costs.get('Cloud Storage', {}).get('cost', 0)
        if storage_cost > 50:
            billing_data['recommendations'].append('Consider lifecycle policies for Cloud Storage')
        
        compute_cost = all_costs.get('Compute Engine', {}).get('cost', 0)
        if compute_cost > 0:
            billing_data['recommendations'].append('Review Compute Engine usage and consider committed use discounts')
        
        bigquery_cost = all_costs.get('BigQuery', {}).get('cost', 0)
        if bigquery_cost > 0:
            billing_data['recommendations'].append('Optimize BigQuery queries and consider slot reservations')
        
        return billing_data
        
    except Exception as e:
        billing_data['error'] = f"Error fetching GCP billing data: {str(e)}"
        return billing_data


@log_function_call
def _get_billing_account_id(bq_client, project_id: str) -> Optional[str]:
    """Get billing account ID from BigQuery billing export tables."""
    try:
        # List datasets to find billing export
        datasets = list(bq_client.list_datasets())
        
        for dataset in datasets:
            if 'billing' in dataset.dataset_id.lower() or 'export' in dataset.dataset_id.lower():
                # List tables in the dataset
                tables = list(bq_client.list_tables(dataset.dataset_id))
                
                for table in tables:
                    # Look for Standard usage cost table pattern
                    if table.table_id.startswith('gcp_billing_export_v1_'):
                        # Extract billing account ID from table name
                        billing_account_id = table.table_id.replace('gcp_billing_export_v1_', '')
                        print(f"✅ Found billing account ID: {billing_account_id}")
                        return billing_account_id
        
        print("❌ No billing export tables found")
        return None
        
    except Exception as e:
        print(f"❌ Error getting billing account ID: {e}")
        return None


@log_function_call
def _get_standard_usage_costs(bq_client, project_id: str, billing_account_id: str, days: int) -> Dict[str, Dict[str, Any]]:
    """Get Standard usage cost data from BigQuery billing export."""
    costs = {}
    
    try:
        # Find the Standard usage cost table
        table_name = f"gcp_billing_export_v1_{billing_account_id}"
        
        # Query for the last N days
        query = f"""
        SELECT 
            service.description as service_name,
            sku.description as sku_description,
            SUM(cost) as total_cost,
            SUM(usage.amount) as total_usage,
            COUNT(*) as record_count
        FROM `{project_id}.billing_export.{table_name}`
        WHERE DATE(usage_end_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY service_name, sku_description
        HAVING total_cost > 0
        ORDER BY total_cost DESC
        """
        
        print(f"🔍 Querying Standard usage cost data...")
        query_job = bq_client.query(query)
        results = query_job.result()
        
        for row in results:
            service_name = row.service_name or 'Unknown Service'
            costs[service_name] = {
                'cost': float(row.total_cost),
                'description': row.sku_description or 'No description',
                'usage': float(row.total_usage) if row.total_usage else 0,
                'sku': row.sku_description or 'Unknown'
            }
        
        print(f"✅ Found {len(costs)} services in Standard usage cost data")
        
    except Exception as e:
        print(f"❌ Error querying Standard usage cost data: {e}")
    
    return costs


@log_function_call
def _get_detailed_usage_costs(bq_client, project_id: str, billing_account_id: str, days: int) -> Dict[str, Dict[str, Any]]:
    """Get Detailed usage cost data from BigQuery billing export."""
    costs = {}
    
    try:
        # Find the Detailed usage cost table
        table_name = f"gcp_billing_export_resource_v1_{billing_account_id}"
        
        # Query for the last N days
        query = f"""
        SELECT 
            service.description as service_name,
            sku.description as sku_description,
            SUM(cost) as total_cost,
            SUM(usage.amount) as total_usage,
            COUNT(*) as record_count
        FROM `{project_id}.billing_export.{table_name}`
        WHERE DATE(usage_end_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY service_name, sku_description
        HAVING total_cost > 0
        ORDER BY total_cost DESC
        """
        
        print(f"🔍 Querying Detailed usage cost data...")
        query_job = bq_client.query(query)
        results = query_job.result()
        
        for row in results:
            service_name = row.service_name or 'Unknown Service'
            # If service already exists, add costs
            if service_name in costs:
                costs[service_name]['cost'] += float(row.total_cost)
                costs[service_name]['usage'] += float(row.total_usage) if row.total_usage else 0
            else:
                costs[service_name] = {
                    'cost': float(row.total_cost),
                    'description': row.sku_description or 'No description',
                    'usage': float(row.total_usage) if row.total_usage else 0,
                    'sku': row.sku_description or 'Unknown'
                }
        
        print(f"✅ Found {len(costs)} services in Detailed usage cost data")
        
    except Exception as e:
        print(f"❌ Error querying Detailed usage cost data: {e}")
    
    return costs


@log_function_call
def get_cost_trends(project_id: str, days: int = 30) -> Dict[str, Any]:
    """
    Get GCP cost trends over time using BigQuery billing export.
    
    Args:
        project_id: GCP project ID
        days: Number of days to analyze (default: 30)
        
    Returns:
        Dictionary containing cost trends analysis
    """
    trends_data = {
        'timestamp': _get_current_timestamp(),
        'project_id': project_id,
        'period_days': days,
        'daily_costs': [],
        'total_cost': 0,
        'average_daily_cost': 0,
        'cost_variance': 0,
        'is_real_data': True
    }
    
    try:
        from google.cloud import bigquery
        from google.auth import default
        
        # Get credentials
        credentials, project = default()
        
        # Initialize BigQuery client
        bq_client = bigquery.Client(credentials=credentials, project=project)
        
        # Get billing account ID
        billing_account_id = _get_billing_account_id(bq_client, project_id)
        
        if not billing_account_id:
            trends_data['error'] = "No billing account found or billing export not enabled"
            return trends_data
        
        # Query daily costs
        daily_costs = _get_daily_cost_trends(bq_client, project_id, billing_account_id, days)
        
        trends_data['daily_costs'] = daily_costs
        trends_data['total_cost'] = sum(day['cost'] for day in daily_costs)
        trends_data['average_daily_cost'] = trends_data['total_cost'] / len(daily_costs) if daily_costs else 0
        
        # Calculate variance
        if daily_costs:
            costs = [day['cost'] for day in daily_costs]
            mean_cost = trends_data['average_daily_cost']
            variance = sum((cost - mean_cost) ** 2 for cost in costs) / len(costs)
            trends_data['cost_variance'] = variance
        
        return trends_data
        
    except Exception as e:
        trends_data['error'] = f"Error fetching GCP cost trends: {str(e)}"
        return trends_data


@log_function_call
def _get_daily_cost_trends(bq_client, project_id: str, billing_account_id: str, days: int) -> List[Dict[str, Any]]:
    """Get daily cost trends from BigQuery billing export."""
    daily_costs = []
    
    try:
        # Try Standard usage cost table first
        table_name = f"gcp_billing_export_v1_{billing_account_id}"
        
        query = f"""
        SELECT 
            DATE(usage_end_time) as date,
            SUM(cost) as daily_cost,
            COUNT(*) as record_count
        FROM `{project_id}.billing_export.{table_name}`
        WHERE DATE(usage_end_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY date
        ORDER BY date
        """
        
        print(f"🔍 Querying daily cost trends...")
        query_job = bq_client.query(query)
        results = query_job.result()
        
        for row in results:
            daily_costs.append({
                'date': row.date.strftime('%Y-%m-%d'),
                'cost': float(row.daily_cost),
                'record_count': row.record_count
            })
        
        print(f"✅ Found {len(daily_costs)} days of cost data")
        
    except Exception as e:
        print(f"❌ Error querying daily cost trends: {e}")
    
    return daily_costs


def print_gcp_cost_report(cost_data: Dict[str, Any]) -> None:
    """Print a formatted GCP cost report."""
    print("\n" + "="*60)
    print("🔍 GCP COST ANALYSIS REPORT")
    print("="*60)
    
    if 'error' in cost_data:
        print(f"❌ Error: {cost_data['error']}")
        return
    
    print(f"📊 Project: {cost_data['project_id']}")
    print(f"📅 Period: {cost_data['period_days']} days")
    print(f"💰 Total Cost: ${cost_data['total_cost']:.2f}")
    print(f"🕒 Timestamp: {cost_data['timestamp']}")
    print(f"✅ Real Data: {cost_data['is_real_data']}")
    
    if cost_data['services']:
        print("\n📈 SERVICE BREAKDOWN:")
        print("-" * 40)
        for service, info in cost_data['services'].items():
            print(f"🔹 {service}: ${info['cost']:.2f}")
            if info.get('description'):
                print(f"   Description: {info['description']}")
    
    if cost_data['recommendations']:
        print("\n💡 RECOMMENDATIONS:")
        print("-" * 40)
        for rec in cost_data['recommendations']:
            print(f"• {rec}")
    
    print("="*60)


def print_cost_trends_report(trends_data: Dict[str, Any]) -> None:
    """Print a formatted GCP cost trends report."""
    print("\n" + "="*60)
    print("📈 GCP COST TRENDS REPORT")
    print("="*60)
    
    if 'error' in trends_data:
        print(f"❌ Error: {trends_data['error']}")
        return
    
    print(f"📊 Project: {trends_data['project_id']}")
    print(f"📅 Period: {trends_data['period_days']} days")
    print(f"💰 Total Cost: ${trends_data['total_cost']:.2f}")
    print(f"📊 Average Daily Cost: ${trends_data['average_daily_cost']:.2f}")
    print(f"📈 Cost Variance: ${trends_data['cost_variance']:.2f}")
    print(f"✅ Real Data: {trends_data['is_real_data']}")
    
    if trends_data['daily_costs']:
        print("\n📅 DAILY COST BREAKDOWN:")
        print("-" * 40)
        for day in trends_data['daily_costs'][-10:]:  # Show last 10 days
            print(f"📅 {day['date']}: ${day['cost']:.2f}")
    
    print("="*60)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='GCP Cost Monitoring CLI')
    parser.add_argument('--project-id', required=True, help='GCP Project ID')
    parser.add_argument('--days', type=int, default=30, help='Number of days to analyze (default: 30)')
    parser.add_argument('--trends', action='store_true', help='Show cost trends instead of current costs')
    parser.add_argument('--output', choices=['text', 'json'], default='text', help='Output format')
    
    args = parser.parse_args()
    
    # Check if GCP is configured
    if not _is_gcp_configured():
        print("❌ GCP is not properly configured. Please set up authentication.")
        sys.exit(1)
    
    try:
        if args.trends:
            # Get cost trends
            trends_data = get_cost_trends(args.project_id, args.days)
            
            if args.output == 'json':
                print(json.dumps(trends_data, indent=2))
            else:
                print_cost_trends_report(trends_data)
        else:
            # Get current costs
            cost_data = get_gcp_billing_data(args.project_id, args.days)
            
            if args.output == 'json':
                print(json.dumps(cost_data, indent=2))
            else:
                print_gcp_cost_report(cost_data)
                
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def _is_gcp_configured() -> bool:
    """Check if GCP is properly configured."""
    try:
        from google.auth import default
        credentials, project = default()
        return credentials is not None
    except Exception:
        return False


def _get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now().isoformat()


if __name__ == "__main__":
    main()
