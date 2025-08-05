#!/usr/bin/env python3
"""
Cost Monitoring CLI Tool

This CLI provides cost monitoring capabilities for Databricks workspace
including cluster costs, storage costs, and usage analytics.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timedelta

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from admin.core.admin_client import AdminClient
from utils.logger import log_function_call


@log_function_call
def get_cluster_cost_analysis(admin_client: AdminClient, days: int = 30) -> Dict[str, Any]:
    """
    Analyze cluster costs over a specified period.
    
    Args:
        admin_client: AdminClient instance
        days: Number of days to analyze (default: 30)
        
    Returns:
        Dictionary containing cluster cost analysis
    """
    cost_analysis = {
        'timestamp': _get_current_timestamp(),
        'period_days': days,
        'clusters': {},
        'total_estimated_cost': 0,
        'recommendations': []
    }
    
    try:
        clusters = admin_client.clusters.list_clusters()
        
        for cluster in clusters:
            cluster_id = cluster.get('cluster_id')
            cluster_name = cluster.get('cluster_name', 'Unknown')
            state = cluster.get('state', 'UNKNOWN')
            
            # Estimate costs based on cluster configuration
            config = cluster.get('cluster_config', {})
            node_type = config.get('node_type_id', 'unknown')
            num_workers = config.get('num_workers', 0)
            
            # Rough cost estimation (this would be replaced with actual billing API)
            estimated_cost = _estimate_cluster_cost(node_type, num_workers, state)
            
            cluster_info = {
                'name': cluster_name,
                'state': state,
                'node_type': node_type,
                'num_workers': num_workers,
                'estimated_cost': estimated_cost,
                'last_activity': cluster.get('last_activity_time'),
                'created_time': cluster.get('created_time')
            }
            
            cost_analysis['clusters'][cluster_id] = cluster_info
            cost_analysis['total_estimated_cost'] += estimated_cost
        
        # Generate cost optimization recommendations
        running_clusters = [c for c in clusters if c.get('state') == 'RUNNING']
        stopped_clusters = [c for c in clusters if c.get('state') == 'TERMINATED']
        
        if len(running_clusters) > 3:
            cost_analysis['recommendations'].append('Consider stopping unused clusters to reduce costs')
        
        if len(stopped_clusters) > 5:
            cost_analysis['recommendations'].append('Consider deleting terminated clusters to clean up workspace')
        
        return cost_analysis
        
    except Exception as e:
        cost_analysis['error'] = str(e)
        return cost_analysis


@log_function_call
def get_storage_cost_analysis(admin_client: AdminClient) -> Dict[str, Any]:
    """
    Analyze storage costs and usage.
    
    Args:
        admin_client: AdminClient instance
        
    Returns:
        Dictionary containing storage cost analysis
    """
    storage_analysis = {
        'timestamp': _get_current_timestamp(),
        'storage_usage': {},
        'total_estimated_cost': 0,
        'recommendations': []
    }
    
    try:
        # Get workspace information to analyze storage
        workspace_info = admin_client.workspace.get_workspace_info()
        
        # Analyze workspace objects (notebooks, files, etc.)
        workspace_stats = workspace_info.get('statistics', {})
        total_objects = workspace_stats.get('total_objects', 0)
        notebooks = workspace_stats.get('notebooks', 0)
        libraries = workspace_stats.get('libraries', 0)
        
        # Estimate storage costs (this would be replaced with actual billing API)
        estimated_storage_cost = _estimate_storage_cost(total_objects, notebooks, libraries)
        
        storage_analysis['storage_usage'] = {
            'total_objects': total_objects,
            'notebooks': notebooks,
            'libraries': libraries,
            'estimated_cost': estimated_storage_cost
        }
        
        storage_analysis['total_estimated_cost'] = estimated_storage_cost
        
        # Generate storage optimization recommendations
        if total_objects > 1000:
            storage_analysis['recommendations'].append('Consider cleaning up unused notebooks and files')
        
        if libraries > 50:
            storage_analysis['recommendations'].append('Review and remove unused libraries')
        
        return storage_analysis
        
    except Exception as e:
        storage_analysis['error'] = str(e)
        return storage_analysis


@log_function_call
def get_usage_analytics(admin_client: AdminClient, days: int = 30) -> Dict[str, Any]:
    """
    Get comprehensive usage analytics.
    
    Args:
        admin_client: AdminClient instance
        days: Number of days to analyze (default: 30)
        
    Returns:
        Dictionary containing usage analytics
    """
    usage_analytics = {
        'timestamp': _get_current_timestamp(),
        'period_days': days,
        'user_activity': {},
        'cluster_usage': {},
        'workspace_usage': {},
        'cost_trends': {},
        'recommendations': []
    }
    
    try:
        # Get user activity
        users = admin_client.users.list_users()
        active_users = [u for u in users if getattr(u, 'active', True)]
        
        usage_analytics['user_activity'] = {
            'total_users': len(users),
            'active_users': len(active_users),
            'inactive_users': len(users) - len(active_users),
            'utilization_rate': (len(active_users) / len(users)) * 100 if users else 0
        }
        
        # Get cluster usage
        clusters = admin_client.clusters.list_clusters()
        running_clusters = [c for c in clusters if getattr(c, 'state', 'UNKNOWN') == 'RUNNING']
        
        usage_analytics['cluster_usage'] = {
            'total_clusters': len(clusters),
            'running_clusters': len(running_clusters),
            'stopped_clusters': len(clusters) - len(running_clusters),
            'utilization_rate': (len(running_clusters) / len(clusters)) * 100 if clusters else 0
        }
        
        # Get workspace usage
        workspace_info = admin_client.workspace.get_workspace_info()
        workspace_stats = workspace_info.get('statistics', {})
        
        usage_analytics['workspace_usage'] = {
            'total_objects': workspace_stats.get('total_objects', 0),
            'notebooks': workspace_stats.get('notebooks', 0),
            'libraries': workspace_stats.get('libraries', 0),
            'avg_path_depth': workspace_stats.get('avg_path_depth', 0)
        }
        
        # Generate usage recommendations
        if usage_analytics['user_activity']['utilization_rate'] < 50:
            usage_analytics['recommendations'].append('Low user utilization - consider user training or cleanup')
        
        if usage_analytics['cluster_usage']['utilization_rate'] < 30:
            usage_analytics['recommendations'].append('Low cluster utilization - consider stopping unused clusters')
        
        if usage_analytics['workspace_usage']['total_objects'] > 1000:
            usage_analytics['recommendations'].append('High object count - consider workspace cleanup')
        
        return usage_analytics
        
    except Exception as e:
        usage_analytics['error'] = str(e)
        return usage_analytics


def print_cost_report(cost_results: Dict[str, Any]) -> None:
    """Print a formatted cost report."""
    print("💰 Cost Monitoring Report")
    print("=" * 50)
    
    if 'error' in cost_results:
        print(f"❌ Cost analysis failed: {cost_results['error']}")
        return
    
    if 'total_estimated_cost' in cost_results:
        print(f"💵 Total Estimated Cost: ${cost_results['total_estimated_cost']:.2f}")
    
    if 'clusters' in cost_results:
        print(f"\n🖥️  Cluster Costs:")
        for cluster_id, cluster_info in cost_results['clusters'].items():
            status_icon = "🟢" if cluster_info['state'] == 'RUNNING' else "🔴"
            print(f"   {status_icon} {cluster_info['name']} ({cluster_info['state']})")
            print(f"      Cost: ${cluster_info['estimated_cost']:.2f}")
            print(f"      Workers: {cluster_info['num_workers']}")
    
    if 'storage_usage' in cost_results:
        print(f"\n💾 Storage Usage:")
        storage = cost_results['storage_usage']
        print(f"   Total Objects: {storage['total_objects']}")
        print(f"   Notebooks: {storage['notebooks']}")
        print(f"   Libraries: {storage['libraries']}")
        print(f"   Estimated Cost: ${storage['estimated_cost']:.2f}")
    
    if cost_results.get('recommendations'):
        print(f"\n💡 Cost Optimization Recommendations:")
        for recommendation in cost_results['recommendations']:
            print(f"   • {recommendation}")


def print_usage_report(usage_results: Dict[str, Any]) -> None:
    """Print a formatted usage report."""
    print("📊 Usage Analytics Report")
    print("=" * 50)
    
    if 'error' in usage_results:
        print(f"❌ Usage analysis failed: {usage_results['error']}")
        return
    
    if 'user_activity' in usage_results:
        user_activity = usage_results['user_activity']
        print(f"👥 User Activity:")
        print(f"   Total Users: {user_activity['total_users']}")
        print(f"   Active Users: {user_activity['active_users']}")
        print(f"   Utilization Rate: {user_activity['utilization_rate']:.1f}%")
    
    if 'cluster_usage' in usage_results:
        cluster_usage = usage_results['cluster_usage']
        print(f"\n🖥️  Cluster Usage:")
        print(f"   Total Clusters: {cluster_usage['total_clusters']}")
        print(f"   Running Clusters: {cluster_usage['running_clusters']}")
        print(f"   Utilization Rate: {cluster_usage['utilization_rate']:.1f}%")
    
    if 'workspace_usage' in usage_results:
        workspace_usage = usage_results['workspace_usage']
        print(f"\n📁 Workspace Usage:")
        print(f"   Total Objects: {workspace_usage['total_objects']}")
        print(f"   Notebooks: {workspace_usage['notebooks']}")
        print(f"   Libraries: {workspace_usage['libraries']}")
        print(f"   Avg Path Depth: {workspace_usage['avg_path_depth']:.1f}")
    
    if usage_results.get('recommendations'):
        print(f"\n💡 Usage Optimization Recommendations:")
        for recommendation in usage_results['recommendations']:
            print(f"   • {recommendation}")


def _estimate_cluster_cost(node_type: str, num_workers: int, state: str) -> float:
    """Estimate cluster cost based on configuration."""
    # Rough cost estimation (would be replaced with actual billing API)
    base_costs = {
        'i3.xlarge': 0.50,
        'i3.2xlarge': 1.00,
        'i3.4xlarge': 2.00,
        'unknown': 0.75
    }
    
    hourly_rate = base_costs.get(node_type, base_costs['unknown'])
    
    if state == 'RUNNING':
        # Estimate 8 hours per day for running clusters
        daily_cost = hourly_rate * (num_workers + 1) * 8  # +1 for driver node
        return daily_cost * 30  # 30 days
    else:
        return 0.0


def _estimate_storage_cost(total_objects: int, notebooks: int, libraries: int) -> float:
    """Estimate storage cost based on usage."""
    # Rough storage cost estimation
    object_cost = total_objects * 0.001  # $0.001 per object
    notebook_cost = notebooks * 0.01     # $0.01 per notebook
    library_cost = libraries * 0.05      # $0.05 per library
    
    return object_cost + notebook_cost + library_cost


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Databricks Cost Monitoring Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get cluster cost analysis
  python cli/monitoring/cost_cli.py cluster-costs --days 30

  # Get storage cost analysis
  python cli/monitoring/cost_cli.py storage-costs

  # Get usage analytics
  python cli/monitoring/cost_cli.py usage-analytics --days 30
        """,
    )
    
    parser.add_argument(
        "command",
        choices=[
            "cluster-costs",
            "storage-costs",
            "usage-analytics"
        ],
        help="Cost monitoring command to execute"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to analyze (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Initialize admin client
    admin_client = AdminClient()
    
    if args.command == "cluster-costs":
        print("💰 Running Cluster Cost Analysis")
        print("=" * 50)
        cost_results = get_cluster_cost_analysis(admin_client, args.days)
        print_cost_report(cost_results)
        
    elif args.command == "storage-costs":
        print("💾 Running Storage Cost Analysis")
        print("=" * 50)
        cost_results = get_storage_cost_analysis(admin_client)
        print_cost_report(cost_results)
        
    elif args.command == "usage-analytics":
        print("📊 Running Usage Analytics")
        print("=" * 50)
        usage_results = get_usage_analytics(admin_client, args.days)
        print_usage_report(usage_results)
        
    else:
        print(f"❌ Unknown command: {args.command}")
        sys.exit(1)


def _get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now().isoformat()


if __name__ == "__main__":
    main() 