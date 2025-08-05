#!/usr/bin/env python3
"""
Health Monitoring CLI Tool

This CLI provides comprehensive health monitoring capabilities for
Databricks workspace including cluster health, user health, and system health.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, List

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from admin.core.admin_client import AdminClient
from utils.logger import log_function_call


@log_function_call
def run_system_health_check(admin_client: AdminClient) -> Dict[str, Any]:
    """
    Run comprehensive system health check.
    
    Args:
        admin_client: AdminClient instance
        
    Returns:
        Dictionary containing health check results
    """
    health_results = {
        'timestamp': _get_current_timestamp(),
        'overall_status': 'UNKNOWN',
        'checks': {},
        'score': 0,
        'recommendations': []
    }
    
    try:
        # Check 1: Workspace Health
        workspace_info = admin_client.workspace.get_workspace_info()
        workspace_health = {
            'status': 'PASS' if workspace_info.get('capabilities', {}).get('can_access_workspace') else 'FAIL',
            'description': 'Workspace accessibility check',
            'details': workspace_info
        }
        health_results['checks']['workspace_health'] = workspace_health
        
        # Check 2: User Health
        users = admin_client.users.list_users()
        active_users = [u for u in users if getattr(u, 'active', True)]
        user_health = {
            'status': 'PASS' if len(active_users) > 0 else 'WARN',
            'description': f'User health check ({len(active_users)} active users)',
            'details': {'total_users': len(users), 'active_users': len(active_users)}
        }
        health_results['checks']['user_health'] = user_health
        
        # Check 3: Cluster Health
        clusters = admin_client.clusters.list_clusters()
        running_clusters = [c for c in clusters if getattr(c, 'state', 'UNKNOWN') == 'RUNNING']
        cluster_health = {
            'status': 'PASS' if len(running_clusters) > 0 else 'WARN',
            'description': f'Cluster health check ({len(running_clusters)} running clusters)',
            'details': {'total_clusters': len(clusters), 'running_clusters': len(running_clusters)}
        }
        health_results['checks']['cluster_health'] = cluster_health
        
        # Check 4: Security Health
        security_audit = admin_client.security.run_security_audit()
        security_health = {
            'status': security_audit.get('overall', {}).get('status', 'UNKNOWN'),
            'description': 'Security health check',
            'details': security_audit
        }
        health_results['checks']['security_health'] = security_health
        
        # Check 5: Privilege Health
        privilege_summary = admin_client.privileges.get_privilege_summary()
        privilege_health = {
            'status': 'PASS' if privilege_summary.get('total_users', 0) > 0 else 'WARN',
            'description': f'Privilege health check ({privilege_summary.get("total_users", 0)} users)',
            'details': privilege_summary
        }
        health_results['checks']['privilege_health'] = privilege_health
        
        # Calculate overall health score
        passed_checks = sum(1 for check in health_results['checks'].values() if check['status'] == 'PASS')
        total_checks = len(health_results['checks'])
        health_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        
        health_results['score'] = health_score
        health_results['overall_status'] = 'PASS' if health_score >= 80 else 'WARN' if health_score >= 60 else 'FAIL'
        
        # Generate recommendations
        if len(running_clusters) == 0:
            health_results['recommendations'].append('Consider starting at least one cluster for data processing')
        
        if len(active_users) == 0:
            health_results['recommendations'].append('No active users found - check user management')
            
        if security_audit.get('overall', {}).get('status') != 'PASS':
            health_results['recommendations'].append('Security audit failed - review security configuration')
        
        return health_results
        
    except Exception as e:
        health_results['overall_status'] = 'ERROR'
        health_results['error'] = str(e)
        return health_results


@log_function_call
def run_cluster_health_check(admin_client: AdminClient, cluster_id: str = None) -> Dict[str, Any]:
    """
    Run cluster-specific health check.
    
    Args:
        admin_client: AdminClient instance
        cluster_id: Specific cluster ID to check (optional)
        
    Returns:
        Dictionary containing cluster health results
    """
    cluster_results = {
        'timestamp': _get_current_timestamp(),
        'cluster_id': cluster_id,
        'status': 'UNKNOWN',
        'checks': {},
        'recommendations': []
    }
    
    try:
        clusters = admin_client.clusters.list_clusters()
        
        if cluster_id:
            # Check specific cluster
            target_cluster = None
            for cluster in clusters:
                if cluster.get('cluster_id') == cluster_id:
                    target_cluster = cluster
                    break
            
            if not target_cluster:
                cluster_results['status'] = 'ERROR'
                cluster_results['error'] = f'Cluster {cluster_id} not found'
                return cluster_results
            
            # Check cluster state
            state = target_cluster.get('state', 'UNKNOWN')
            cluster_results['checks']['cluster_state'] = {
                'status': 'PASS' if state == 'RUNNING' else 'FAIL',
                'description': f'Cluster state: {state}',
                'details': target_cluster
            }
            
            # Check cluster configuration
            config = target_cluster.get('cluster_config', {})
            cluster_results['checks']['cluster_config'] = {
                'status': 'PASS' if config else 'WARN',
                'description': 'Cluster configuration check',
                'details': config
            }
            
        else:
            # Check all clusters
            running_clusters = [c for c in clusters if c.get('state') == 'RUNNING']
            stopped_clusters = [c for c in clusters if c.get('state') == 'TERMINATED']
            
            cluster_results['checks']['cluster_count'] = {
                'status': 'PASS' if len(clusters) > 0 else 'WARN',
                'description': f'Cluster count check ({len(clusters)} total clusters)',
                'details': {
                    'total_clusters': len(clusters),
                    'running_clusters': len(running_clusters),
                    'stopped_clusters': len(stopped_clusters)
                }
            }
            
            cluster_results['checks']['cluster_availability'] = {
                'status': 'PASS' if len(running_clusters) > 0 else 'WARN',
                'description': f'Cluster availability check ({len(running_clusters)} running)',
                'details': {'running_clusters': len(running_clusters)}
            }
        
        # Determine overall status
        passed_checks = sum(1 for check in cluster_results['checks'].values() if check['status'] == 'PASS')
        total_checks = len(cluster_results['checks'])
        
        if passed_checks == total_checks:
            cluster_results['status'] = 'PASS'
        elif passed_checks > 0:
            cluster_results['status'] = 'WARN'
        else:
            cluster_results['status'] = 'FAIL'
        
        # Generate recommendations
        if cluster_id and cluster_results['checks'].get('cluster_state', {}).get('status') == 'FAIL':
            cluster_results['recommendations'].append(f'Cluster {cluster_id} is not running - consider starting it')
        
        if not cluster_id and len(running_clusters) == 0:
            cluster_results['recommendations'].append('No running clusters - consider starting at least one cluster')
        
        return cluster_results
        
    except Exception as e:
        cluster_results['status'] = 'ERROR'
        cluster_results['error'] = str(e)
        return cluster_results


@log_function_call
def run_user_health_check(admin_client: AdminClient, username: str = None) -> Dict[str, Any]:
    """
    Run user-specific health check.
    
    Args:
        admin_client: AdminClient instance
        username: Specific username to check (optional)
        
    Returns:
        Dictionary containing user health results
    """
    user_results = {
        'timestamp': _get_current_timestamp(),
        'username': username,
        'status': 'UNKNOWN',
        'checks': {},
        'recommendations': []
    }
    
    try:
        users = admin_client.users.list_users()
        
        if username:
            # Check specific user
            target_user = None
            for user in users:
                if user.get('user_name') == username:
                    target_user = user
                    break
            
            if not target_user:
                user_results['status'] = 'ERROR'
                user_results['error'] = f'User {username} not found'
                return user_results
            
            # Check user status
            active = target_user.get('active', True)
            user_results['checks']['user_status'] = {
                'status': 'PASS' if active else 'FAIL',
                'description': f'User status: {"Active" if active else "Inactive"}',
                'details': target_user
            }
            
            # Check user entitlements
            entitlements = admin_client.privileges.get_user_entitlements(username)
            user_results['checks']['user_entitlements'] = {
                'status': 'PASS' if entitlements.get('success') else 'WARN',
                'description': f'User entitlements ({entitlements.get("total_entitlements", 0)} entitlements)',
                'details': entitlements
            }
            
        else:
            # Check all users
            active_users = [u for u in users if u.get('active', True)]
            inactive_users = [u for u in users if not u.get('active', True)]
            
            user_results['checks']['user_count'] = {
                'status': 'PASS' if len(users) > 0 else 'WARN',
                'description': f'User count check ({len(users)} total users)',
                'details': {
                    'total_users': len(users),
                    'active_users': len(active_users),
                    'inactive_users': len(inactive_users)
                }
            }
            
            user_results['checks']['user_activity'] = {
                'status': 'PASS' if len(active_users) > 0 else 'WARN',
                'description': f'User activity check ({len(active_users)} active users)',
                'details': {'active_users': len(active_users)}
            }
        
        # Determine overall status
        passed_checks = sum(1 for check in user_results['checks'].values() if check['status'] == 'PASS')
        total_checks = len(user_results['checks'])
        
        if passed_checks == total_checks:
            user_results['status'] = 'PASS'
        elif passed_checks > 0:
            user_results['status'] = 'WARN'
        else:
            user_results['status'] = 'FAIL'
        
        # Generate recommendations
        if username and user_results['checks'].get('user_status', {}).get('status') == 'FAIL':
            user_results['recommendations'].append(f'User {username} is inactive - consider activating')
        
        if not username and len(active_users) == 0:
            user_results['recommendations'].append('No active users - check user management')
        
        return user_results
        
    except Exception as e:
        user_results['status'] = 'ERROR'
        user_results['error'] = str(e)
        return user_results


def print_health_report(health_results: Dict[str, Any]) -> None:
    """Print a formatted health report."""
    print("🏥 Health Monitoring Report")
    print("=" * 50)
    
    if 'error' in health_results:
        print(f"❌ Health check failed: {health_results['error']}")
        return
    
    print(f"📊 Overall Status: {health_results['overall_status']}")
    if 'score' in health_results:
        print(f"📈 Health Score: {health_results['score']:.1f}/100")
    
    print(f"\n🔍 Health Checks:")
    for check_name, check_result in health_results['checks'].items():
        status_icon = {
            'PASS': '✅',
            'WARN': '⚠️',
            'FAIL': '❌',
            'ERROR': '💥'
        }.get(check_result['status'], '❓')
        
        print(f"   {status_icon} {check_name.replace('_', ' ').title()}")
        print(f"      Description: {check_result['description']}")
    
    if health_results.get('recommendations'):
        print(f"\n💡 Recommendations:")
        for recommendation in health_results['recommendations']:
            print(f"   • {recommendation}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Databricks Health Monitoring Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run system health check
  python cli/monitoring/health_cli.py system-health

  # Run cluster health check
  python cli/monitoring/health_cli.py cluster-health --cluster-id 1234-567890-abcdef

  # Run user health check
  python cli/monitoring/health_cli.py user-health --username john.doe
        """,
    )
    
    parser.add_argument(
        "command",
        choices=[
            "system-health",
            "cluster-health", 
            "user-health"
        ],
        help="Health check command to execute"
    )
    
    parser.add_argument(
        "--cluster-id",
        help="Specific cluster ID to check"
    )
    
    parser.add_argument(
        "--username", 
        help="Specific username to check"
    )
    
    args = parser.parse_args()
    
    # Initialize admin client
    admin_client = AdminClient()
    
    if args.command == "system-health":
        print("🏥 Running System Health Check")
        print("=" * 50)
        health_results = run_system_health_check(admin_client)
        print_health_report(health_results)
        
    elif args.command == "cluster-health":
        print("🖥️  Running Cluster Health Check")
        print("=" * 50)
        health_results = run_cluster_health_check(admin_client, args.cluster_id)
        print_health_report(health_results)
        
    elif args.command == "user-health":
        print("👤 Running User Health Check")
        print("=" * 50)
        health_results = run_user_health_check(admin_client, args.username)
        print_health_report(health_results)
        
    else:
        print(f"❌ Unknown command: {args.command}")
        sys.exit(1)


def _get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    from datetime import datetime
    return datetime.now().isoformat()


if __name__ == "__main__":
    main() 