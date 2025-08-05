#!/usr/bin/env python3
"""
Administrative CLI Tool

This CLI provides comprehensive administrative tools for managing
Databricks workspace resources including users, clusters, security, and workspace.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from admin.core.admin_client import AdminClient


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Databricks Administrative Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get workspace information
  python admin/cli/admin_cli.py workspace-info

  # Run health check
  python admin/cli/admin_cli.py health-check

  # Get usage summary
  python admin/cli/admin_cli.py usage-summary

  # Run security audit
  python admin/cli/admin_cli.py security-audit

  # List users
  python admin/cli/admin_cli.py list-users

  # List clusters
  python admin/cli/admin_cli.py list-clusters

  # Create user
  python admin/cli/admin_cli.py create-user --username john.doe --display-name "John Doe" --email john.doe@company.com

  # Create cluster
  python admin/cli/admin_cli.py create-cluster --name "test-cluster" --workers 2
        """
    )
    
    parser.add_argument(
        'command',
        choices=[
            'workspace-info',
            'health-check',
            'usage-summary',
            'security-audit',
            'workspace-audit',
            'list-users',
            'list-clusters',
            'create-user',
            'create-cluster',
            'user-summary',
            'cluster-summary',
            'workspace-summary',
            'security-summary',
            'privilege-summary',
            'privilege-audit',
            'grant-entitlement',
            'revoke-entitlement',
            'add-user-to-group',
            'remove-user-from-group'
        ],
        help='Administrative command to execute'
    )
    
    parser.add_argument(
        '--profile',
        default='databricks',
        help='Databricks profile name (default: databricks)'
    )
    
    # User management arguments
    parser.add_argument('--username', help='Username for user operations')
    parser.add_argument('--display-name', help='Display name for user operations')
    parser.add_argument('--email', help='Email for user operations')
    parser.add_argument('--groups', nargs='+', help='Groups to add user to')
    
    # Cluster management arguments
    parser.add_argument('--name', help='Cluster name')
    parser.add_argument('--workers', type=int, default=1, help='Number of worker nodes')
    parser.add_argument('--node-type', default='i3.xlarge', help='Node type for cluster')
    parser.add_argument('--spark-version', default='13.3.x-scala2.12', help='Spark version')
    parser.add_argument('--autotermination', type=int, default=60, help='Auto-termination minutes')
    
    # Privilege management arguments
    parser.add_argument('--entitlement-type', help='Type of entitlement')
    parser.add_argument('--entitlement-value', help='Value of entitlement')
    parser.add_argument('--group-name', help='Group name for group operations')
    
    args = parser.parse_args()
    
    try:
        # Initialize admin client
        admin_client = AdminClient(profile=args.profile)
        
        # Execute command
        if args.command == 'workspace-info':
            execute_workspace_info(admin_client)
        elif args.command == 'health-check':
            execute_health_check(admin_client)
        elif args.command == 'usage-summary':
            execute_usage_summary(admin_client)
        elif args.command == 'security-audit':
            execute_security_audit(admin_client)
        elif args.command == 'workspace-audit':
            execute_workspace_audit(admin_client)
        elif args.command == 'list-users':
            execute_list_users(admin_client)
        elif args.command == 'list-clusters':
            execute_list_clusters(admin_client)
        elif args.command == 'create-user':
            execute_create_user(admin_client, args)
        elif args.command == 'create-cluster':
            execute_create_cluster(admin_client, args)
        elif args.command == 'user-summary':
            execute_user_summary(admin_client)
        elif args.command == 'cluster-summary':
            execute_cluster_summary(admin_client)
        elif args.command == 'workspace-summary':
            execute_workspace_summary(admin_client)
        elif args.command == 'security-summary':
            execute_security_summary(admin_client)
        elif args.command == 'privilege-summary':
            execute_privilege_summary(admin_client, args)
        elif args.command == 'privilege-audit':
            execute_privilege_audit(admin_client)
        elif args.command == 'grant-entitlement':
            execute_grant_entitlement(admin_client, args)
        elif args.command == 'revoke-entitlement':
            execute_revoke_entitlement(admin_client, args)
        elif args.command == 'add-user-to-group':
            execute_add_user_to_group(admin_client, args)
        elif args.command == 'remove-user-from-group':
            execute_remove_user_from_group(admin_client, args)
        else:
            print(f"❌ Unknown command: {args.command}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


def execute_workspace_info(admin_client: AdminClient):
    """Execute workspace info command."""
    print("🌐 Getting Workspace Information")
    print("=" * 50)
    
    info = admin_client.get_workspace_info()
    
    if 'error' in info:
        print(f"❌ Failed to get workspace info: {info['error']}")
        return
    
    print(f"🌐 Workspace URL: {info.get('workspace_url', 'Unknown')}")
    
    current_user = info.get('current_user', {})
    print(f"\n👤 Current User:")
    print(f"   Username: {current_user.get('user_name', 'Unknown')}")
    print(f"   Display Name: {current_user.get('display_name', 'Unknown')}")
    print(f"   Active: {'✅' if current_user.get('active') else '❌'}")
    
    capabilities = info.get('capabilities', {})
    print(f"\n🔧 Capabilities:")
    print(f"   Admin Access: {'✅' if capabilities.get('is_admin') else '❌'}")
    print(f"   User Management: {'✅' if capabilities.get('can_manage_users') else '❌'}")
    print(f"   Cluster Management: {'✅' if capabilities.get('can_manage_clusters') else '❌'}")


def execute_health_check(admin_client: AdminClient):
    """Execute health check command."""
    admin_client.run_health_check()


def execute_usage_summary(admin_client: AdminClient):
    """Execute usage summary command."""
    admin_client.print_usage_summary()


def execute_security_audit(admin_client: AdminClient):
    """Execute security audit command."""
    admin_client.security.run_security_audit()


def execute_workspace_audit(admin_client: AdminClient):
    """Execute workspace audit command."""
    admin_client.workspace.run_workspace_audit()


def execute_list_users(admin_client: AdminClient):
    """Execute list users command."""
    print("👥 Listing Users")
    print("=" * 50)
    
    users = admin_client.users.list_users()
    
    if not users:
        print("❌ No users found or unable to list users")
        return
    
    print(f"📊 Found {len(users)} users:")
    print()
    
    for user in users:
        status = "✅" if user.active else "❌"
        print(f"{status} {user.user_name} ({user.display_name})")
        # Handle email attribute safely
        email = getattr(user, 'email', None)
        if not email and hasattr(user, 'emails') and user.emails:
            # Try to get email from emails list
            email = user.emails[0].value if user.emails else None
        if email:
            print(f"    📧 {email}")


def execute_list_clusters(admin_client: AdminClient):
    """Execute list clusters command."""
    print("🖥️  Listing Clusters")
    print("=" * 50)
    
    clusters = admin_client.clusters.list_clusters()
    
    if not clusters:
        print("❌ No clusters found or unable to list clusters")
        return
    
    print(f"📊 Found {len(clusters)} clusters:")
    print()
    
    for cluster in clusters:
        status_icon = {
            'RUNNING': '🟢',
            'TERMINATED': '🔴',
            'ERROR': '🔴',
            'STARTING': '🟡',
            'STOPPING': '🟡'
        }.get(cluster.state, '⚪')
        
        print(f"{status_icon} {cluster.cluster_name}")
        print(f"    ID: {cluster.cluster_id}")
        print(f"    State: {cluster.state}")
        print(f"    Workers: {cluster.num_workers}")


def execute_create_user(admin_client: AdminClient, args):
    """Execute create user command."""
    if not args.username or not args.display_name or not args.email:
        print("❌ Error: --username, --display-name, and --email are required")
        return
    
    print(f"👤 Creating User: {args.username}")
    print("=" * 50)
    
    result = admin_client.users.create_user(
        user_name=args.username,
        display_name=args.display_name,
        email=args.email,
        groups=args.groups
    )
    
    if result['success']:
        print(f"✅ User created successfully: {result['message']}")
        user = result['user']
        print(f"   Username: {user.user_name}")
        print(f"   Display Name: {user.display_name}")
        print(f"   Email: {user.email}")
        print(f"   Active: {'✅' if user.active else '❌'}")
    else:
        print(f"❌ Failed to create user: {result['error']}")


def execute_create_cluster(admin_client: AdminClient, args):
    """Execute create cluster command."""
    if not args.name:
        print("❌ Error: --name is required")
        return
    
    print(f"🖥️  Creating Cluster: {args.name}")
    print("=" * 50)
    
    result = admin_client.clusters.create_cluster(
        cluster_name=args.name,
        spark_version=args.spark_version,
        node_type_id=args.node_type,
        num_workers=args.workers,
        autotermination_minutes=args.autotermination
    )
    
    if result['success']:
        print(f"✅ Cluster created successfully: {result['message']}")
        cluster = result['cluster']
        print(f"   Name: {cluster.cluster_name}")
        print(f"   ID: {cluster.cluster_id}")
        print(f"   Workers: {cluster.num_workers}")
        print(f"   Node Type: {cluster.node_type_id}")
        print(f"   Spark Version: {cluster.spark_version}")
    else:
        print(f"❌ Failed to create cluster: {result['error']}")


def execute_user_summary(admin_client: AdminClient):
    """Execute user summary command."""
    admin_client.users.print_user_summary()


def execute_cluster_summary(admin_client: AdminClient):
    """Execute cluster summary command."""
    admin_client.clusters.print_cluster_summary()


def execute_workspace_summary(admin_client: AdminClient):
    """Execute workspace summary command."""
    admin_client.workspace.print_workspace_summary()


def execute_security_summary(admin_client: AdminClient):
    """Execute security summary command."""
    admin_client.security.print_security_summary()


def execute_privilege_summary(admin_client: AdminClient, args):
    """Execute privilege summary command."""
    user_name = args.username if args.username else None
    admin_client.privileges.print_privilege_summary(user_name)


def execute_privilege_audit(admin_client: AdminClient):
    """Execute privilege audit command."""
    admin_client.privileges.run_privilege_audit()


def execute_grant_entitlement(admin_client: AdminClient, args):
    """Execute grant entitlement command."""
    if not args.username or not args.entitlement_type or not args.entitlement_value:
        print("❌ Error: --username, --entitlement-type, and --entitlement-value are required")
        return
    
    print(f"🔐 Granting Entitlement: {args.entitlement_type}:{args.entitlement_value}")
    print("=" * 50)
    
    result = admin_client.privileges.grant_entitlement(
        user_name=args.username,
        entitlement_type=args.entitlement_type,
        entitlement_value=args.entitlement_value
    )
    
    if result['success']:
        print(f"✅ Entitlement granted successfully: {result['message']}")
    else:
        print(f"❌ Failed to grant entitlement: {result['error']}")


def execute_revoke_entitlement(admin_client: AdminClient, args):
    """Execute revoke entitlement command."""
    if not args.username or not args.entitlement_type or not args.entitlement_value:
        print("❌ Error: --username, --entitlement-type, and --entitlement-value are required")
        return
    
    print(f"🔐 Revoking Entitlement: {args.entitlement_type}:{args.entitlement_value}")
    print("=" * 50)
    
    result = admin_client.privileges.revoke_entitlement(
        user_name=args.username,
        entitlement_type=args.entitlement_type,
        entitlement_value=args.entitlement_value
    )
    
    if result['success']:
        print(f"✅ Entitlement revoked successfully: {result['message']}")
    else:
        print(f"❌ Failed to revoke entitlement: {result['error']}")


def execute_add_user_to_group(admin_client: AdminClient, args):
    """Execute add user to group command."""
    if not args.username or not args.group_name:
        print("❌ Error: --username and --group-name are required")
        return
    
    print(f"👥 Adding User to Group: {args.username} -> {args.group_name}")
    print("=" * 50)
    
    result = admin_client.privileges.add_user_to_group(
        user_name=args.username,
        group_name=args.group_name
    )
    
    if result['success']:
        print(f"✅ User added to group successfully: {result['message']}")
    else:
        print(f"❌ Failed to add user to group: {result['error']}")


def execute_remove_user_from_group(admin_client: AdminClient, args):
    """Execute remove user from group command."""
    if not args.username or not args.group_name:
        print("❌ Error: --username and --group-name are required")
        return
    
    print(f"👥 Removing User from Group: {args.username} <- {args.group_name}")
    print("=" * 50)
    
    result = admin_client.privileges.remove_user_from_group(
        user_name=args.username,
        group_name=args.group_name
    )
    
    if result['success']:
        print(f"✅ User removed from group successfully: {result['message']}")
    else:
        print(f"❌ Failed to remove user from group: {result['error']}")


if __name__ == "__main__":
    main() 