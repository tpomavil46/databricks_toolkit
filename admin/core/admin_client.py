"""
Administrative Client

This module provides a unified client for all administrative operations
including user management, security, cluster management, and workspace administration.
"""

from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.iam import User, Group
from utils.logger import log_function_call

from .security_manager import SecurityManager
from .user_manager import UserManager
from .cluster_manager import ClusterManager
from .workspace_manager import WorkspaceManager
from .privilege_manager import PrivilegeManager


class AdminClient:
    """
    Unified administrative client for Databricks workspace management.
    """
    
    def __init__(self, profile: str = "databricks"):
        """
        Initialize the administrative client.
        
        Args:
            profile: Databricks profile name from .databrickscfg
        """
        self.client = WorkspaceClient(profile=profile)
        self.security = SecurityManager(self.client)
        self.users = UserManager(self.client)
        self.clusters = ClusterManager(self.client)
        self.workspace = WorkspaceManager(self.client)
        self.privileges = PrivilegeManager(self.client)
    
    @log_function_call
    def get_workspace_info(self) -> Dict[str, Any]:
        """
        Get comprehensive workspace information.
        
        Returns:
            Dictionary containing workspace details
        """
        try:
            # Get current user info
            current_user = self.client.current_user.me()
            
            # Get workspace details
            workspace_info = {
                'workspace_url': self.client.config.host,
                'current_user': {
                    'user_name': getattr(current_user, 'user_name', None),
                    'display_name': getattr(current_user, 'display_name', None),
                    'email': getattr(current_user, 'email', None),
                    'active': getattr(current_user, 'active', True)
                },
                'capabilities': {
                    'is_admin': self._check_admin_capabilities(),
                    'can_manage_users': self._check_user_management_capabilities(),
                    'can_manage_clusters': self._check_cluster_management_capabilities()
                }
            }
            
            return workspace_info
            
        except Exception as e:
            return {
                'error': f"Failed to get workspace info: {str(e)}",
                'status': 'error'
            }
    
    def _check_admin_capabilities(self) -> bool:
        """Check if current user has admin capabilities."""
        try:
            # Try to list all users (admin-only operation)
            self.client.users.list()
            return True
        except Exception:
            return False
    
    def _check_user_management_capabilities(self) -> bool:
        """Check if current user can manage users."""
        try:
            # Try to list users
            self.client.users.list()
            return True
        except Exception:
            return False
    
    def _check_cluster_management_capabilities(self) -> bool:
        """Check if current user can manage clusters."""
        try:
            # Try to list clusters
            self.client.clusters.list()
            return True
        except Exception:
            return False
    
    @log_function_call
    def get_system_health(self) -> Dict[str, Any]:
        """
        Get system health information.
        
        Returns:
            Dictionary containing system health metrics
        """
        try:
            health_info = {
                'workspace': self.workspace.get_workspace_health(),
                'clusters': self.clusters.get_cluster_health(),
                'users': self.users.get_user_health(),
                'security': self.security.get_security_health()
            }
            
            # Calculate overall health score
            scores = []
            for component, data in health_info.items():
                if isinstance(data, dict) and 'health_score' in data:
                    scores.append(data['health_score'])
            
            overall_score = sum(scores) / len(scores) if scores else 0
            
            health_info['overall'] = {
                'health_score': overall_score,
                'status': 'healthy' if overall_score >= 80 else 'warning' if overall_score >= 60 else 'critical'
            }
            
            return health_info
            
        except Exception as e:
            return {
                'error': f"Failed to get system health: {str(e)}",
                'status': 'error'
            }
    
    @log_function_call
    def run_health_check(self) -> Dict[str, Any]:
        """
        Run comprehensive health check on all systems.
        
        Returns:
            Dictionary containing health check results
        """
        print("🏥 Running System Health Check")
        print("=" * 50)
        
        health_info = self.get_system_health()
        
        if 'error' in health_info:
            print(f"❌ Health check failed: {health_info['error']}")
            return health_info
        
        # Print health summary
        overall = health_info.get('overall', {})
        print(f"📊 Overall Health Score: {overall.get('health_score', 0):.1f}/100")
        print(f"🏷️  Status: {overall.get('status', 'unknown')}")
        
        # Print component health
        for component, data in health_info.items():
            if component != 'overall' and isinstance(data, dict):
                score = data.get('health_score', 0)
                status = data.get('status', 'unknown')
                print(f"  {component.title()}: {score:.1f}/100 ({status})")
        
        return health_info
    
    @log_function_call
    def get_usage_summary(self) -> Dict[str, Any]:
        """
        Get workspace usage summary.
        
        Returns:
            Dictionary containing usage statistics
        """
        try:
            # Get cluster usage
            clusters = self.clusters.list_clusters()
            active_clusters = [c for c in clusters if c.state == 'RUNNING']
            
            # Get user statistics
            users = self.users.list_users()
            active_users = [u for u in users if u.active]
            
            # Get workspace statistics
            workspace_stats = self.workspace.get_workspace_stats()
            
            usage_summary = {
                'clusters': {
                    'total': len(clusters),
                    'active': len(active_clusters),
                    'inactive': len(clusters) - len(active_clusters)
                },
                'users': {
                    'total': len(users),
                    'active': len(active_users),
                    'inactive': len(users) - len(active_users)
                },
                'workspace': workspace_stats,
                'timestamp': self._get_current_timestamp()
            }
            
            return usage_summary
            
        except Exception as e:
            return {
                'error': f"Failed to get usage summary: {str(e)}",
                'status': 'error'
            }
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def print_usage_summary(self) -> None:
        """Print a formatted usage summary."""
        print("📊 Workspace Usage Summary")
        print("=" * 50)
        
        usage = self.get_usage_summary()
        
        if 'error' in usage:
            print(f"❌ Failed to get usage summary: {usage['error']}")
            return
        
        # Print cluster usage
        clusters = usage.get('clusters', {})
        print(f"🖥️  Clusters:")
        print(f"   Total: {clusters.get('total', 0)}")
        print(f"   Active: {clusters.get('active', 0)}")
        print(f"   Inactive: {clusters.get('inactive', 0)}")
        
        # Print user usage
        users = usage.get('users', {})
        print(f"👥 Users:")
        print(f"   Total: {users.get('total', 0)}")
        print(f"   Active: {users.get('active', 0)}")
        print(f"   Inactive: {users.get('inactive', 0)}")
        
        # Print workspace stats
        workspace = usage.get('workspace', {})
        if workspace:
            print(f"📁 Workspace:")
            for key, value in workspace.items():
                print(f"   {key.replace('_', ' ').title()}: {value}")
        
        print(f"⏰ Generated: {usage.get('timestamp', 'unknown')}")
    
    def __str__(self) -> str:
        """String representation of the admin client."""
        return f"AdminClient(workspace={self.client.config.host})"
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"AdminClient(profile='{self.client.config.profile}', workspace='{self.client.config.host}')" 