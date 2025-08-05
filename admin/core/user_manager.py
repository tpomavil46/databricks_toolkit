"""
User Management Module

This module provides comprehensive user management capabilities including
user creation, modification, deletion, and health monitoring.
"""

from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User, Group
from utils.logger import log_function_call


class UserManager:
    """
    User management operations for Databricks workspace.
    """
    
    def __init__(self, client: WorkspaceClient):
        """
        Initialize the user manager.
        
        Args:
            client: Databricks workspace client
        """
        self.client = client
    
    @log_function_call
    def list_users(self) -> List[User]:
        """
        List all users in the workspace.
        
        Returns:
            List of user objects
        """
        try:
            users = list(self.client.users.list())
            return users
        except Exception as e:
            print(f"❌ Failed to list users: {str(e)}")
            return []
    
    @log_function_call
    def get_user(self, user_name: str) -> Optional[User]:
        """
        Get a specific user by username.
        
        Args:
            user_name: Username to retrieve
            
        Returns:
            User object or None if not found
        """
        try:
            users = self.list_users()
            for user in users:
                if user.user_name == user_name:
                    return user
            return None
        except Exception as e:
            print(f"❌ Failed to get user {user_name}: {str(e)}")
            return None
    
    @log_function_call
    def create_user(self, user_name: str, display_name: str, email: str, 
                   groups: List[str] = None) -> Dict[str, Any]:
        """
        Create a new user.
        
        Args:
            user_name: Username for the new user
            display_name: Display name for the user
            email: Email address for the user
            groups: List of group names to add user to
            
        Returns:
            Dictionary containing creation result
        """
        try:
            # Check if user already exists
            existing_user = self.get_user(user_name)
            if existing_user:
                return {
                    'success': False,
                    'error': f"User {user_name} already exists",
                    'user': existing_user
                }
            
            # Create user
            user = self.client.users.create(
                user_name=user_name,
                display_name=display_name,
                email=email
            )
            
            # Add to groups if specified
            if groups:
                for group_name in groups:
                    try:
                        self.add_user_to_group(user_name, group_name)
                    except Exception as e:
                        print(f"⚠️  Warning: Failed to add user to group {group_name}: {str(e)}")
            
            return {
                'success': True,
                'user': user,
                'message': f"User {user_name} created successfully"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to create user {user_name}: {str(e)}"
            }
    
    @log_function_call
    def update_user(self, user_name: str, **kwargs) -> Dict[str, Any]:
        """
        Update user information.
        
        Args:
            user_name: Username to update
            **kwargs: Fields to update (display_name, email, active, etc.)
            
        Returns:
            Dictionary containing update result
        """
        try:
            user = self.get_user(user_name)
            if not user:
                return {
                    'success': False,
                    'error': f"User {user_name} not found"
                }
            
            # Update user
            updated_user = self.client.users.update(
                id=user.id,
                **kwargs
            )
            
            return {
                'success': True,
                'user': updated_user,
                'message': f"User {user_name} updated successfully"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to update user {user_name}: {str(e)}"
            }
    
    @log_function_call
    def delete_user(self, user_name: str) -> Dict[str, Any]:
        """
        Delete a user.
        
        Args:
            user_name: Username to delete
            
        Returns:
            Dictionary containing deletion result
        """
        try:
            user = self.get_user(user_name)
            if not user:
                return {
                    'success': False,
                    'error': f"User {user_name} not found"
                }
            
            # Delete user
            self.client.users.delete(id=user.id)
            
            return {
                'success': True,
                'message': f"User {user_name} deleted successfully"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to delete user {user_name}: {str(e)}"
            }
    
    @log_function_call
    def list_groups(self) -> List[Group]:
        """
        List all groups in the workspace.
        
        Returns:
            List of group objects
        """
        try:
            groups = list(self.client.groups.list())
            return groups
        except Exception as e:
            print(f"❌ Failed to list groups: {str(e)}")
            return []
    
    @log_function_call
    def add_user_to_group(self, user_name: str, group_name: str) -> Dict[str, Any]:
        """
        Add a user to a group.
        
        Args:
            user_name: Username to add
            group_name: Group name to add user to
            
        Returns:
            Dictionary containing operation result
        """
        try:
            # Get user
            user = self.get_user(user_name)
            if not user:
                return {
                    'success': False,
                    'error': f"User {user_name} not found"
                }
            
            # Get group
            groups = self.list_groups()
            group = None
            for g in groups:
                if g.display_name == group_name:
                    group = g
                    break
            
            if not group:
                return {
                    'success': False,
                    'error': f"Group {group_name} not found"
                }
            
            # Add user to group
            self.client.groups.create_member(
                parent_name=group_name,
                user_name=user_name
            )
            
            return {
                'success': True,
                'message': f"User {user_name} added to group {group_name}"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to add user {user_name} to group {group_name}: {str(e)}"
            }
    
    @log_function_call
    def remove_user_from_group(self, user_name: str, group_name: str) -> Dict[str, Any]:
        """
        Remove a user from a group.
        
        Args:
            user_name: Username to remove
            group_name: Group name to remove user from
            
        Returns:
            Dictionary containing operation result
        """
        try:
            # Remove user from group
            self.client.groups.delete_member(
                parent_name=group_name,
                user_name=user_name
            )
            
            return {
                'success': True,
                'message': f"User {user_name} removed from group {group_name}"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to remove user {user_name} from group {group_name}: {str(e)}"
            }
    
    @log_function_call
    def get_user_health(self) -> Dict[str, Any]:
        """
        Get user system health metrics.
        
        Returns:
            Dictionary containing user health information
        """
        try:
            users = self.list_users()
            
            if not users:
                return {
                    'health_score': 0,
                    'status': 'critical',
                    'error': 'No users found or unable to list users'
                }
            
            # Calculate health metrics
            total_users = len(users)
            active_users = len([u for u in users if u.active])
            inactive_users = total_users - active_users
            
            # Users with email addresses
            users_with_email = len([u for u in users if hasattr(u, 'email') and u.email])
            
            # Calculate health score
            health_score = 0
            issues = []
            
            # Active users ratio (should be > 80%)
            active_ratio = active_users / total_users if total_users > 0 else 0
            if active_ratio < 0.8:
                issues.append(f"Low active user ratio: {active_ratio:.1%}")
                health_score += 20
            else:
                health_score += 40
            
            # Email coverage (should be > 90%)
            email_ratio = users_with_email / total_users if total_users > 0 else 0
            if email_ratio < 0.9:
                issues.append(f"Low email coverage: {email_ratio:.1%}")
                health_score += 20
            else:
                health_score += 40
            
            # User count (should have reasonable number)
            if total_users < 1:
                issues.append("No users found")
                health_score += 0
            elif total_users > 1000:
                issues.append("Very large user base")
                health_score += 20
            else:
                health_score += 20
            
            # Determine status
            if health_score >= 80:
                status = 'healthy'
            elif health_score >= 60:
                status = 'warning'
            else:
                status = 'critical'
            
            return {
                'health_score': health_score,
                'status': status,
                'metrics': {
                    'total_users': total_users,
                    'active_users': active_users,
                    'inactive_users': inactive_users,
                    'users_with_email': users_with_email,
                    'active_ratio': active_ratio,
                    'email_ratio': email_ratio
                },
                'issues': issues
            }
            
        except Exception as e:
            return {
                'health_score': 0,
                'status': 'critical',
                'error': f"Failed to get user health: {str(e)}"
            }
    
    def print_user_summary(self) -> None:
        """Print a formatted user summary."""
        print("👥 User Management Summary")
        print("=" * 50)
        
        users = self.list_users()
        if not users:
            print("❌ No users found or unable to list users")
            return
        
        # Print user statistics
        total_users = len(users)
        active_users = len([u for u in users if u.active])
        inactive_users = total_users - active_users
        
        print(f"📊 User Statistics:")
        print(f"   Total Users: {total_users}")
        print(f"   Active Users: {active_users}")
        print(f"   Inactive Users: {inactive_users}")
        print(f"   Active Ratio: {active_users/total_users:.1%}")
        
        # Print recent users (last 5)
        print(f"\n👤 Recent Users:")
        for user in users[:5]:
            status = "✅" if user.active else "❌"
            print(f"   {status} {user.user_name} ({user.display_name})")
        
        if len(users) > 5:
            print(f"   ... and {len(users) - 5} more users")
        
        # Print health status
        health = self.get_user_health()
        if 'health_score' in health:
            print(f"\n🏥 User Health: {health['health_score']:.1f}/100 ({health['status']})") 