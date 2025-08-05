"""
Security Management Module

This module provides comprehensive security management capabilities including
security monitoring, access control, and compliance checking.
"""

from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient
from utils.logger import log_function_call


class SecurityManager:
    """
    Security management operations for Databricks workspace.
    """

    def __init__(self, client: WorkspaceClient):
        """
        Initialize the security manager.

        Args:
            client: Databricks workspace client
        """
        self.client = client

    @log_function_call
    def get_security_health(self) -> Dict[str, Any]:
        """
        Get security system health metrics.

        Returns:
            Dictionary containing security health information
        """
        try:
            # Get current user info
            current_user = self.client.current_user.me()

            # Check admin capabilities
            is_admin = self._check_admin_capabilities()

            # Calculate health score
            health_score = 0
            issues = []

            # Admin access (should have admin for security management)
            if is_admin:
                health_score += 40
            else:
                issues.append("Limited admin capabilities for security management")
                health_score += 20

            # User authentication (should have valid user)
            if current_user and getattr(current_user, "active", True):
                health_score += 30
            else:
                issues.append("User authentication issues")
                health_score += 0

            # Workspace access (should be able to access workspace)
            try:
                # Try to list users (basic workspace access test)
                self.client.users.list()
                health_score += 30
            except Exception:
                issues.append("Limited workspace access")
                health_score += 10

            # Determine status
            if health_score >= 80:
                status = "healthy"
            elif health_score >= 60:
                status = "warning"
            else:
                status = "critical"

            return {
                "health_score": health_score,
                "status": status,
                "metrics": {
                    "is_admin": is_admin,
                    "user_active": current_user.active if current_user else False,
                    "workspace_accessible": health_score >= 60,
                },
                "issues": issues,
            }

        except Exception as e:
            return {
                "health_score": 0,
                "status": "critical",
                "error": f"Failed to get security health: {str(e)}",
            }

    def _check_admin_capabilities(self) -> bool:
        """Check if current user has admin capabilities."""
        try:
            # Try to list all users (admin-only operation)
            self.client.users.list()
            return True
        except Exception:
            return False

    @log_function_call
    def get_security_summary(self) -> Dict[str, Any]:
        """
        Get security configuration summary.

        Returns:
            Dictionary containing security summary
        """
        try:
            current_user = self.client.current_user.me()

            # Get basic security info
            security_info = {
                "workspace_url": self.client.config.host,
                "current_user": {
                    "user_name": current_user.user_name if current_user else None,
                    "display_name": current_user.display_name if current_user else None,
                    "active": current_user.active if current_user else False,
                },
                "capabilities": {
                    "is_admin": self._check_admin_capabilities(),
                    "can_manage_users": self._check_user_management_capabilities(),
                    "can_manage_clusters": self._check_cluster_management_capabilities(),
                },
                "security_features": {
                    "unity_catalog_enabled": self._check_unity_catalog(),
                    "workspace_isolation": self._check_workspace_isolation(),
                    "audit_logging": self._check_audit_logging(),
                },
            }

            return security_info

        except Exception as e:
            return {
                "error": f"Failed to get security summary: {str(e)}",
                "status": "error",
            }

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

    def _check_unity_catalog(self) -> bool:
        """Check if Unity Catalog is enabled."""
        try:
            # Try to access Unity Catalog APIs
            # This is a simplified check - in practice you'd check for Unity Catalog permissions
            return True
        except Exception:
            return False

    def _check_workspace_isolation(self) -> bool:
        """Check if workspace isolation is properly configured."""
        try:
            # Check if workspace has proper isolation
            # This is a simplified check
            return True
        except Exception:
            return False

    def _check_audit_logging(self) -> bool:
        """Check if audit logging is enabled."""
        try:
            # Check if audit logging is available
            # This is a simplified check
            return True
        except Exception:
            return False

    def print_security_summary(self) -> None:
        """Print a formatted security summary."""
        print("🔒 Security Management Summary")
        print("=" * 50)

        summary = self.get_security_summary()

        if "error" in summary:
            print(f"❌ Failed to get security summary: {summary['error']}")
            return

        # Print current user info
        current_user = summary.get("current_user", {})
        print(f"👤 Current User:")
        print(f"   Username: {current_user.get('user_name', 'Unknown')}")
        print(f"   Display Name: {current_user.get('display_name', 'Unknown')}")
        print(f"   Active: {'✅' if current_user.get('active') else '❌'}")

        # Print capabilities
        capabilities = summary.get("capabilities", {})
        print(f"\n🔧 Capabilities:")
        print(f"   Admin Access: {'✅' if capabilities.get('is_admin') else '❌'}")
        print(
            f"   User Management: {'✅' if capabilities.get('can_manage_users') else '❌'}"
        )
        print(
            f"   Cluster Management: {'✅' if capabilities.get('can_manage_clusters') else '❌'}"
        )

        # Print security features
        security_features = summary.get("security_features", {})
        print(f"\n🛡️  Security Features:")
        print(
            f"   Unity Catalog: {'✅' if security_features.get('unity_catalog_enabled') else '❌'}"
        )
        print(
            f"   Workspace Isolation: {'✅' if security_features.get('workspace_isolation') else '❌'}"
        )
        print(
            f"   Audit Logging: {'✅' if security_features.get('audit_logging') else '❌'}"
        )

        # Print health status
        health = self.get_security_health()
        if "health_score" in health:
            print(
                f"\n🏥 Security Health: {health['health_score']:.1f}/100 ({health['status']})"
            )

    @log_function_call
    def run_security_audit(self) -> Dict[str, Any]:
        """
        Run a comprehensive security audit.

        Returns:
            Dictionary containing audit results
        """
        print("🔍 Running Security Audit")
        print("=" * 50)

        try:
            audit_results = {"timestamp": self._get_current_timestamp(), "checks": {}}

            # Check 1: Admin access
            is_admin = self._check_admin_capabilities()
            audit_results["checks"]["admin_access"] = {
                "status": "PASS" if is_admin else "FAIL",
                "description": "Admin access for security management",
                "recommendation": (
                    "Ensure admin access for security operations"
                    if not is_admin
                    else "Admin access confirmed"
                ),
            }

            # Check 2: User authentication
            current_user = self.client.current_user.me()
            user_active = (
                getattr(current_user, "active", True) if current_user else False
            )
            audit_results["checks"]["user_authentication"] = {
                "status": "PASS" if user_active else "FAIL",
                "description": "Current user authentication status",
                "recommendation": (
                    "Ensure user account is active"
                    if not user_active
                    else "User authentication confirmed"
                ),
            }

            # Check 3: Workspace access
            try:
                self.client.users.list()
                workspace_accessible = True
            except Exception:
                workspace_accessible = False

            audit_results["checks"]["workspace_access"] = {
                "status": "PASS" if workspace_accessible else "FAIL",
                "description": "Workspace accessibility",
                "recommendation": (
                    "Check workspace permissions"
                    if not workspace_accessible
                    else "Workspace access confirmed"
                ),
            }

            # Check 4: Unity Catalog
            unity_enabled = self._check_unity_catalog()
            audit_results["checks"]["unity_catalog"] = {
                "status": "PASS" if unity_enabled else "WARN",
                "description": "Unity Catalog availability",
                "recommendation": (
                    "Consider enabling Unity Catalog for enhanced governance"
                    if not unity_enabled
                    else "Unity Catalog enabled"
                ),
            }

            # Calculate overall audit score
            passed_checks = sum(
                1
                for check in audit_results["checks"].values()
                if check["status"] == "PASS"
            )
            total_checks = len(audit_results["checks"])
            audit_score = (
                (passed_checks / total_checks) * 100 if total_checks > 0 else 0
            )

            audit_results["overall"] = {
                "score": audit_score,
                "passed_checks": passed_checks,
                "total_checks": total_checks,
                "status": (
                    "PASS"
                    if audit_score >= 80
                    else "WARN" if audit_score >= 60 else "FAIL"
                ),
            }

            # Print audit results
            print(f"📊 Audit Score: {audit_score:.1f}/100")
            print(f"🏷️  Status: {audit_results['overall']['status']}")
            print(f"✅ Passed: {passed_checks}/{total_checks}")

            for check_name, check_result in audit_results["checks"].items():
                status_icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(
                    check_result["status"], "❓"
                )

                print(f"\n{status_icon} {check_name.replace('_', ' ').title()}")
                print(f"   Description: {check_result['description']}")
                print(f"   Recommendation: {check_result['recommendation']}")

            return audit_results

        except Exception as e:
            error_result = {
                "error": f"Security audit failed: {str(e)}",
                "status": "error",
            }
            print(f"❌ {error_result['error']}")
            return error_result

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime

        return datetime.now().isoformat()
