"""
Privilege Management Module

This module provides comprehensive privilege management capabilities including
user permissions, entitlements, access control, and privilege auditing.
"""

from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User, Group
from utils.logger import log_function_call


class PrivilegeManager:
    """
    Privilege management operations for Databricks workspace.
    """

    def __init__(self, client: WorkspaceClient):
        """
        Initialize the privilege manager.

        Args:
            client: Databricks workspace client
        """
        self.client = client

    @log_function_call
    def list_entitlements(self) -> List[Any]:
        """
        List all available entitlements in the workspace.

        Returns:
            List of entitlement objects
        """
        try:
            entitlements = list(self.client.entitlements.list())
            return entitlements
        except Exception as e:
            print(f"❌ Failed to list entitlements: {str(e)}")
            return []

    @log_function_call
    def get_user_entitlements(self, user_name: str) -> Dict[str, Any]:
        """
        Get entitlements for a specific user.

        Args:
            user_name: Username to get entitlements for

        Returns:
            Dictionary containing user entitlements
        """
        try:
            # Get user
            users = list(self.client.users.list())
            user = None
            for u in users:
                if u.user_name == user_name:
                    user = u
                    break

            if not user:
                return {"success": False, "error": f"User {user_name} not found"}

            # Get user entitlements
            entitlements = []
            if hasattr(user, "entitlements") and user.entitlements:
                for entitlement in user.entitlements:
                    entitlements.append(
                        {
                            "type": getattr(entitlement, "type", None),
                            "value": getattr(entitlement, "value", None),
                            "display": getattr(entitlement, "display", None),
                            "primary": getattr(entitlement, "primary", None),
                        }
                    )

            return {
                "success": True,
                "user_name": user_name,
                "entitlements": entitlements,
                "total_entitlements": len(entitlements),
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to get entitlements for {user_name}: {str(e)}",
            }

    @log_function_call
    def grant_entitlement(
        self, user_name: str, entitlement_type: str, entitlement_value: str
    ) -> Dict[str, Any]:
        """
        Grant an entitlement to a user.

        Args:
            user_name: Username to grant entitlement to
            entitlement_type: Type of entitlement
            entitlement_value: Value of the entitlement

        Returns:
            Dictionary containing operation result
        """
        try:
            # Grant entitlement
            self.client.users.update(
                user_name=user_name,
                entitlements=[{"type": entitlement_type, "value": entitlement_value}],
            )

            return {
                "success": True,
                "message": f"Granted {entitlement_type}:{entitlement_value} to {user_name}",
            }

        except Exception as e:
            return {"success": False, "error": f"Failed to grant entitlement: {str(e)}"}

    @log_function_call
    def revoke_entitlement(
        self, user_name: str, entitlement_type: str, entitlement_value: str
    ) -> Dict[str, Any]:
        """
        Revoke an entitlement from a user.

        Args:
            user_name: Username to revoke entitlement from
            entitlement_type: Type of entitlement
            entitlement_value: Value of the entitlement

        Returns:
            Dictionary containing operation result
        """
        try:
            # Get current user entitlements
            current_entitlements = self.get_user_entitlements(user_name)

            if not current_entitlements["success"]:
                return current_entitlements

            # Remove the specific entitlement
            updated_entitlements = []
            for entitlement in current_entitlements["entitlements"]:
                if not (
                    entitlement["type"] == entitlement_type
                    and entitlement["value"] == entitlement_value
                ):
                    updated_entitlements.append(entitlement)

            # Update user with remaining entitlements
            self.client.users.update(
                user_name=user_name, entitlements=updated_entitlements
            )

            return {
                "success": True,
                "message": f"Revoked {entitlement_type}:{entitlement_value} from {user_name}",
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to revoke entitlement: {str(e)}",
            }

    @log_function_call
    def get_user_groups(self, user_name: str) -> Dict[str, Any]:
        """
        Get groups for a specific user.

        Args:
            user_name: Username to get groups for

        Returns:
            Dictionary containing user groups
        """
        try:
            # Get user
            users = list(self.client.users.list())
            user = None
            for u in users:
                if u.user_name == user_name:
                    user = u
                    break

            if not user:
                return {"success": False, "error": f"User {user_name} not found"}

            # Get user groups
            groups = []
            if hasattr(user, "groups") and user.groups:
                for group in user.groups:
                    groups.append(
                        {
                            "display": getattr(group, "display", None),
                            "value": getattr(group, "value", None),
                            "type": getattr(group, "type", None),
                            "ref": getattr(group, "ref", None),
                        }
                    )

            return {
                "success": True,
                "user_name": user_name,
                "groups": groups,
                "total_groups": len(groups),
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to get groups for {user_name}: {str(e)}",
            }

    @log_function_call
    def add_user_to_group(self, user_name: str, group_name: str) -> Dict[str, Any]:
        """
        Add a user to a group.

        Args:
            user_name: Username to add to group
            group_name: Group name to add user to

        Returns:
            Dictionary containing operation result
        """
        try:
            # Add user to group
            self.client.groups.create_member(
                parent_name=group_name, user_name=user_name
            )

            return {
                "success": True,
                "message": f"Added {user_name} to group {group_name}",
            }

        except Exception as e:
            return {"success": False, "error": f"Failed to add user to group: {str(e)}"}

    @log_function_call
    def remove_user_from_group(self, user_name: str, group_name: str) -> Dict[str, Any]:
        """
        Remove a user from a group.

        Args:
            user_name: Username to remove from group
            group_name: Group name to remove user from

        Returns:
            Dictionary containing operation result
        """
        try:
            # Remove user from group
            self.client.groups.delete_member(
                parent_name=group_name, user_name=user_name
            )

            return {
                "success": True,
                "message": f"Removed {user_name} from group {group_name}",
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to remove user from group: {str(e)}",
            }

    @log_function_call
    def get_privilege_summary(self, user_name: str = None) -> Dict[str, Any]:
        """
        Get comprehensive privilege summary for a user or all users.

        Args:
            user_name: Optional username to get summary for (if None, get for all users)

        Returns:
            Dictionary containing privilege summary
        """
        try:
            if user_name:
                # Get summary for specific user
                entitlements = self.get_user_entitlements(user_name)
                groups = self.get_user_groups(user_name)

                return {
                    "user_name": user_name,
                    "entitlements": entitlements,
                    "groups": groups,
                    "summary": {
                        "total_entitlements": (
                            entitlements.get("total_entitlements", 0)
                            if entitlements["success"]
                            else 0
                        ),
                        "total_groups": (
                            groups.get("total_groups", 0) if groups["success"] else 0
                        ),
                        "has_admin_access": self._check_admin_access(entitlements),
                        "has_cluster_access": self._check_cluster_access(entitlements),
                        "has_workspace_access": self._check_workspace_access(
                            entitlements
                        ),
                    },
                }
            else:
                # Get summary for all users
                users = list(self.client.users.list())
                user_summaries = []

                for user in users:
                    user_name = user.user_name
                    entitlements = self.get_user_entitlements(user_name)
                    groups = self.get_user_groups(user_name)

                    user_summaries.append(
                        {
                            "user_name": user_name,
                            "display_name": getattr(user, "display_name", None),
                            "active": getattr(user, "active", True),
                            "entitlements": (
                                entitlements.get("total_entitlements", 0)
                                if entitlements["success"]
                                else 0
                            ),
                            "groups": (
                                groups.get("total_groups", 0)
                                if groups["success"]
                                else 0
                            ),
                            "has_admin_access": self._check_admin_access(entitlements),
                            "has_cluster_access": self._check_cluster_access(
                                entitlements
                            ),
                            "has_workspace_access": self._check_workspace_access(
                                entitlements
                            ),
                        }
                    )

                return {
                    "total_users": len(users),
                    "user_summaries": user_summaries,
                    "summary": {
                        "users_with_admin": len(
                            [u for u in user_summaries if u["has_admin_access"]]
                        ),
                        "users_with_cluster_access": len(
                            [u for u in user_summaries if u["has_cluster_access"]]
                        ),
                        "users_with_workspace_access": len(
                            [u for u in user_summaries if u["has_workspace_access"]]
                        ),
                        "average_entitlements": (
                            sum(u["entitlements"] for u in user_summaries)
                            / len(user_summaries)
                            if user_summaries
                            else 0
                        ),
                        "average_groups": (
                            sum(u["groups"] for u in user_summaries)
                            / len(user_summaries)
                            if user_summaries
                            else 0
                        ),
                    },
                }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to get privilege summary: {str(e)}",
            }

    def _check_admin_access(self, entitlements: Dict[str, Any]) -> bool:
        """Check if user has admin access based on entitlements."""
        if not entitlements["success"]:
            return False

        admin_entitlements = [
            "allow-cluster-create",
            "allow-instance-pool-create",
            "workspace-access",
        ]
        user_entitlements = [e["value"] for e in entitlements["entitlements"]]

        return any(admin in user_entitlements for admin in admin_entitlements)

    def _check_cluster_access(self, entitlements: Dict[str, Any]) -> bool:
        """Check if user has cluster access based on entitlements."""
        if not entitlements["success"]:
            return False

        cluster_entitlements = ["allow-cluster-create", "allow-instance-pool-create"]
        user_entitlements = [e["value"] for e in entitlements["entitlements"]]

        return any(cluster in user_entitlements for cluster in cluster_entitlements)

    def _check_workspace_access(self, entitlements: Dict[str, Any]) -> bool:
        """Check if user has workspace access based on entitlements."""
        if not entitlements["success"]:
            return False

        workspace_entitlements = ["workspace-access"]
        user_entitlements = [e["value"] for e in entitlements["entitlements"]]

        return any(
            workspace in user_entitlements for workspace in workspace_entitlements
        )

    def print_privilege_summary(self, user_name: str = None) -> None:
        """Print a formatted privilege summary."""
        print("🔐 Privilege Management Summary")
        print("=" * 50)

        summary = self.get_privilege_summary(user_name)

        if "success" in summary and not summary["success"]:
            print(f"❌ Failed to get privilege summary: {summary['error']}")
            return

        if user_name:
            # Single user summary
            print(f"👤 User: {user_name}")
            print(f"📊 Entitlements: {summary['summary']['total_entitlements']}")
            print(f"👥 Groups: {summary['summary']['total_groups']}")
            print(
                f"🔧 Admin Access: {'✅' if summary['summary']['has_admin_access'] else '❌'}"
            )
            print(
                f"🖥️  Cluster Access: {'✅' if summary['summary']['has_cluster_access'] else '❌'}"
            )
            print(
                f"📁 Workspace Access: {'✅' if summary['summary']['has_workspace_access'] else '❌'}"
            )
        else:
            # All users summary
            print(f"📊 Total Users: {summary['total_users']}")
            print(
                f"🔧 Users with Admin Access: {summary['summary']['users_with_admin']}"
            )
            print(
                f"🖥️  Users with Cluster Access: {summary['summary']['users_with_cluster_access']}"
            )
            print(
                f"📁 Users with Workspace Access: {summary['summary']['users_with_workspace_access']}"
            )
            print(
                f"📈 Average Entitlements: {summary['summary']['average_entitlements']:.1f}"
            )
            print(f"📈 Average Groups: {summary['summary']['average_groups']:.1f}")

            print(f"\n👥 User Details:")
            for user in summary["user_summaries"]:
                status = "✅" if user["active"] else "❌"
                admin_icon = "🔧" if user["has_admin_access"] else ""
                cluster_icon = "🖥️" if user["has_cluster_access"] else ""
                workspace_icon = "📁" if user["has_workspace_access"] else ""

                print(f"   {status} {user['user_name']} ({user['display_name']})")
                print(
                    f"      📊 Entitlements: {user['entitlements']}, Groups: {user['groups']}"
                )
                print(f"      {admin_icon}{cluster_icon}{workspace_icon}")

    @log_function_call
    def run_privilege_audit(self) -> Dict[str, Any]:
        """
        Run a comprehensive privilege audit.

        Returns:
            Dictionary containing audit results
        """
        print("🔍 Running Privilege Audit")
        print("=" * 50)

        try:
            audit_results = {"timestamp": self._get_current_timestamp(), "checks": {}}

            # Check 1: Admin privilege distribution
            summary = self.get_privilege_summary()
            if "success" not in summary or summary["success"]:
                admin_users = summary["summary"]["users_with_admin"]
                total_users = summary["total_users"]
                admin_ratio = admin_users / total_users if total_users > 0 else 0

                audit_results["checks"]["admin_distribution"] = {
                    "status": "PASS" if admin_ratio <= 0.2 else "WARN",
                    "description": f"Admin privilege distribution ({admin_ratio:.1%} of users have admin)",
                    "recommendation": (
                        "Consider reducing admin privileges"
                        if admin_ratio > 0.2
                        else "Admin distribution looks good"
                    ),
                }
            else:
                audit_results["checks"]["admin_distribution"] = {
                    "status": "FAIL",
                    "description": "Unable to assess admin distribution",
                    "recommendation": "Check user access permissions",
                }

            # Check 2: Entitlement coverage
            if "success" not in summary or summary["success"]:
                avg_entitlements = summary["summary"]["average_entitlements"]

                audit_results["checks"]["entitlement_coverage"] = {
                    "status": "PASS" if avg_entitlements >= 2 else "WARN",
                    "description": f"Average entitlements per user ({avg_entitlements:.1f})",
                    "recommendation": (
                        "Consider granting more entitlements"
                        if avg_entitlements < 2
                        else "Entitlement coverage looks good"
                    ),
                }
            else:
                audit_results["checks"]["entitlement_coverage"] = {
                    "status": "FAIL",
                    "description": "Unable to assess entitlement coverage",
                    "recommendation": "Check entitlement permissions",
                }

            # Check 3: Group membership
            if "success" not in summary or summary["success"]:
                avg_groups = summary["summary"]["average_groups"]

                audit_results["checks"]["group_membership"] = {
                    "status": "PASS" if avg_groups >= 1 else "WARN",
                    "description": f"Average groups per user ({avg_groups:.1f})",
                    "recommendation": (
                        "Consider adding users to groups"
                        if avg_groups < 1
                        else "Group membership looks good"
                    ),
                }
            else:
                audit_results["checks"]["group_membership"] = {
                    "status": "FAIL",
                    "description": "Unable to assess group membership",
                    "recommendation": "Check group permissions",
                }

            # Check 4: Privilege consistency
            if "success" not in summary or summary["success"]:
                users_with_cluster = summary["summary"]["users_with_cluster_access"]
                users_with_workspace = summary["summary"]["users_with_workspace_access"]

                consistency_good = users_with_cluster > 0 and users_with_workspace > 0

                audit_results["checks"]["privilege_consistency"] = {
                    "status": "PASS" if consistency_good else "WARN",
                    "description": f"Privilege consistency (cluster: {users_with_cluster}, workspace: {users_with_workspace})",
                    "recommendation": (
                        "Ensure consistent privilege distribution"
                        if not consistency_good
                        else "Privilege consistency looks good"
                    ),
                }
            else:
                audit_results["checks"]["privilege_consistency"] = {
                    "status": "FAIL",
                    "description": "Unable to assess privilege consistency",
                    "recommendation": "Check privilege permissions",
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
                "error": f"Privilege audit failed: {str(e)}",
                "status": "error",
            }
            print(f"❌ {error_result['error']}")
            return error_result

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime

        return datetime.now().isoformat()
