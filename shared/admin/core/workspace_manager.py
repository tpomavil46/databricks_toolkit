"""
Workspace Management Module

This module provides comprehensive workspace management capabilities including
workspace monitoring, health checks, and administrative operations.
"""

from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient
from utils.logger import log_function_call


class WorkspaceManager:
    """
    Workspace management operations for Databricks workspace.
    """

    def __init__(self, client: WorkspaceClient):
        """
        Initialize the workspace manager.

        Args:
            client: Databricks workspace client
        """
        self.client = client

    @log_function_call
    def get_workspace_health(self) -> Dict[str, Any]:
        """
        Get workspace system health metrics.

        Returns:
            Dictionary containing workspace health information
        """
        try:
            # Test basic workspace access
            workspace_accessible = self._test_workspace_access()

            # Get workspace statistics
            workspace_stats = self.get_workspace_stats()

            # Calculate health score
            health_score = 0
            issues = []

            # Workspace accessibility (should be 100%)
            if workspace_accessible:
                health_score += 50
            else:
                issues.append("Workspace not accessible")
                health_score += 0

            # Workspace functionality (should have basic functionality)
            if workspace_stats.get("total_objects", 0) >= 0:
                health_score += 30
            else:
                issues.append("Workspace functionality issues")
                health_score += 10

            # User access (should be able to get current user)
            try:
                current_user = self.client.current_user.me()
                if current_user and getattr(current_user, "active", True):
                    health_score += 20
                else:
                    issues.append("User access issues")
                    health_score += 10
            except Exception:
                issues.append("User access problems")
                health_score += 0

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
                    "workspace_accessible": workspace_accessible,
                    "total_objects": workspace_stats.get("total_objects", 0),
                    "user_active": True if workspace_accessible else False,
                },
                "issues": issues,
            }

        except Exception as e:
            return {
                "health_score": 0,
                "status": "critical",
                "error": f"Failed to get workspace health: {str(e)}",
            }

    def _test_workspace_access(self) -> bool:
        """Test if workspace is accessible."""
        try:
            # Try to list workspace objects (basic access test)
            self.client.workspace.list()
            return True
        except Exception:
            return False

    @log_function_call
    def get_workspace_stats(self) -> Dict[str, Any]:
        """
        Get workspace statistics.

        Returns:
            Dictionary containing workspace statistics
        """
        try:
            # Get workspace objects
            workspace_objects = list(self.client.workspace.list(path="/"))

            # Count different types of objects
            notebooks = len(
                [
                    obj
                    for obj in workspace_objects
                    if getattr(obj, "object_type", None) == "NOTEBOOK"
                ]
            )
            directories = len(
                [
                    obj
                    for obj in workspace_objects
                    if getattr(obj, "object_type", None) == "DIRECTORY"
                ]
            )
            libraries = len(
                [
                    obj
                    for obj in workspace_objects
                    if getattr(obj, "object_type", None) == "LIBRARY"
                ]
            )

            # Calculate total objects
            total_objects = len(workspace_objects)

            # Get workspace path depth distribution
            path_depths = [
                len(getattr(obj, "path", "").split("/")) for obj in workspace_objects
            ]
            avg_depth = sum(path_depths) / len(path_depths) if path_depths else 0
            max_depth = max(path_depths) if path_depths else 0

            return {
                "total_objects": total_objects,
                "notebooks": notebooks,
                "directories": directories,
                "libraries": libraries,
                "avg_path_depth": avg_depth,
                "max_path_depth": max_depth,
                "object_types": {
                    "notebooks": notebooks,
                    "directories": directories,
                    "libraries": libraries,
                },
            }

        except Exception as e:
            return {
                "error": f"Failed to get workspace stats: {str(e)}",
                "status": "error",
            }

    @log_function_call
    def list_workspace_objects(self, path: str = "/") -> List[Any]:
        """
        List workspace objects at a specific path.

        Args:
            path: Workspace path to list

        Returns:
            List of workspace objects
        """
        try:
            objects = list(self.client.workspace.list(path=path))
            return objects
        except Exception as e:
            print(f"❌ Failed to list workspace objects at {path}: {str(e)}")
            return []

    @log_function_call
    def get_workspace_info(self) -> Dict[str, Any]:
        """
        Get comprehensive workspace information.

        Returns:
            Dictionary containing workspace information
        """
        try:
            # Get current user
            current_user = self.client.current_user.me()

            # Get workspace configuration
            workspace_info = {
                "workspace_url": self.client.config.host,
                "workspace_id": self._get_workspace_id(),
                "current_user": {
                    "user_name": (
                        getattr(current_user, "user_name", None)
                        if current_user
                        else None
                    ),
                    "display_name": (
                        getattr(current_user, "display_name", None)
                        if current_user
                        else None
                    ),
                    "active": (
                        getattr(current_user, "active", True) if current_user else False
                    ),
                },
                "capabilities": {
                    "can_access_workspace": self._test_workspace_access(),
                    "can_list_objects": self._test_list_objects(),
                    "can_read_objects": self._test_read_objects(),
                },
                "statistics": self.get_workspace_stats(),
            }

            return workspace_info

        except Exception as e:
            return {
                "error": f"Failed to get workspace info: {str(e)}",
                "status": "error",
            }

    def _get_workspace_id(self) -> Optional[str]:
        """Get workspace ID from URL."""
        try:
            # Extract workspace ID from host URL
            host = self.client.config.host
            if "dev.azure.com" in host or "visualstudio.com" in host:
                # Azure DevOps workspace
                return host.split(".")[0]
            elif "cloud.databricks.com" in host:
                # Databricks cloud workspace
                return host.split(".")[0]
            else:
                return None
        except Exception:
            return None

    def _test_list_objects(self) -> bool:
        """Test if can list workspace objects."""
        try:
            self.client.workspace.list()
            return True
        except Exception:
            return False

    def _test_read_objects(self) -> bool:
        """Test if can read workspace objects."""
        try:
            # Try to get workspace root info
            self.client.workspace.get_status(path="/")
            return True
        except Exception:
            return False

    def print_workspace_summary(self) -> None:
        """Print a formatted workspace summary."""
        print("📁 Workspace Management Summary")
        print("=" * 50)

        info = self.get_workspace_info()

        if "error" in info:
            print(f"❌ Failed to get workspace info: {info['error']}")
            return

        # Print workspace details
        print(f"🌐 Workspace URL: {info.get('workspace_url', 'Unknown')}")
        print(f"🆔 Workspace ID: {info.get('workspace_id', 'Unknown')}")

        # Print current user
        current_user = info.get("current_user", {})
        print(f"\n👤 Current User:")
        print(f"   Username: {current_user.get('user_name', 'Unknown')}")
        print(f"   Display Name: {current_user.get('display_name', 'Unknown')}")
        print(f"   Active: {'✅' if current_user.get('active') else '❌'}")

        # Print capabilities
        capabilities = info.get("capabilities", {})
        print(f"\n🔧 Capabilities:")
        print(
            f"   Workspace Access: {'✅' if capabilities.get('can_access_workspace') else '❌'}"
        )
        print(
            f"   List Objects: {'✅' if capabilities.get('can_list_objects') else '❌'}"
        )
        print(
            f"   Read Objects: {'✅' if capabilities.get('can_read_objects') else '❌'}"
        )

        # Print statistics
        statistics = info.get("statistics", {})
        if statistics and "error" not in statistics:
            print(f"\n📊 Workspace Statistics:")
            print(f"   Total Objects: {statistics.get('total_objects', 0)}")
            print(f"   Notebooks: {statistics.get('notebooks', 0)}")
            print(f"   Directories: {statistics.get('directories', 0)}")
            print(f"   Libraries: {statistics.get('libraries', 0)}")
            print(f"   Avg Path Depth: {statistics.get('avg_path_depth', 0):.1f}")

        # Print health status
        health = self.get_workspace_health()
        if "health_score" in health:
            print(
                f"\n🏥 Workspace Health: {health['health_score']:.1f}/100 ({health['status']})"
            )

    @log_function_call
    def run_workspace_audit(self) -> Dict[str, Any]:
        """
        Run a comprehensive workspace audit.

        Returns:
            Dictionary containing audit results
        """
        print("🔍 Running Workspace Audit")
        print("=" * 50)

        try:
            audit_results = {"timestamp": self._get_current_timestamp(), "checks": {}}

            # Check 1: Workspace accessibility
            workspace_accessible = self._test_workspace_access()
            audit_results["checks"]["workspace_access"] = {
                "status": "PASS" if workspace_accessible else "FAIL",
                "description": "Workspace accessibility",
                "recommendation": (
                    "Check workspace permissions"
                    if not workspace_accessible
                    else "Workspace access confirmed"
                ),
            }

            # Check 2: Object listing capability
            can_list_objects = self._test_list_objects()
            audit_results["checks"]["object_listing"] = {
                "status": "PASS" if can_list_objects else "FAIL",
                "description": "Ability to list workspace objects",
                "recommendation": (
                    "Check workspace permissions"
                    if not can_list_objects
                    else "Object listing confirmed"
                ),
            }

            # Check 3: Object reading capability
            can_read_objects = self._test_read_objects()
            audit_results["checks"]["object_reading"] = {
                "status": "PASS" if can_read_objects else "FAIL",
                "description": "Ability to read workspace objects",
                "recommendation": (
                    "Check workspace permissions"
                    if not can_read_objects
                    else "Object reading confirmed"
                ),
            }

            # Check 4: Workspace organization
            workspace_stats = self.get_workspace_stats()
            if "error" not in workspace_stats:
                total_objects = workspace_stats.get("total_objects", 0)
                avg_depth = workspace_stats.get("avg_path_depth", 0)

                organization_good = total_objects > 0 and avg_depth <= 5
                audit_results["checks"]["workspace_organization"] = {
                    "status": "PASS" if organization_good else "WARN",
                    "description": f"Workspace organization (objects: {total_objects}, avg depth: {avg_depth:.1f})",
                    "recommendation": (
                        "Consider organizing workspace structure"
                        if not organization_good
                        else "Workspace organization looks good"
                    ),
                }
            else:
                audit_results["checks"]["workspace_organization"] = {
                    "status": "FAIL",
                    "description": "Unable to assess workspace organization",
                    "recommendation": "Check workspace access permissions",
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
                "error": f"Workspace audit failed: {str(e)}",
                "status": "error",
            }
            print(f"❌ {error_result['error']}")
            return error_result

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime

        return datetime.now().isoformat()
