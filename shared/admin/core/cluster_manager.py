"""
Cluster Management Module

This module provides comprehensive cluster management capabilities including
cluster creation, monitoring, health checks, and optimization.
"""

from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
from utils.logger import log_function_call


class ClusterManager:
    """
    Cluster management operations for Databricks workspace.
    """

    def __init__(self, client: WorkspaceClient):
        """
        Initialize the cluster manager.

        Args:
            client: Databricks workspace client
        """
        self.client = client

    @log_function_call
    def list_clusters(self) -> List[ClusterDetails]:
        """
        List all clusters in the workspace.

        Returns:
            List of cluster objects
        """
        try:
            clusters = list(self.client.clusters.list())
            return clusters
        except Exception as e:
            print(f"❌ Failed to list clusters: {str(e)}")
            return []

    @log_function_call
    def get_cluster(self, cluster_id: str) -> Optional[ClusterDetails]:
        """
        Get a specific cluster by ID.

        Args:
            cluster_id: Cluster ID to retrieve

        Returns:
            Cluster object or None if not found
        """
        try:
            cluster = self.client.clusters.get(cluster_id=cluster_id)
            return cluster
        except Exception as e:
            print(f"❌ Failed to get cluster {cluster_id}: {str(e)}")
            return None

    @log_function_call
    def create_cluster(
        self,
        cluster_name: str,
        spark_version: str = "13.3.x-scala2.12",
        node_type_id: str = "i3.xlarge",
        num_workers: int = 1,
        autotermination_minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Create a new cluster.

        Args:
            cluster_name: Name for the new cluster
            spark_version: Spark version to use
            node_type_id: Node type for the cluster
            num_workers: Number of worker nodes
            autotermination_minutes: Auto-termination timeout in minutes

        Returns:
            Dictionary containing creation result
        """
        try:
            # Check if cluster with same name already exists
            clusters = self.list_clusters()
            for cluster in clusters:
                if cluster.cluster_name == cluster_name:
                    return {
                        "success": False,
                        "error": f"Cluster {cluster_name} already exists",
                        "cluster": cluster,
                    }

            # Create cluster
            cluster = self.client.clusters.create(
                cluster_name=cluster_name,
                spark_version=spark_version,
                node_type_id=node_type_id,
                num_workers=num_workers,
                autotermination_minutes=autotermination_minutes,
            )

            return {
                "success": True,
                "cluster": cluster,
                "message": f"Cluster {cluster_name} created successfully",
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to create cluster {cluster_name}: {str(e)}",
            }

    @log_function_call
    def start_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Start a cluster.

        Args:
            cluster_id: Cluster ID to start

        Returns:
            Dictionary containing operation result
        """
        try:
            self.client.clusters.start(cluster_id=cluster_id)

            return {
                "success": True,
                "message": f"Cluster {cluster_id} started successfully",
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to start cluster {cluster_id}: {str(e)}",
            }

    @log_function_call
    def stop_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Stop a cluster.

        Args:
            cluster_id: Cluster ID to stop

        Returns:
            Dictionary containing operation result
        """
        try:
            self.client.clusters.delete(cluster_id=cluster_id)

            return {
                "success": True,
                "message": f"Cluster {cluster_id} stopped successfully",
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to stop cluster {cluster_id}: {str(e)}",
            }

    @log_function_call
    def delete_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Permanently delete a cluster.

        Args:
            cluster_id: Cluster ID to delete

        Returns:
            Dictionary containing operation result
        """
        try:
            self.client.clusters.permanent_delete(cluster_id=cluster_id)

            return {
                "success": True,
                "message": f"Cluster {cluster_id} deleted permanently",
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to delete cluster {cluster_id}: {str(e)}",
            }

    @log_function_call
    def get_cluster_health(self) -> Dict[str, Any]:
        """
        Get cluster system health metrics.

        Returns:
            Dictionary containing cluster health information
        """
        try:
            clusters = self.list_clusters()

            if not clusters:
                return {
                    "health_score": 0,
                    "status": "critical",
                    "error": "No clusters found or unable to list clusters",
                }

            # Calculate health metrics
            total_clusters = len(clusters)
            running_clusters = len([c for c in clusters if c.state == "RUNNING"])
            terminated_clusters = len([c for c in clusters if c.state == "TERMINATED"])
            error_clusters = len([c for c in clusters if c.state == "ERROR"])

            # Clusters with reasonable autotermination
            reasonable_autotermination = len(
                [
                    c
                    for c in clusters
                    if c.autotermination_minutes and c.autotermination_minutes <= 120
                ]
            )

            # Calculate health score
            health_score = 0
            issues = []

            # Running clusters ratio (should be > 50% if any clusters exist)
            if total_clusters > 0:
                running_ratio = running_clusters / total_clusters
                if running_ratio < 0.5:
                    issues.append(f"Low running cluster ratio: {running_ratio:.1%}")
                    health_score += 20
                else:
                    health_score += 40
            else:
                health_score += 40  # No clusters is acceptable

            # Error clusters (should be 0)
            if error_clusters > 0:
                issues.append(f"Clusters in error state: {error_clusters}")
                health_score += 0
            else:
                health_score += 30

            # Autotermination coverage (should be > 80%)
            if total_clusters > 0:
                autotermination_ratio = reasonable_autotermination / total_clusters
                if autotermination_ratio < 0.8:
                    issues.append(
                        f"Low autotermination coverage: {autotermination_ratio:.1%}"
                    )
                    health_score += 15
                else:
                    health_score += 30
            else:
                health_score += 30

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
                    "total_clusters": total_clusters,
                    "running_clusters": running_clusters,
                    "terminated_clusters": terminated_clusters,
                    "error_clusters": error_clusters,
                    "reasonable_autotermination": reasonable_autotermination,
                    "running_ratio": (
                        running_clusters / total_clusters if total_clusters > 0 else 0
                    ),
                    "autotermination_ratio": (
                        reasonable_autotermination / total_clusters
                        if total_clusters > 0
                        else 0
                    ),
                },
                "issues": issues,
            }

        except Exception as e:
            return {
                "health_score": 0,
                "status": "critical",
                "error": f"Failed to get cluster health: {str(e)}",
            }

    def print_cluster_summary(self) -> None:
        """Print a formatted cluster summary."""
        print("🖥️  Cluster Management Summary")
        print("=" * 50)

        clusters = self.list_clusters()
        if not clusters:
            print("❌ No clusters found or unable to list clusters")
            return

        # Print cluster statistics
        total_clusters = len(clusters)
        running_clusters = len([c for c in clusters if c.state == "RUNNING"])
        terminated_clusters = len([c for c in clusters if c.state == "TERMINATED"])
        error_clusters = len([c for c in clusters if c.state == "ERROR"])

        print(f"📊 Cluster Statistics:")
        print(f"   Total Clusters: {total_clusters}")
        print(f"   Running: {running_clusters}")
        print(f"   Terminated: {terminated_clusters}")
        print(f"   Error: {error_clusters}")

        # Print recent clusters (last 5)
        print(f"\n🖥️  Recent Clusters:")
        for cluster in clusters[:5]:
            status_icon = {
                "RUNNING": "🟢",
                "TERMINATED": "🔴",
                "ERROR": "🔴",
                "STARTING": "🟡",
                "STOPPING": "🟡",
            }.get(cluster.state, "⚪")

            print(f"   {status_icon} {cluster.cluster_name} ({cluster.state})")

        if len(clusters) > 5:
            print(f"   ... and {len(clusters) - 5} more clusters")

        # Print health status
        health = self.get_cluster_health()
        if "health_score" in health:
            print(
                f"\n🏥 Cluster Health: {health['health_score']:.1f}/100 ({health['status']})"
            )

    @log_function_call
    def get_cluster_usage_stats(self) -> Dict[str, Any]:
        """
        Get cluster usage statistics.

        Returns:
            Dictionary containing usage statistics
        """
        try:
            clusters = self.list_clusters()

            # Calculate usage metrics
            total_clusters = len(clusters)
            running_clusters = len([c for c in clusters if c.state == "RUNNING"])

            # Calculate total workers
            total_workers = sum(c.num_workers for c in clusters if c.num_workers)

            # Calculate autotermination coverage
            autotermination_enabled = len(
                [
                    c
                    for c in clusters
                    if c.autotermination_minutes and c.autotermination_minutes > 0
                ]
            )

            # Calculate average autotermination time
            autotermination_times = [
                c.autotermination_minutes for c in clusters if c.autotermination_minutes
            ]
            avg_autotermination = (
                sum(autotermination_times) / len(autotermination_times)
                if autotermination_times
                else 0
            )

            return {
                "total_clusters": total_clusters,
                "running_clusters": running_clusters,
                "total_workers": total_workers,
                "autotermination_enabled": autotermination_enabled,
                "avg_autotermination_minutes": avg_autotermination,
                "utilization_rate": (
                    running_clusters / total_clusters if total_clusters > 0 else 0
                ),
            }

        except Exception as e:
            return {
                "error": f"Failed to get cluster usage stats: {str(e)}",
                "status": "error",
            }
