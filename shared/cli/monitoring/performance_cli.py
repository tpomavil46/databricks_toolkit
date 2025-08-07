#!/usr/bin/env python3
"""
Performance Monitoring CLI Tool

This CLI provides performance monitoring capabilities for Databricks workspace
including cluster performance, job performance, and system metrics.
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
def get_cluster_performance_metrics(
    admin_client: AdminClient, cluster_id: str = None
) -> Dict[str, Any]:
    """
    Get cluster performance metrics.

    Args:
        admin_client: AdminClient instance
        cluster_id: Specific cluster ID to analyze (optional)

    Returns:
        Dictionary containing cluster performance metrics
    """
    performance_metrics = {
        "timestamp": _get_current_timestamp(),
        "cluster_id": cluster_id,
        "metrics": {},
        "recommendations": [],
    }

    try:
        clusters = admin_client.clusters.list_clusters()

        if cluster_id:
            # Analyze specific cluster
            target_cluster = None
            for cluster in clusters:
                if cluster.get("cluster_id") == cluster_id:
                    target_cluster = cluster
                    break

            if not target_cluster:
                performance_metrics["error"] = f"Cluster {cluster_id} not found"
                return performance_metrics

            # Analyze cluster performance
            config = target_cluster.get("cluster_config", {})
            state = target_cluster.get("state", "UNKNOWN")

            performance_metrics["metrics"] = {
                "cluster_state": state,
                "node_type": config.get("node_type_id", "unknown"),
                "num_workers": config.get("num_workers", 0),
                "driver_node_type": config.get("driver_node_type_id", "unknown"),
                "autoscaling": config.get("autoscale", {}),
                "last_activity": target_cluster.get("last_activity_time"),
                "created_time": target_cluster.get("created_time"),
            }

            # Generate performance recommendations
            if state != "RUNNING":
                performance_metrics["recommendations"].append(
                    f"Cluster {cluster_id} is not running - consider starting it"
                )

            if config.get("num_workers", 0) == 0:
                performance_metrics["recommendations"].append(
                    "Single-node cluster detected - consider adding workers for better performance"
                )

        else:
            # Analyze all clusters
            running_clusters = [c for c in clusters if c.get("state") == "RUNNING"]
            stopped_clusters = [c for c in clusters if c.get("state") == "TERMINATED"]

            total_workers = sum(
                c.get("cluster_config", {}).get("num_workers", 0)
                for c in running_clusters
            )
            avg_workers = (
                total_workers / len(running_clusters) if running_clusters else 0
            )

            performance_metrics["metrics"] = {
                "total_clusters": len(clusters),
                "running_clusters": len(running_clusters),
                "stopped_clusters": len(stopped_clusters),
                "total_workers": total_workers,
                "avg_workers_per_cluster": avg_workers,
                "utilization_rate": (
                    (len(running_clusters) / len(clusters)) * 100 if clusters else 0
                ),
            }

            # Generate performance recommendations
            if len(running_clusters) == 0:
                performance_metrics["recommendations"].append(
                    "No running clusters - start clusters for data processing"
                )

            if avg_workers < 2 and len(running_clusters) > 0:
                performance_metrics["recommendations"].append(
                    "Low worker count - consider adding workers for better performance"
                )

            if len(stopped_clusters) > len(running_clusters):
                performance_metrics["recommendations"].append(
                    "More stopped than running clusters - consider cleanup"
                )

        return performance_metrics

    except Exception as e:
        performance_metrics["error"] = str(e)
        return performance_metrics


@log_function_call
def get_system_performance_metrics(admin_client: AdminClient) -> Dict[str, Any]:
    """
    Get system-wide performance metrics.

    Args:
        admin_client: AdminClient instance

    Returns:
        Dictionary containing system performance metrics
    """
    system_metrics = {
        "timestamp": _get_current_timestamp(),
        "metrics": {},
        "recommendations": [],
    }

    try:
        # Get workspace information
        workspace_info = admin_client.workspace.get_workspace_info()
        workspace_stats = workspace_info.get("statistics", {})

        # Get user activity
        users = admin_client.users.list_users()
        active_users = [u for u in users if u.get("active", True)]

        # Get cluster information
        clusters = admin_client.clusters.list_clusters()
        running_clusters = [c for c in clusters if c.get("state") == "RUNNING"]

        # Calculate system metrics
        system_metrics["metrics"] = {
            "workspace_objects": workspace_stats.get("total_objects", 0),
            "notebooks": workspace_stats.get("notebooks", 0),
            "libraries": workspace_stats.get("libraries", 0),
            "avg_path_depth": workspace_stats.get("avg_path_depth", 0),
            "total_users": len(users),
            "active_users": len(active_users),
            "user_utilization": (len(active_users) / len(users)) * 100 if users else 0,
            "total_clusters": len(clusters),
            "running_clusters": len(running_clusters),
            "cluster_utilization": (
                (len(running_clusters) / len(clusters)) * 100 if clusters else 0
            ),
        }

        # Generate system recommendations
        if system_metrics["metrics"]["user_utilization"] < 50:
            system_metrics["recommendations"].append(
                "Low user utilization - consider user training or cleanup"
            )

        if system_metrics["metrics"]["cluster_utilization"] < 30:
            system_metrics["recommendations"].append(
                "Low cluster utilization - consider stopping unused clusters"
            )

        if system_metrics["metrics"]["workspace_objects"] > 1000:
            system_metrics["recommendations"].append(
                "High object count - consider workspace cleanup"
            )

        if system_metrics["metrics"]["avg_path_depth"] > 5:
            system_metrics["recommendations"].append(
                "Deep directory structure - consider reorganizing workspace"
            )

        return system_metrics

    except Exception as e:
        system_metrics["error"] = str(e)
        return system_metrics


@log_function_call
def get_job_performance_metrics(admin_client: AdminClient) -> Dict[str, Any]:
    """
    Get job performance metrics.

    Args:
        admin_client: AdminClient instance

    Returns:
        Dictionary containing job performance metrics
    """
    job_metrics = {
        "timestamp": _get_current_timestamp(),
        "metrics": {},
        "recommendations": [],
    }

    try:
        # This would integrate with actual job monitoring APIs
        # For now, we'll provide a framework for job performance analysis

        job_metrics["metrics"] = {
            "total_jobs": 0,  # Would be populated from job API
            "running_jobs": 0,
            "completed_jobs": 0,
            "failed_jobs": 0,
            "avg_job_duration": 0,
            "success_rate": 0,
        }

        # Generate job recommendations
        job_metrics["recommendations"].append(
            "Job monitoring requires integration with Databricks Jobs API"
        )
        job_metrics["recommendations"].append(
            "Consider implementing job performance tracking"
        )

        return job_metrics

    except Exception as e:
        job_metrics["error"] = str(e)
        return job_metrics


def print_performance_report(performance_results: Dict[str, Any]) -> None:
    """Print a formatted performance report."""
    print("⚡ Performance Monitoring Report")
    print("=" * 50)

    if "error" in performance_results:
        print(f"❌ Performance analysis failed: {performance_results['error']}")
        return

    if "cluster_id" in performance_results and performance_results["cluster_id"]:
        print(f"🖥️  Cluster Performance: {performance_results['cluster_id']}")
    else:
        print("🖥️  System Performance Overview")

    if "metrics" in performance_results:
        metrics = performance_results["metrics"]

        if "cluster_state" in metrics:
            print(f"   State: {metrics['cluster_state']}")
            print(f"   Node Type: {metrics['node_type']}")
            print(f"   Workers: {metrics['num_workers']}")

        if "total_clusters" in metrics:
            print(f"   Total Clusters: {metrics['total_clusters']}")
            print(f"   Running Clusters: {metrics['running_clusters']}")
            print(f"   Utilization Rate: {metrics['utilization_rate']:.1f}%")

        if "total_users" in metrics:
            print(f"   Total Users: {metrics['total_users']}")
            print(f"   Active Users: {metrics['active_users']}")
            print(f"   User Utilization: {metrics['user_utilization']:.1f}%")

        if "workspace_objects" in metrics:
            print(f"   Workspace Objects: {metrics['workspace_objects']}")
            print(f"   Notebooks: {metrics['notebooks']}")
            print(f"   Libraries: {metrics['libraries']}")

    if performance_results.get("recommendations"):
        print(f"\n💡 Performance Optimization Recommendations:")
        for recommendation in performance_results["recommendations"]:
            print(f"   • {recommendation}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Databricks Performance Monitoring Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get cluster performance metrics
  python cli/monitoring/performance_cli.py cluster-performance --cluster-id 1234-567890-abcdef

  # Get system performance metrics
  python cli/monitoring/performance_cli.py system-performance

  # Get job performance metrics
  python cli/monitoring/performance_cli.py job-performance
        """,
    )

    parser.add_argument(
        "command",
        choices=["cluster-performance", "system-performance", "job-performance"],
        help="Performance monitoring command to execute",
    )

    parser.add_argument("--cluster-id", help="Specific cluster ID to analyze")

    args = parser.parse_args()

    # Initialize admin client
    admin_client = AdminClient()

    if args.command == "cluster-performance":
        print("⚡ Running Cluster Performance Analysis")
        print("=" * 50)
        performance_results = get_cluster_performance_metrics(
            admin_client, args.cluster_id
        )
        print_performance_report(performance_results)

    elif args.command == "system-performance":
        print("⚡ Running System Performance Analysis")
        print("=" * 50)
        performance_results = get_system_performance_metrics(admin_client)
        print_performance_report(performance_results)

    elif args.command == "job-performance":
        print("⚡ Running Job Performance Analysis")
        print("=" * 50)
        performance_results = get_job_performance_metrics(admin_client)
        print_performance_report(performance_results)

    else:
        print(f"❌ Unknown command: {args.command}")
        sys.exit(1)


def _get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now().isoformat()


if __name__ == "__main__":
    main()
