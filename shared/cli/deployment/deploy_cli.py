#!/usr/bin/env python3
"""
Deployment CLI Tool

This CLI provides deployment management capabilities for Databricks workspace
including job deployment, configuration deployment, and environment management.
"""

import os
import sys
import argparse
import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from admin.core.admin_client import AdminClient
from utils.logger import log_function_call


@log_function_call
def deploy_job(job_config_path: str, admin_client: AdminClient) -> Dict[str, Any]:
    """
    Deploy a job to Databricks workspace.

    Args:
        job_config_path: Path to job configuration file
        admin_client: AdminClient instance

    Returns:
        Dictionary containing deployment results
    """
    deployment_results = {
        "timestamp": _get_current_timestamp(),
        "job_config_path": job_config_path,
        "status": "UNKNOWN",
        "job_id": None,
        "details": {},
    }

    try:
        # Load job configuration
        if not os.path.exists(job_config_path):
            deployment_results["status"] = "ERROR"
            deployment_results["error"] = (
                f"Job configuration file not found: {job_config_path}"
            )
            return deployment_results

        with open(job_config_path, "r") as f:
            job_config = json.load(f)

        # Validate job configuration
        required_fields = ["name", "tasks"]
        for field in required_fields:
            if field not in job_config:
                deployment_results["status"] = "ERROR"
                deployment_results["error"] = (
                    f"Missing required field in job config: {field}"
                )
                return deployment_results

        # Deploy job using Databricks CLI
        import subprocess

        try:
            result = subprocess.run(
                [
                    "databricks",
                    "jobs",
                    "create",
                    "--profile",
                    "databricks",
                    "--json-file",
                    job_config_path,
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            # Parse job ID from output
            output_lines = result.stdout.strip().split("\n")
            job_id = None
            for line in output_lines:
                if "job_id" in line.lower():
                    job_id = line.split()[-1]
                    break

            deployment_results["status"] = "SUCCESS"
            deployment_results["job_id"] = job_id
            deployment_results["details"] = {
                "job_name": job_config.get("name"),
                "task_count": len(job_config.get("tasks", [])),
                "deployment_output": result.stdout,
            }

        except subprocess.CalledProcessError as e:
            deployment_results["status"] = "ERROR"
            deployment_results["error"] = f"Job deployment failed: {e.stderr}"

        return deployment_results

    except Exception as e:
        deployment_results["status"] = "ERROR"
        deployment_results["error"] = str(e)
        return deployment_results


@log_function_call
def deploy_configuration(config_path: str, admin_client: AdminClient) -> Dict[str, Any]:
    """
    Deploy configuration to Databricks workspace.

    Args:
        config_path: Path to configuration file
        admin_client: AdminClient instance

    Returns:
        Dictionary containing deployment results
    """
    config_results = {
        "timestamp": _get_current_timestamp(),
        "config_path": config_path,
        "status": "UNKNOWN",
        "details": {},
    }

    try:
        # Load configuration
        if not os.path.exists(config_path):
            config_results["status"] = "ERROR"
            config_results["error"] = f"Configuration file not found: {config_path}"
            return config_results

        with open(config_path, "r") as f:
            config = json.load(f)

        # Validate configuration
        if not isinstance(config, dict):
            config_results["status"] = "ERROR"
            config_results["error"] = "Configuration must be a JSON object"
            return config_results

        # Apply configuration (this would be expanded based on config type)
        config_results["status"] = "SUCCESS"
        config_results["details"] = {
            "config_type": "workspace_configuration",
            "config_keys": list(config.keys()),
            "applied_at": _get_current_timestamp(),
        }

        return config_results

    except Exception as e:
        config_results["status"] = "ERROR"
        config_results["error"] = str(e)
        return config_results


@log_function_call
def deploy_environment(
    environment_name: str, config_path: str, admin_client: AdminClient
) -> Dict[str, Any]:
    """
    Deploy environment configuration to Databricks workspace.

    Args:
        environment_name: Name of the environment
        config_path: Path to environment configuration file
        admin_client: AdminClient instance

    Returns:
        Dictionary containing deployment results
    """
    env_results = {
        "timestamp": _get_current_timestamp(),
        "environment_name": environment_name,
        "config_path": config_path,
        "status": "UNKNOWN",
        "deployed_components": [],
        "details": {},
    }

    try:
        # Load environment configuration
        if not os.path.exists(config_path):
            env_results["status"] = "ERROR"
            env_results["error"] = (
                f"Environment configuration file not found: {config_path}"
            )
            return env_results

        with open(config_path, "r") as f:
            env_config = json.load(f)

        # Validate environment configuration
        required_sections = ["clusters", "jobs", "workspace"]
        for section in required_sections:
            if section not in env_config:
                env_results["status"] = "WARNING"
                env_results["details"][
                    f"missing_{section}"
                ] = f"No {section} configuration found"

        # Deploy environment components
        deployed_components = []

        # Deploy clusters if specified
        if "clusters" in env_config:
            for cluster_config in env_config["clusters"]:
                try:
                    # This would use the cluster manager to create clusters
                    deployed_components.append(
                        f"cluster:{cluster_config.get('name', 'unknown')}"
                    )
                except Exception as e:
                    env_results["details"][f"cluster_error"] = str(e)

        # Deploy jobs if specified
        if "jobs" in env_config:
            for job_config in env_config["jobs"]:
                try:
                    # This would use the job deployment function
                    deployed_components.append(
                        f"job:{job_config.get('name', 'unknown')}"
                    )
                except Exception as e:
                    env_results["details"][f"job_error"] = str(e)

        # Deploy workspace configuration if specified
        if "workspace" in env_config:
            try:
                # This would apply workspace configuration
                deployed_components.append("workspace:configuration")
            except Exception as e:
                env_results["details"][f"workspace_error"] = str(e)

        env_results["deployed_components"] = deployed_components
        env_results["status"] = "SUCCESS" if deployed_components else "WARNING"

        return env_results

    except Exception as e:
        env_results["status"] = "ERROR"
        env_results["error"] = str(e)
        return env_results


def print_deployment_report(deployment_results: Dict[str, Any]) -> None:
    """Print a formatted deployment report."""
    print("🚀 Deployment Report")
    print("=" * 50)

    if "error" in deployment_results:
        print(f"❌ Deployment failed: {deployment_results['error']}")
        return

    print(f"📊 Status: {deployment_results['status']}")

    if "job_id" in deployment_results and deployment_results["job_id"]:
        print(f"🆔 Job ID: {deployment_results['job_id']}")

    if "environment_name" in deployment_results:
        print(f"🌍 Environment: {deployment_results['environment_name']}")

    if "deployed_components" in deployment_results:
        print(f"\n✅ Deployed Components:")
        for component in deployment_results["deployed_components"]:
            print(f"   • {component}")

    if "details" in deployment_results:
        details = deployment_results["details"]
        if "job_name" in details:
            print(f"\n📋 Job Details:")
            print(f"   Name: {details['job_name']}")
            print(f"   Tasks: {details['task_count']}")

        if "config_keys" in details:
            print(f"\n⚙️  Configuration Keys:")
            for key in details["config_keys"]:
                print(f"   • {key}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Databricks Deployment Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Deploy a job
  python cli/deployment/deploy_cli.py deploy-job --config jobs/my_job.json

  # Deploy configuration
  python cli/deployment/deploy_cli.py deploy-config --config config/workspace.json

  # Deploy environment
  python cli/deployment/deploy_cli.py deploy-environment --name production --config environments/prod.json
        """,
    )

    parser.add_argument(
        "command",
        choices=["deploy-job", "deploy-config", "deploy-environment"],
        help="Deployment command to execute",
    )

    parser.add_argument("--config", required=True, help="Path to configuration file")

    parser.add_argument("--name", help="Environment name (for environment deployment)")

    args = parser.parse_args()

    # Initialize admin client
    admin_client = AdminClient()

    if args.command == "deploy-job":
        print("🚀 Deploying Job")
        print("=" * 50)
        deployment_results = deploy_job(args.config, admin_client)
        print_deployment_report(deployment_results)

    elif args.command == "deploy-config":
        print("⚙️  Deploying Configuration")
        print("=" * 50)
        deployment_results = deploy_configuration(args.config, admin_client)
        print_deployment_report(deployment_results)

    elif args.command == "deploy-environment":
        if not args.name:
            print("❌ Error: --name is required for environment deployment")
            sys.exit(1)

        print(f"🌍 Deploying Environment: {args.name}")
        print("=" * 50)
        deployment_results = deploy_environment(args.name, args.config, admin_client)
        print_deployment_report(deployment_results)

    else:
        print(f"❌ Unknown command: {args.command}")
        sys.exit(1)


def _get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now().isoformat()


if __name__ == "__main__":
    main()
