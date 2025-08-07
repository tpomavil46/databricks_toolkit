#!/usr/bin/env python3
"""
Configuration Management CLI Tool

This CLI provides configuration management capabilities for Databricks workspace
including configuration validation, backup, and restoration.
"""

import os
import sys
import argparse
import json
import shutil
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from admin.core.admin_client import AdminClient
from utils.logger import log_function_call


@log_function_call
def validate_configuration(config_path: str) -> Dict[str, Any]:
    """
    Validate a configuration file.

    Args:
        config_path: Path to configuration file

    Returns:
        Dictionary containing validation results
    """
    validation_results = {
        "timestamp": _get_current_timestamp(),
        "config_path": config_path,
        "status": "UNKNOWN",
        "errors": [],
        "warnings": [],
        "details": {},
    }

    try:
        # Check if file exists
        if not os.path.exists(config_path):
            validation_results["status"] = "ERROR"
            validation_results["errors"].append(
                f"Configuration file not found: {config_path}"
            )
            return validation_results

        # Load and parse JSON
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
        except json.JSONDecodeError as e:
            validation_results["status"] = "ERROR"
            validation_results["errors"].append(f"Invalid JSON format: {str(e)}")
            return validation_results

        # Validate configuration structure
        if not isinstance(config, dict):
            validation_results["status"] = "ERROR"
            validation_results["errors"].append("Configuration must be a JSON object")
            return validation_results

        # Validate required fields based on config type
        config_type = config.get("type", "unknown")
        validation_results["details"]["config_type"] = config_type

        if config_type == "job":
            required_fields = ["name", "tasks"]
            for field in required_fields:
                if field not in config:
                    validation_results["errors"].append(
                        f"Missing required field: {field}"
                    )

        elif config_type == "cluster":
            required_fields = ["cluster_name", "spark_version"]
            for field in required_fields:
                if field not in config:
                    validation_results["errors"].append(
                        f"Missing required field: {field}"
                    )

        elif config_type == "workspace":
            # Workspace configs are more flexible
            if not config:
                validation_results["warnings"].append("Empty workspace configuration")

        # Check for common issues
        if "name" in config and len(config["name"]) > 100:
            validation_results["warnings"].append("Name is very long (>100 characters)")

        if "description" in config and len(config["description"]) > 500:
            validation_results["warnings"].append(
                "Description is very long (>500 characters)"
            )

        # Determine overall status
        if validation_results["errors"]:
            validation_results["status"] = "ERROR"
        elif validation_results["warnings"]:
            validation_results["status"] = "WARNING"
        else:
            validation_results["status"] = "PASS"

        return validation_results

    except Exception as e:
        validation_results["status"] = "ERROR"
        validation_results["errors"].append(f"Validation failed: {str(e)}")
        return validation_results


@log_function_call
def backup_configuration(config_path: str, backup_dir: str = None) -> Dict[str, Any]:
    """
    Backup a configuration file.

    Args:
        config_path: Path to configuration file
        backup_dir: Directory for backup (optional)

    Returns:
        Dictionary containing backup results
    """
    backup_results = {
        "timestamp": _get_current_timestamp(),
        "config_path": config_path,
        "backup_path": None,
        "status": "UNKNOWN",
        "details": {},
    }

    try:
        # Check if source file exists
        if not os.path.exists(config_path):
            backup_results["status"] = "ERROR"
            backup_results["error"] = f"Configuration file not found: {config_path}"
            return backup_results

        # Determine backup directory
        if not backup_dir:
            backup_dir = os.path.join(os.path.dirname(config_path), "backups")

        # Create backup directory if it doesn't exist
        os.makedirs(backup_dir, exist_ok=True)

        # Generate backup filename
        config_name = os.path.splitext(os.path.basename(config_path))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{config_name}_backup_{timestamp}.json"
        backup_path = os.path.join(backup_dir, backup_filename)

        # Copy configuration file
        shutil.copy2(config_path, backup_path)

        # Add metadata to backup
        backup_metadata = {
            "original_path": config_path,
            "backup_timestamp": _get_current_timestamp(),
            "backup_version": "1.0",
        }

        metadata_path = backup_path.replace(".json", "_metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(backup_metadata, f, indent=2)

        backup_results["status"] = "SUCCESS"
        backup_results["backup_path"] = backup_path
        backup_results["metadata_path"] = metadata_path
        backup_results["details"] = {
            "original_size": os.path.getsize(config_path),
            "backup_size": os.path.getsize(backup_path),
            "backup_directory": backup_dir,
        }

        return backup_results

    except Exception as e:
        backup_results["status"] = "ERROR"
        backup_results["error"] = str(e)
        return backup_results


@log_function_call
def restore_configuration(backup_path: str, target_path: str = None) -> Dict[str, Any]:
    """
    Restore a configuration from backup.

    Args:
        backup_path: Path to backup file
        target_path: Target path for restoration (optional)

    Returns:
        Dictionary containing restoration results
    """
    restore_results = {
        "timestamp": _get_current_timestamp(),
        "backup_path": backup_path,
        "target_path": None,
        "status": "UNKNOWN",
        "details": {},
    }

    try:
        # Check if backup file exists
        if not os.path.exists(backup_path):
            restore_results["status"] = "ERROR"
            restore_results["error"] = f"Backup file not found: {backup_path}"
            return restore_results

        # Determine target path
        if not target_path:
            # Extract original path from backup metadata
            metadata_path = backup_path.replace(".json", "_metadata.json")
            if os.path.exists(metadata_path):
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)
                target_path = metadata.get(
                    "original_path", backup_path.replace("_backup_", "_restored_")
                )
            else:
                target_path = backup_path.replace("_backup_", "_restored_")

        # Validate backup file
        try:
            with open(backup_path, "r") as f:
                config = json.load(f)
        except json.JSONDecodeError as e:
            restore_results["status"] = "ERROR"
            restore_results["error"] = f"Invalid backup file format: {str(e)}"
            return restore_results

        # Create target directory if needed
        target_dir = os.path.dirname(target_path)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        # Restore configuration
        shutil.copy2(backup_path, target_path)

        restore_results["status"] = "SUCCESS"
        restore_results["target_path"] = target_path
        restore_results["details"] = {
            "backup_size": os.path.getsize(backup_path),
            "restored_size": os.path.getsize(target_path),
            "config_keys": list(config.keys()) if isinstance(config, dict) else [],
        }

        return restore_results

    except Exception as e:
        restore_results["status"] = "ERROR"
        restore_results["error"] = str(e)
        return restore_results


def print_config_report(config_results: Dict[str, Any]) -> None:
    """Print a formatted configuration report."""
    print("⚙️  Configuration Management Report")
    print("=" * 50)

    if "error" in config_results:
        print(f"❌ Operation failed: {config_results['error']}")
        return

    print(f"📊 Status: {config_results['status']}")

    if "config_path" in config_results:
        print(f"📁 Config Path: {config_results['config_path']}")

    if "backup_path" in config_results:
        print(f"💾 Backup Path: {config_results['backup_path']}")

    if "target_path" in config_results:
        print(f"🎯 Target Path: {config_results['target_path']}")

    if "details" in config_results:
        details = config_results["details"]
        if "config_type" in details:
            print(f"📋 Config Type: {details['config_type']}")

        if "config_keys" in details:
            print(f"\n🔑 Configuration Keys:")
            for key in details["config_keys"]:
                print(f"   • {key}")

    if config_results.get("errors"):
        print(f"\n❌ Errors:")
        for error in config_results["errors"]:
            print(f"   • {error}")

    if config_results.get("warnings"):
        print(f"\n⚠️  Warnings:")
        for warning in config_results["warnings"]:
            print(f"   • {warning}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Databricks Configuration Management Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate configuration
  python cli/deployment/config_cli.py validate --config jobs/my_job.json

  # Backup configuration
  python cli/deployment/config_cli.py backup --config jobs/my_job.json --backup-dir backups/

  # Restore configuration
  python cli/deployment/config_cli.py restore --backup backups/my_job_backup_20231201_143022.json
        """,
    )

    parser.add_argument(
        "command",
        choices=["validate", "backup", "restore"],
        help="Configuration management command to execute",
    )

    parser.add_argument("--config", help="Path to configuration file")

    parser.add_argument("--backup-dir", help="Directory for backup files")

    parser.add_argument("--backup", help="Path to backup file (for restore)")

    parser.add_argument("--target", help="Target path for restoration")

    args = parser.parse_args()

    # Initialize admin client
    admin_client = AdminClient()

    if args.command == "validate":
        if not args.config:
            print("❌ Error: --config is required for validation")
            sys.exit(1)

        print("🔍 Validating Configuration")
        print("=" * 50)
        config_results = validate_configuration(args.config)
        print_config_report(config_results)

    elif args.command == "backup":
        if not args.config:
            print("❌ Error: --config is required for backup")
            sys.exit(1)

        print("💾 Backing Up Configuration")
        print("=" * 50)
        config_results = backup_configuration(args.config, args.backup_dir)
        print_config_report(config_results)

    elif args.command == "restore":
        if not args.backup:
            print("❌ Error: --backup is required for restoration")
            sys.exit(1)

        print("🔄 Restoring Configuration")
        print("=" * 50)
        config_results = restore_configuration(args.backup, args.target)
        print_config_report(config_results)

    else:
        print(f"❌ Unknown command: {args.command}")
        sys.exit(1)


def _get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now().isoformat()


if __name__ == "__main__":
    main()
